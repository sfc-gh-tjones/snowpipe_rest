package com.example.SnowpipeRest.buffer;

import com.example.SnowpipeRest.utils.TablePartitionKey;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.*;

/** Manages draining across buffers */
@Component
public class DrainManager implements Runnable {

  static final Logger LOGGER = LoggerFactory.getLogger(DrainManager.class);

  // The manager of the buffers that we wish to drain
  private final BufferManager bufferManager;

  // The executor service that will invoke the `drain` methods on a particular buffer
  ThreadPoolExecutor executor;

  // The current working set of tables that we are working on. These should be in sync
  private final Set<TablePartitionKey> tableWorkingSet;
  private final Queue<TablePartitionKey> tableWorkQueue;

  private final long maxDurationToDrainMs;
  private final long maxRecordsToDrain;
  private final long ingestEngineEpochTs;
  private final int maxSecondsToWaitToDrain;

  private final boolean useWAL;

  enum Action {
    ADD_TO_QUEUE,
    REMOVE_FROM_QUEUE
  }

  /** Default constructor */
  public DrainManager(
      long ingestEngineEpochTs,
      BufferManager bufferManager,
      int numThreads,
      long maxDurationToDrainMs,
      long maxRecordsToDrain,
      int maxSecondsToWaitToDrain,
      boolean useWAL) {
    this.ingestEngineEpochTs = ingestEngineEpochTs;
    this.bufferManager = bufferManager;
    executor =
        new ThreadPoolExecutor(
            numThreads, numThreads, 10, TimeUnit.SECONDS, new LinkedBlockingQueue<>(10));
    executor.setRejectedExecutionHandler(
        (r, exec) -> LOGGER.info("Rejected work queue draining task"));
    tableWorkingSet = Collections.newSetFromMap(new ConcurrentHashMap<>());
    tableWorkQueue = new ConcurrentLinkedQueue<>();
    this.maxDurationToDrainMs = maxDurationToDrainMs;
    this.maxRecordsToDrain = maxRecordsToDrain;
    this.maxSecondsToWaitToDrain = maxSecondsToWaitToDrain;
    this.useWAL = useWAL;

    LOGGER.info(
        "Started DrainManager. numThreads={} maxDurationToDrainMs={} maxRecordsToDrain={} maxSecondsToWaitToDrain={} useWAL={}",
        numThreads,
        maxDurationToDrainMs,
        maxRecordsToDrain,
        maxSecondsToWaitToDrain,
        useWAL);
  }

  @VisibleForTesting
  public Set<TablePartitionKey> getTableWorkSet() {
    return tableWorkingSet;
  }

  @VisibleForTesting
  public Queue<TablePartitionKey> getTableWorkQueue() {
    return tableWorkQueue;
  }

  // This is hella hacky but coordinate add/remove from the work set via an action
  private synchronized void modifyTableWorkSet(TablePartitionKey tableKey, Action action) {
    if (action == Action.ADD_TO_QUEUE) {
      if (!tableWorkingSet.contains(tableKey)) {
        LOGGER.info(
            "Enqueueing table to work queue. db={} schema={} table={}",
            tableKey.getDatabase(),
            tableKey.getSchema(),
            tableKey.getTable());
        tableWorkingSet.add(tableKey);
        tableWorkQueue.add(tableKey);
      }
    } else if (action == Action.REMOVE_FROM_QUEUE) {
      tableWorkingSet.remove(tableKey);
    } else {
      throw new RuntimeException("Unrecognized action: " + action);
    }
  }

  /** Enqueues a work item if one is not present */
  public void enqueueWorkItemIfNeeded(final TablePartitionKey tableKey) {
    if (!tableWorkingSet.contains(tableKey)) {
      // Quick and dirty check before entering the synchronized method
      modifyTableWorkSet(tableKey, Action.ADD_TO_QUEUE);
    }
  }

  public void processWorKQueueItem(TablePartitionKey tableKey) {
    final Buffer buffer =
        bufferManager.getBufferWithIndex(
            tableKey.getDatabase(),
            tableKey.getSchema(),
            tableKey.getTable(),
            tableKey.getPartitionIndex());
    if (buffer == null) {
      LOGGER.error("Attempting to drain a buffer that no longer exists");
      return;
    }

    Drainer drainer =
        new Drainer(
            ingestEngineEpochTs,
            buffer,
            maxDurationToDrainMs,
            maxRecordsToDrain,
            maxSecondsToWaitToDrain);
    CompletableFuture.supplyAsync(drainer::drain, executor)
        .thenAccept(
            result -> {
              String message =
                  result == Drainer.TerminationReason.SUCCESS
                      ? "Able to successfully drain buffer for db={} schema={} table={}"
                      : "Unable to successfully drain buffer for db={} schema={} table={}";
              LOGGER.info(
                  message, tableKey.getDatabase(), tableKey.getSchema(), tableKey.getTable());
              modifyTableWorkSet(tableKey, Action.REMOVE_FROM_QUEUE);
            });
  }

  /**
   * The basic idea is this:
   *
   * <ul>
   *   <li>Iterate over `Buffer` instances maintained by the Buffer Manager
   *   <li>Check whether any have any outstanding data in their buffers
   *   <li>If a buffer has outstanding data then potentially enqueue it by adding it to a work set
   *       which is essentially a queue of outstanding items to process
   *   <li>Then, look at our existing working set queue. Start de-queueing items in order
   *   <li>For each item add it to our thread executor
   * </ul>
   *
   * that we iterate over queues maintained by the Buffer Manager and enqueue them to be processed
   * if there are outstanding items in the queue. We check every five seconds if there is
   * outstanding work to do.
   */
  @Override
  public void run() {
    LOGGER.info("DrainManager started");
    while (true) {
      LOGGER.trace("Starting iteration of the DrainManager");
      for (Map.Entry<TablePartitionKey, Buffer> entry :
          this.bufferManager.getTableToBuffer().entrySet()) {
        final TablePartitionKey tableKey = entry.getKey();
        final Buffer buffer = entry.getValue();
        if (buffer.hasOutstandingRows()) {
          enqueueWorkItemIfNeeded(tableKey);
        }
      }

      while (tableWorkQueue.iterator().hasNext()) {
        TablePartitionKey tablePartitionKey = tableWorkQueue.poll();
        if (tablePartitionKey == null) {
          LOGGER.error("Received a null tableKey");
          continue;
        }
        processWorKQueueItem(tablePartitionKey);
      }

      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
      }
    }
  }

  /** Shuts down the manager. */
  public void shutdown() {
    executor.shutdown();
  }
}
