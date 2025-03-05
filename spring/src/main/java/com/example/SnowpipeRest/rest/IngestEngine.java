package com.example.SnowpipeRest.rest;

import com.example.SnowpipeRest.buffer.DrainManager;
import com.example.SnowpipeRest.snowflake.ChannelManager;
import com.example.SnowpipeRest.utils.EnqueueResponse;
import com.example.SnowpipeRest.buffer.BufferManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Responsible for ingesting data to a Snowflake table from a REST request. The general strategy is:
 *
 * <ul>
 *   <li>Enqueue requests arrive from one or more threads
 *   <li>Each thread enqueues a request in an in-memory buffer
 *   <li>After enqueue, the thread responds back to the Client saying that data has been received,
 *       but not yet committed
 * </ul>
 *
 * - Enqueue requests can come in from one or more threads - Each thread
 */
public class IngestEngine {

  private static final Logger LOGGER = LoggerFactory.getLogger(IngestEngine.class);

  private final BufferManager bufferManager;

  private final DrainManager drainManager;

  // This is a fun one. Basically the idea is that this gets set once, and we use this as an epoch
  // of the app. This is used to reason about replay and the like later on
  private final long epochTs;

  private final ScheduledExecutorService executorService;

  /**
   * Default constructor. Note that this MUST be empty due to how Spring does property to BEAN
   * binding.
   */
  public IngestEngine(
      long maxBufferRowCount,
      long numThreads,
      long maxDurationToDrainMs,
      long maxRecordsToDrain,
      int maxSecondsToWaitToDrain,
      long maxShardsPerTable,
      boolean persistentWAL) {
    LOGGER.info("Initializing Ingest Engine...");
    this.bufferManager = new BufferManager(maxBufferRowCount, maxShardsPerTable, persistentWAL);
    this.epochTs = System.currentTimeMillis();
    this.drainManager =
        new DrainManager(
            epochTs,
            bufferManager,
            (int) numThreads,
            maxDurationToDrainMs,
            maxRecordsToDrain,
            maxSecondsToWaitToDrain,
            persistentWAL);
    executorService = Executors.newSingleThreadScheduledExecutor();
    executorService.scheduleWithFixedDelay(drainManager::run, 1, 1, TimeUnit.SECONDS);
    LOGGER.info("Scheduled run of Drain Manager");
  }

  /**
   * Enqueues data to be inserted into a table
   *
   * @param database the destination database
   * @param schema the destination schema
   * @param table the destination table
   * @param requestData the application supplied request body containing one or more rows
   * @return response indicating what was accepted, rejected, etc
   */
  public EnqueueResponse enqueueData(
      final String database, final String schema, final String table, final String requestData) {
    return bufferManager.getBuffer(database, schema, table).expandRowsEnqueueData(requestData);
  }

  /**
   * Shuts down the ingest engine. This does the following:
   *
   * <ul>
   *   <li>Suspends any threads running in the background via the DrainManager class
   *   <li>Causes close on any outstanding channels, effectively draining them
   *   <li>Drops the channels managed by this instance
   * </ul>
   */
  public void shutDown() {
    drainManager.shutdown();
    // bufferManager.tearDown();
    ChannelManager.getInstance().removeAllChannels();
  }
}
