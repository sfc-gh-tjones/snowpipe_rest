package com.example.SnowpipeRest.buffer;

import com.example.SnowpipeRest.snowflake.ChannelManager;
import com.example.SnowpipeRest.utils.Utils;
import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.utils.Pair;
import net.snowflake.ingest.utils.SFException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;

/** Responsible for draining a buffer and sending it to Snowflake */
public class Drainer {

  static final Logger LOGGER = LoggerFactory.getLogger(Drainer.class);

  enum TerminationReason {
    SUCCESS,
    CHANNEL_ERROR,
    UNEXPECTED_ERROR
  }

  // The buffer that we should be draining from
  Buffer buffer;

  // Maximum time or records to drain before giving back to the main thread
  long maxRecordsToDrain;
  long maxDurationToDrainMs;

  long ingestEngineEpochTs;

  int maxSecondsToWaitToDrain;

  /**
   * Default constructor. Takes in two parameters that dictate when the method should be returned,
   * barring errors originating from a Channel instance. The thread will return based on whatever
   * condition is hit first. Ex: duration is 100ms and max records are 10: if we hit 10 records in
   * 10ms then we'll return at 10ms but if we only get 9 records in 100ms then we'll return at
   * 100ms.
   *
   * @param maxDurationToDrainMs the maximum duration that we should be draining in an invocation,
   *     in MS
   * @param maxRecordsToDrain the maximum row count that we should be draining in an invocation, in
   *     MS
   */
  Drainer(
      final long ingestEngineEpochTs,
      Buffer buffer,
      long maxDurationToDrainMs,
      long maxRecordsToDrain,
      int maxSecondsToWaitToDrain) {
    this.ingestEngineEpochTs = ingestEngineEpochTs;
    this.buffer = buffer;
    this.maxRecordsToDrain = maxRecordsToDrain;
    this.maxDurationToDrainMs = maxDurationToDrainMs;
    this.maxSecondsToWaitToDrain = maxSecondsToWaitToDrain;
  }

  private boolean abortDueToLimits(long drainStartTimeMs, long recordsDrained) {
    long now = System.currentTimeMillis();
    if (now - drainStartTimeMs > maxDurationToDrainMs) {
      LOGGER.info(
          "Hit the elapsed time, returning to the thread pool. now={} elapsedMs={} maxDurationToDrainMs={} db={} schema={} table={}",
          now,
          now - drainStartTimeMs,
          maxDurationToDrainMs,
          buffer.getDatabase(),
          buffer.getSchema(),
          buffer.getTable());
      return true;
    }
    if (recordsDrained >= maxRecordsToDrain) {
      LOGGER.info(
          "Hit the max records, returning to the thread pool.... max/drained={} db={} schema={} table={}",
          maxRecordsToDrain,
          buffer.getDatabase(),
          buffer.getSchema(),
          buffer.getTable());
      return true;
    }
    return false;
  }

  private void waitForMoreData() {
    try {
      Thread.sleep(10);
    } catch (InterruptedException e) {
    }
  }

  private void logDrainExitCriteriaReached(Buffer buffer, long recordsDrained) {
    LOGGER.info(
        "Drain exit criteria reached. db={} schema={} table={} recordsDrained={}",
        buffer.getDatabase(),
        buffer.getSchema(),
        buffer.getTable(),
        recordsDrained);
  }

  private void logOutstandingDataError(Buffer buffer, SFException e) {
    LOGGER.error(
        "The channel has either been closed or reopened but we still have data in our buffer. db={} schema={} table={} vendorCode={} msg={}",
        buffer.getDatabase(),
        buffer.getSchema(),
        buffer.getTable(),
        e.getVendorCode(),
        e.getMessage());
  }

  // TODO: this is admittedly pretty weak error handling but the basic idea is that we'll
  // continue to ingest rows per above and simply log that we received errors for now.
  private void logResponseErrors(InsertValidationResponse response) {
    for (InsertValidationResponse.InsertError err : response.getInsertErrors()) {
      LOGGER.error(
          "Unable to insert row. db={} schema={} table={} rowIndex={} msg={} err={}",
          buffer.getDatabase(),
          buffer.getSchema(),
          buffer.getTable(),
          err.getRowIndex(),
          err.getException() != null ? err.getException().getMessage() : "",
          err);
    }
  }

  private void logInvalidChannel(Buffer buffer, SnowflakeStreamingIngestChannel channel) {
    LOGGER.info(
        "Attempting to re-open the channel due to being an invalid channel db={} schema={} table={} channel={}",
        buffer.getDatabase(),
        buffer.getSchema(),
        buffer.getTable(),
        channel.getName());
  }

  /** Santa Cruz hardcore represent! */
  public TerminationReason drain() {
    LOGGER.info(
        "Invoking drain for a buffer. db={} schema={} table={}",
        buffer.getDatabase(),
        buffer.getSchema(),
        buffer.getTable());

    try {
      long drainStartTimeMs = System.currentTimeMillis();
      long recordsDrained = 0;
      SnowflakeStreamingIngestChannel channel =
          ChannelManager.getInstance()
              .getChannelForTable(buffer.getDatabase(), buffer.getSchema(), buffer.getTable());
      if (!channel.isValid()) {
        logInvalidChannel(buffer, channel);
        ChannelManager.getInstance()
            .invalidateChannel(buffer.getDatabase(), buffer.getSchema(), buffer.getTable());
        channel =
            ChannelManager.getInstance()
                .getChannelForTable(buffer.getDatabase(), buffer.getSchema(), buffer.getTable());
      }
      String lastSentOffsetToken = null;

      while (true) {
        if (abortDueToLimits(drainStartTimeMs, recordsDrained)) {
          logDrainExitCriteriaReached(buffer, recordsDrained);
          Utils.DrainReason reason =
              Utils.waitForChannelToDrain(
                  maxSecondsToWaitToDrain, channel, ingestEngineEpochTs, lastSentOffsetToken);
          LOGGER.info("Exiting drain loop drainReason={}", reason.name());
          return TerminationReason.SUCCESS;
        }

        Optional<Pair<Long, Map<String, Object>>> row = buffer.getAndAdvanceLatestUncommittedRow();
        if (row.isEmpty()) {
          waitForMoreData();
          continue;
        }
        recordsDrained++;

        // Send that row to the channel using the offset token in the queue along with the data
        String offsetToken = Utils.getOffsetToken(row.get().getFirst(), ingestEngineEpochTs);
        Map<String, Object> rowData = row.get().getSecond();

        InsertValidationResponse response;
        try {
          response = channel.insertRow(rowData, offsetToken);
        } catch (SFException e) {
          // This indicates that the channel has been closed or is now invalid. So we have to reopen
          // it and go from there. We do this by essentially removing it from the map and re-opening
          // on the next go around. This banks heavily on a single thread invoking this method
          // on a per-table basis as managed in `DrainManager`, otherwise there may be concurrency
          // issues wherein someone attempts to use a channel that is being removed.
          logOutstandingDataError(buffer, e);
          ChannelManager.getInstance()
              .invalidateChannel(buffer.getDatabase(), buffer.getSchema(), buffer.getTable());
          return TerminationReason.CHANNEL_ERROR;
        }

        lastSentOffsetToken = offsetToken;

        if (response.hasErrors()) {
          logResponseErrors(response);
        }
      }

    } catch (Exception e) {
      LOGGER.error("Unexpected error. Invalidating channel as a get out of jail free card", e);
      ChannelManager.getInstance()
          .invalidateChannel(buffer.getDatabase(), buffer.getSchema(), buffer.getTable());
    }
    return TerminationReason.UNEXPECTED_ERROR;
  }
}
