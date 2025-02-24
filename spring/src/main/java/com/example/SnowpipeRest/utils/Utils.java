package com.example.SnowpipeRest.utils;

import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;

/** Set of utilities used across channels and the application */
public class Utils {

  private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);

  public enum DrainReason {
    INVALID_EPOCH,
    OFFSET_MATCHED,
    OFFSET_NEVER_MATCHED
  }

  /**
   * @return host name
   */
  public static String getHostName() {
    try {
      InetAddress localHost = InetAddress.getLocalHost();
      return localHost.getHostName(); // Returns hostname
    } catch (UnknownHostException e) {
      LOGGER.error("Unable to get channel name", e);
      throw new RuntimeException("Unable to get channel name", e);
    }
  }

  /** Gets an offset token for the current epoch */
  public static String getOffsetToken(long offsetCounter, long epochTs) {
    return offsetCounter + "-" + epochTs;
  }

  /** Given a persisted offset token, extract the buffer index */
  public static long getBufferIndexFromOffsetToken(String offsetToken) {
    String[] arr = offsetToken.split("-");
    if (arr.length != 2) {
      LOGGER.error("Invalid offset token: {}", offsetToken);
      throw new RuntimeException("Invalid offset token: " + offsetToken);
    }
    return Long.parseLong(arr[0].replace("\"", ""));
  }

  /** Given a persisted offset token, extract the epoch TS */
  public static long getEpochTsFromOffsetToken(String offsetToken) {
    String[] arr = offsetToken.split("-");
    if (arr.length != 2) {
      LOGGER.error("Invalid offset token: {}", offsetToken);
      throw new RuntimeException("Invalid offset token: " + offsetToken);
    }
    return Long.parseLong(arr[1].replace("\"", ""));
  }

  /**
   * Waits for a channel to drain
   *
   * @param ingestEngineEpochTs the epoch TS of this particular instance
   * @param channel the channel to fetch the latest persisted offset token from
   * @param lastSentOffsetToken the offset token that was last sent to Snowflake
   * @return the result of waiting to drain
   */
  public static DrainReason waitForChannelToDrain(
      int maxSecondsToWaitToDrain,
      SnowflakeStreamingIngestChannel channel,
      long ingestEngineEpochTs,
      String lastSentOffsetToken) {
    String latestPersistedOffsetTokenCached = null;
    for (int i = 0; i < maxSecondsToWaitToDrain; i++) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        //
      }
      String latestPersistedOffsetToken = channel.getLatestCommittedOffsetToken();

      if (latestPersistedOffsetToken == null || latestPersistedOffsetToken.isEmpty()) {
        // Nothing ingested yet
        continue;
      }

      long latestPersistedEpochTs = getEpochTsFromOffsetToken(latestPersistedOffsetToken);
      if (latestPersistedEpochTs > ingestEngineEpochTs) {
        // Indicates someone else opened the channel
        LOGGER.info(
            "No longer owned of this channel. latestPersistedEpochTs={} currentEpochTs={}",
            latestPersistedEpochTs,
            ingestEngineEpochTs);
        return DrainReason.INVALID_EPOCH;
      }
      latestPersistedOffsetTokenCached = latestPersistedOffsetToken;

      long persistedBufferIndex = Utils.getBufferIndexFromOffsetToken(latestPersistedOffsetToken);
      long latestSentBufferIndex = Utils.getBufferIndexFromOffsetToken(lastSentOffsetToken);
      if (latestSentBufferIndex < persistedBufferIndex) {
        LOGGER.info(
            "Ingest is lagging, going to wait for commit. latestPersistedBufferIndex={} lastSentBufferIndex={}",
            persistedBufferIndex,
            latestSentBufferIndex);
      } else {
        LOGGER.info(
            "Ingested all sent data. latestPersistedBufferIndex={} lastSentBufferIndex={}",
            persistedBufferIndex,
            latestSentBufferIndex);
        return DrainReason.OFFSET_MATCHED;
      }
    }

    LOGGER.error(
        "Server side ingest id not complete in time. maxSecondsToWait={} latestPersistedOffsetToken={} lastSentOffsetToken={}",
        maxSecondsToWaitToDrain,
        latestPersistedOffsetTokenCached,
        lastSentOffsetToken);
    return DrainReason.OFFSET_NEVER_MATCHED;
  }
}
