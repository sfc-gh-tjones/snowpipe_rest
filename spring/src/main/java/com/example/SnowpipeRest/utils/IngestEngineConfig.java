package com.example.SnowpipeRest.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class IngestEngineConfig {

  static final Logger LOGGER = LoggerFactory.getLogger(IngestEngineConfig.class);

  // The maximum row count that any buffer should have
  @Value("${rest_api.buffer_manager_max_buffer_row_count}")
  private long maxBufferRowCount;

  @Value("${rest_api.buffer_manager_max_shards_per_table}")
  private long maxShardsPerTable;

  @Value("${rest_api.buffer_manager_use_wal}")
  private boolean useWAL;

  @Value("${rest_api.split_late_arriving_rows}")
  private boolean splitLateArrivingRows;

  @Value("${rest_api.drain_manager_num_threads}")
  private long numThreads;

  @Value("${rest_api.drain_manager_max_duration_to_drain_ms}")
  private long maxDurationToDrainMs;

  @Value("${rest_api.drain_manager_max_records_to_drain}")
  private long maxRecordsToDrain;

  @Value("${rest_api.drain_manager_max_seconds_to_wait_to_drain}")
  private int maxSecondsToWaitToDrain;

  private void checkEnv(String envName) {
    String val = System.getenv(envName);
    if (val == null || val.isEmpty()) {
      LOGGER.error("Environment variable not set. var={}", envName);
      throw new RuntimeException("Environment variable not set. var=" + envName);
    }
  }

  private boolean isEnvSet(String envName) {
    String val = System.getenv(envName);
    return val != null && !val.isEmpty();
  }

  private long getEnv(String envName) {
    return Integer.parseInt(System.getenv(envName));
  }

  public long getMaxShardsPerTable() {
    if (maxShardsPerTable <= 0) {
      checkEnv("REST_API_BUFFER_MANAGER_MAX_SHARDS_PER_TABLE");
      return getEnv("REST_API_BUFFER_MANAGER_MAX_SHARDS_PER_TABLE");
    }
    return maxShardsPerTable;
  }

  public boolean getUseWAL() {
    if (useWAL == false) {
      // Double check that it is not set at the environment level
      String env = "REST_API_BUFFER_MANAGER_USE_WAL";
      boolean isSet = isEnvSet(env);
      if (isSet) {
        String val = System.getenv(env);
        return Boolean.parseBoolean(val);
      }
    }
    return useWAL;
  }

  public long getMaxBufferRowCount() {
    if (maxBufferRowCount <= 0) {
      LOGGER.info(
          "Max buffer row count is set to 0, default to REST_API_BUFFER_MANAGER_MAX_BUFFER_ROW_COUNT environment variable");
      checkEnv("REST_API_BUFFER_MANAGER_MAX_BUFFER_ROW_COUNT");
      return getEnv("REST_API_BUFFER_MANAGER_MAX_BUFFER_ROW_COUNT");
    }
    return maxBufferRowCount;
  }

  public long getNumThreads() {
    if (numThreads <= 0) {
      LOGGER.info(
          "Num threads is set to 0, default to REST_API_DRAIN_MANAGER_NUM_THREADS environment variable");
      checkEnv("REST_API_DRAIN_MANAGER_NUM_THREADS");
      return getEnv("REST_API_DRAIN_MANAGER_NUM_THREADS");
    }
    return numThreads;
  }

  public long getMaxDurationToDrainMs() {
    if (maxDurationToDrainMs <= 0) {
      LOGGER.info(
          "Max duration to drain ms set to 0, default to REST_API_DRAIN_MANAGER_MAX_DURATION_TO_DRAIN_MS  environment variable");
      checkEnv("REST_API_DRAIN_MANAGER_MAX_DURATION_TO_DRAIN_MS");
      return getEnv("REST_API_DRAIN_MANAGER_MAX_DURATION_TO_DRAIN_MS");
    }
    return maxDurationToDrainMs;
  }

  public long getMaxRecordsToDrain() {
    if (maxRecordsToDrain <= 0) {
      LOGGER.info(
          "Max records to drain set to 0, default to REST_API_DRAIN_MANAGER_MAX_RECORDS_TO_DRAIN environment variable");
      checkEnv("REST_API_DRAIN_MANAGER_MAX_RECORDS_TO_DRAIN");
      return getEnv("REST_API_DRAIN_MANAGER_MAX_RECORDS_TO_DRAIN");
    }
    return maxRecordsToDrain;
  }

  public int getMaxSecondsToWaitToDrain() {
    if (maxSecondsToWaitToDrain <= 0) {
      LOGGER.info(
          "Max records to drain set to 0, default to REST_API_DRAIN_MANAGER_MAX_SECONDS_TO_WAIT_TO_DRAIN environment variable");
      checkEnv("REST_API_DRAIN_MANAGER_MAX_SECONDS_TO_WAIT_TO_DRAIN");
      return (int) getEnv("REST_API_DRAIN_MANAGER_MAX_SECONDS_TO_WAIT_TO_DRAIN");
    }
    return maxSecondsToWaitToDrain;
  }

  public boolean getSplitLateArrivingRows() {
    if (!splitLateArrivingRows) {
      // Double check that it is not set at the environment level
      String env = "REST_API_BUFFER_MANAGER_SPLIT_LATE_ARRIVING_ROWS";
      boolean isSet = isEnvSet(env);
      if (isSet) {
        String val = System.getenv(env);
        return Boolean.parseBoolean(val);
      }
    }
    return splitLateArrivingRows;
  }
}
