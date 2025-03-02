package com.example.SnowpipeRest.snowflake;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class ClientConfig {

  static final Logger LOGGER = LoggerFactory.getLogger(ClientConfig.class);

  @Value("${snowflake.url}")
  private String snowflakeUrl;

  @Value("${snowflake.user}")
  private String snowflakeUser;

  @Value("${snowflake.role}")
  private String snowflakeRole;

  @Value("${snowflake.private_key}")
  private String snowflakePrivateKey;

  @Value("${rest_api.max_client_lag}")
  private String maxClientLag;

  @Value("${rest_api.default_compression_algorithm}")
  private String defaultCompressionAlgorithm;

  @Value("${rest_api.drain_manager_max_channel_size_in_bytes}")
  private int maxChannelSizeInBytes;

  @Value("${rest_api.drain_manager_max_chunk_size_in_bytes}")
  private int maxChunkSizeInBytes;

  private void checkEnv(String envName) {
    String val = System.getenv(envName);
    if (val == null || val.isEmpty()) {
      LOGGER.error("Environment variable not set. var={}", envName);
      throw new RuntimeException("Environment variable not set. var=" + envName);
    }
  }

  private long getEnv(String envName) {
    String envVal = System.getenv(envName);
    if (envVal == null || envVal.isEmpty()) {
      return 0;
    }
    return Integer.parseInt(envVal);
  }

  public long getMaxChunkSizeInBytes() {
    if (maxChunkSizeInBytes <= 0) {
      String envName = "REST_API_DRAIN_MANAGER_MAX_CHUNK_SIZE_IN_BYTES";
      LOGGER.info(
          "Max chunk size in bytes is 0, default to REST_API_DRAIN_MANAGER_MAX_CHUNK_SIZE_IN_BYTES environment variable");
      return getEnv(envName);
    }
    return maxChunkSizeInBytes;
  }

  public long getMaxChannelSizeInBytes() {
    if (maxChannelSizeInBytes <= 0) {
      String envName = "REST_API_DRAIN_MANAGER_MAX_CHANNEL_SIZE_IN_BYTES";
      LOGGER.info(
          "Max chunk size in bytes is 0, default to REST_API_DRAIN_MANAGER_MAX_CHANNEL_SIZE_IN_BYTES environment variable");
      // checkEnv(envName);
      return getEnv(envName);
    }
    return maxChannelSizeInBytes;
  }

  public String getSnowflakeUrl() {
    if (snowflakeUrl == null) {
      LOGGER.info("Defaulting to SNOWFLAKE_URL environment variable");
      return System.getenv("SNOWFLAKE_URL");
    }
    return snowflakeUrl;
  }

  public String getSnowflakeUser() {
    if (snowflakeUser == null) {
      LOGGER.info("Defaulting to SNOWFLAKE_USER environment variable");
      return System.getenv("SNOWFLAKE_USER");
    }
    return snowflakeUser;
  }

  public String getSnowflakeRole() {
    if (snowflakeRole == null) {
      LOGGER.info("Defaulting to SNOWFLAKE_ROLE environment variable");
      return System.getenv("SNOWFLAKE_ROLE");
    }
    return snowflakeRole;
  }

  public String getSnowflakePrivateKey() {
    if (snowflakePrivateKey == null) {
      LOGGER.info("Defaulting to SNOWFLAKE_PRIVATE_KEY environment variable");
      return System.getenv("SNOWFLAKE_PRIVATE_KEY");
    }
    return snowflakePrivateKey;
  }

  public String getMaxClientLag() {
    if (maxClientLag == null) {
      LOGGER.info("Defaulting to REST_API_MAX_CLIENT_LAG environment variable");
      return System.getenv("REST_API_MAX_CLIENT_LAG");
    }
    return maxClientLag;
  }

  public boolean shouldUseMultipleClients() {
    String isSet = System.getenv("REST_API_USE_MULTIPLE_CLIENTS");
    if (isSet == null || isSet.isEmpty()) {
      LOGGER.info("Defaulting to using multiple clients");
      return true;
    }
    return Boolean.parseBoolean(isSet);
  }

  public String getCompressionAlgorithm() {
    if (defaultCompressionAlgorithm == null) {
      LOGGER.info("Defaulting to REST_API_DEFAULT_COMPRESSION_ALGORITHM environment variable");
      String ret = System.getenv("REST_API_DEFAULT_COMPRESSION_ALGORITHM");
      if (ret == null || ret.isEmpty()) {
        LOGGER.info("Default to ZSTD");
        return "ZSTD";
      }
      return ret;
    }
    return defaultCompressionAlgorithm;
  }
}
