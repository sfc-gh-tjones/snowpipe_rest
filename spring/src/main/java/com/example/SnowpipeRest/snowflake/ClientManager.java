package com.example.SnowpipeRest.snowflake;

import com.example.SnowpipeRest.utils.Utils;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import net.snowflake.ingest.utils.ParameterProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Snowpipe Streaming Client Manager. May return unique Clients or may share a Client. For now this
 * simply creates one Client per destination table as to not interleave.
 */
public class ClientManager {

  static final Logger LOGGER = LoggerFactory.getLogger(ClientManager.class);

  // Shared Client instance across all tables. We have relatively few tables hence one Client.
  private SnowflakeStreamingIngestClient client;

  private ClientConfig config;

  /** Initializes a Client manager backed by a single Snowpipe Streaming Client instance */
  public ClientManager() {}

  public void init(ClientConfig config) {
    this.config = config;
    this.client = buildSingletonClientInstance();
  }

  /** Returns the Client instance (currently a singleton) */
  public SnowflakeStreamingIngestClient getClient() {
    return client;
  }

  private SnowflakeStreamingIngestClient buildSingletonClientInstance() {
    if (config == null) {
      LOGGER.error("No configuration provided");
      throw new RuntimeException("Null configuration provided");
    }
    if (config.getSnowflakeUrl() == null
        || config.getSnowflakeRole() == null
        || config.getSnowflakePrivateKey() == null
        || config.getSnowflakeUser() == null) {
      LOGGER.error("Invalid configuration supplied - missing required parameter");
      throw new RuntimeException("Invalid configuration supplied - missing required parameter");
    }
    java.util.Properties props = new Properties();
    props.put("url", config.getSnowflakeUrl());
    props.put("user", config.getSnowflakeUser());
    props.put("role", config.getSnowflakeRole());
    props.put("private_key", config.getSnowflakePrivateKey());
    if (config.getMaxClientLag() != null) {
      props.put(ParameterProvider.MAX_CLIENT_LAG, config.getMaxClientLag());
    }
    try {
      return SnowflakeStreamingIngestClientFactory.builder("REST_" + Utils.getHostName())
          .setProperties(props)
          .build();
    } catch (Exception e) {
      LOGGER.error(
          "Unable to create a Client. Likely due to invalid credentials or line of sight (VPN, etc) e={}",
          e.getMessage());
      throw new RuntimeException(e);
    }
  }
}
