package com.example.SnowpipeRest.snowflake;

import com.example.SnowpipeRest.utils.TableKey;
import com.example.SnowpipeRest.utils.TablePartitionKey;
import com.example.SnowpipeRest.utils.Utils;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import net.snowflake.ingest.utils.ParameterProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Snowpipe Streaming Client Manager. May return unique Clients or may share a Client. For now this
 * simply creates one Client per destination table as to not interleave.
 */
public class ClientManager {

  static final Logger LOGGER = LoggerFactory.getLogger(ClientManager.class);

  private ClientConfig config;

  private ConcurrentHashMap<TableKey, SnowflakeStreamingIngestClient> clientsPerTable;

  private boolean useMultipleClients;
  private SnowflakeStreamingIngestClient singletonClientInstance;

  /** Initializes a Client manager backed by a single Snowpipe Streaming Client instance */
  public ClientManager() {}

  public void init(ClientConfig config) {
    this.config = config;
    this.useMultipleClients = config.shouldUseMultipleClients();
    if (!useMultipleClients) {
      singletonClientInstance = buildSingletonClientInstance();
    }
    this.clientsPerTable = new ConcurrentHashMap<>();
  }

  /** Returns the Client instance (currently a singleton) */
  public SnowflakeStreamingIngestClient getClient(TablePartitionKey tableKey) {
    TableKey tk = new TableKey(tableKey.getDatabase(), tableKey.getSchema(), tableKey.getTable());
    if (useMultipleClients) {
      if (tableKey.getTable().equals("EDR_DATA")
          || tableKey.getTable().equals("PANW_TRAFFIC_RAW")) {
        return clientsPerTable.computeIfAbsent(tk, tkk -> buildSingletonClientInstance());
      } else {
        TableKey tableKey1 = new TableKey("ALL", "ALL", "ALL");
        return clientsPerTable.computeIfAbsent(tableKey1, tkk -> buildSingletonClientInstance());
      }
    } else {
      return singletonClientInstance;
    }
  }

  /** Verifies the connection by creating a single Client instance not bound to a table */
  public boolean credentialsValid() {
    try {
      buildSingletonClientInstance();
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  SnowflakeStreamingIngestClient buildSingletonClientInstance() {
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
    if (config.getMaxChannelSizeInBytes() > 0) {
      props.put(ParameterProvider.MAX_CHANNEL_SIZE_IN_BYTES, config.getMaxChannelSizeInBytes());
    }
    if (config.getMaxChunkSizeInBytes() > 0) {
      props.put(ParameterProvider.MAX_CHUNK_SIZE_IN_BYTES, config.getMaxChunkSizeInBytes());
    }
    props.put(ParameterProvider.IO_TIME_CPU_RATIO, 0);
    props.put(
        ParameterProvider.BDEC_PARQUET_COMPRESSION_ALGORITHM, config.getCompressionAlgorithm());

    String clientName = "REST_" + Utils.getHostName();
    try {
      return SnowflakeStreamingIngestClientFactory.builder(clientName).setProperties(props).build();
    } catch (Exception e) {
      LOGGER.error(
          "Unable to create a Client. Likely due to invalid credentials or line of sight (VPN, etc) e={}",
          e.getMessage());
      throw new RuntimeException(e);
    }
  }
}
