package com.example.SnowpipeRest.snowflake;

import com.example.SnowpipeRest.utils.TableKey;
import com.example.SnowpipeRest.utils.Utils;
import com.google.common.annotations.VisibleForTesting;
import net.snowflake.ingest.streaming.DropChannelRequest;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

/** Manages Channels across tables */
public class ChannelManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ChannelManager.class);

  private static ChannelManager INSTANCE;

  public static ChannelManager getInstance() {
    if (INSTANCE == null) {
      // Lazy load this to get around local testing and Spring Boot reflection insanity
      ClientManager clientManager = new ClientManager();
      clientManager.init(new ClientConfig());
      INSTANCE = new ChannelManager(clientManager.getClient());
    }
    return INSTANCE;
  }

  @VisibleForTesting
  public static void setInstance(ChannelManager instance) {
    INSTANCE = instance;
  }

  private final ConcurrentHashMap<TableKey, SnowflakeStreamingIngestChannel> cachedChannels;

  private final SnowflakeStreamingIngestClient client;

  public ChannelManager(final SnowflakeStreamingIngestClient client) {
    this.client = client;
    this.cachedChannels = new ConcurrentHashMap<>();
  }

  /** Gets or computes a channel instance. */
  public SnowflakeStreamingIngestChannel getChannelForTable(
      String database, String schema, String table) {
    OpenChannelRequest request =
        OpenChannelRequest.builder(Utils.getHostName())
            .setDBName(database)
            .setSchemaName(schema)
            .setTableName(table)
            .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE)
            .build();
    final TableKey tableKey = new TableKey(database, schema, table);
    return cachedChannels.computeIfAbsent(tableKey, t -> client.openChannel(request));
  }

  /** Invalidates a channel by removing it from the map */
  public void invalidateChannel(String database, String schema, String table) {
    cachedChannels.remove(new TableKey(database, schema, table));
  }

  /** Removes all channels */
  public void removeAllChannels() {
    for (SnowflakeStreamingIngestChannel channel : cachedChannels.values()) {
      channel.close();
      LOGGER.info(
          "Channel closed: db={} schema={} table={} channel={}",
          channel.getDBName(),
          channel.getSchemaName(),
          channel.getTableName(),
          channel.getName());
      DropChannelRequest request =
          DropChannelRequest.builder(channel.getName())
              .setDBName(channel.getDBName())
              .setSchemaName(channel.getSchemaName())
              .setTableName(channel.getTableName())
              .build();
      client.dropChannel(request);
      LOGGER.info(
          "Channel dropped: db={} schema={} table={} channel={}",
          channel.getDBName(),
          channel.getSchemaName(),
          channel.getTableName(),
          channel.getName());
    }
  }
}
