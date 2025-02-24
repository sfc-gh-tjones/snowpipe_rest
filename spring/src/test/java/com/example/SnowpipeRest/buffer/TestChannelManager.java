package com.example.SnowpipeRest.buffer;

import com.example.SnowpipeRest.snowflake.ChannelManager;
import com.example.SnowpipeRest.snowflake.ClientManager;
import com.example.SnowpipeRest.utils.TableKey;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;

import java.util.HashMap;
import java.util.Map;

public class TestChannelManager extends ChannelManager {

  final Map<TableKey, SnowflakeStreamingIngestChannel> channels;

  final boolean throwSFExceptionOnInsert;
  final boolean returnResponseWithErrors;

  public TestChannelManager(
      SnowflakeStreamingIngestClient client,
      boolean throwSFExceptionOnInsert,
      boolean returnResponseWithErrors) {
    super(new ClientManager());
    this.channels = new HashMap<>();
    this.throwSFExceptionOnInsert = throwSFExceptionOnInsert;
    this.returnResponseWithErrors = returnResponseWithErrors;
  }

  /**
   * Gets or computes a channel instance.
   *
   * @param database
   * @param schema
   * @param table
   */
  @Override
  public SnowflakeStreamingIngestChannel getChannelForTable(
      String database, String schema, String table) {
    return channels.computeIfAbsent(
        new TableKey(database, schema, table),
        t -> new TestChannel(throwSFExceptionOnInsert, returnResponseWithErrors));
  }
}
