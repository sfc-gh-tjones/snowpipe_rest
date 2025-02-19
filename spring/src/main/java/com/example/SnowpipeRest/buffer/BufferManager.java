package com.example.SnowpipeRest.buffer;

import com.example.SnowpipeRest.utils.TableKey;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;

/** Manages buffers used to hold data for a destination table */
@Component
public class BufferManager {

  // Map of table identifier to buffer
  ConcurrentHashMap<TableKey, Buffer> tableToBuffer;

  long maxBufferRowCount;

  /** Default constructor */
  public BufferManager(long maxBufferRowCount) {
    tableToBuffer = new ConcurrentHashMap<>();
    this.maxBufferRowCount = maxBufferRowCount;
  }

  public Buffer getBuffer(final String database, final String schema, final String table) {
    final TableKey key = new TableKey(database, schema, table);
    return tableToBuffer.computeIfAbsent(
        key, k -> new Buffer(database, schema, table, maxBufferRowCount));
  }

  /**
   * Returns the mapping of keys to buffers maintained by this instance
   *
   * @return
   */
  public ConcurrentHashMap<TableKey, Buffer> getTableToBuffer() {
    return tableToBuffer;
  }
}
