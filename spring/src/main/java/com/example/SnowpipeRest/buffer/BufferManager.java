package com.example.SnowpipeRest.buffer;

import com.example.SnowpipeRest.utils.TableKey;
import com.example.SnowpipeRest.utils.TablePartitionKey;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/** Manages buffers used to hold data for a destination table */
@Component
public class BufferManager {

  //
  ConcurrentHashMap<TableKey, AtomicInteger> tableToPartitionIndex;

  // Map of table identifier to buffer
  ConcurrentHashMap<TablePartitionKey, Buffer> tableToBuffer;

  long maxBufferRowCount;

  long maxShardsPerTable;

  private long getPartitionIndex(AtomicInteger atomicInteger) {
    return atomicInteger.incrementAndGet() % maxShardsPerTable;
  }

  /** Default constructor */
  public BufferManager(long maxBufferRowCount, long maxShardsPerTable) {
    tableToBuffer = new ConcurrentHashMap<>();
    tableToPartitionIndex = new ConcurrentHashMap<>();
    this.maxBufferRowCount = maxBufferRowCount;
    this.maxShardsPerTable = maxShardsPerTable;
  }

  public Buffer getBuffer(final String database, final String schema, final String table) {
    final TableKey key = new TableKey(database, schema, table);
    AtomicInteger counter = tableToPartitionIndex.computeIfAbsent(key, k -> new AtomicInteger(0));
    long partitionIndex = getPartitionIndex(counter);
    TablePartitionKey pk = new TablePartitionKey(database, schema, table, partitionIndex);
    return tableToBuffer.computeIfAbsent(
        pk, k -> new Buffer(database, schema, table, maxBufferRowCount, partitionIndex));
  }

  public Buffer getBufferWithIndex(
      final String database, final String schema, final String table, final long partitionIndex) {
    TablePartitionKey pk = new TablePartitionKey(database, schema, table, partitionIndex);
    return tableToBuffer.get(pk);
  }

  /**
   * Returns the mapping of keys to buffers maintained by this instance
   *
   * @return
   */
  public ConcurrentHashMap<TablePartitionKey, Buffer> getTableToBuffer() {
    return tableToBuffer;
  }
}
