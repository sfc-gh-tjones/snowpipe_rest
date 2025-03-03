package com.example.SnowpipeRest.buffer;

import com.example.SnowpipeRest.utils.TableKey;
import com.example.SnowpipeRest.utils.TablePartitionKey;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/** Manages buffers used to hold data for a destination table */
@Component
public class BufferManager {

  static Set<String> highVolumeTables = new HashSet<>();

  static {
    highVolumeTables.add("EDR_DATA");
  }

  //
  ConcurrentHashMap<TableKey, AtomicInteger> tableToPartitionIndex;

  // Map of table identifier to buffer
  ConcurrentHashMap<TablePartitionKey, Buffer> tableToBuffer;

  long maxBufferRowCount;

  long maxShardsPerTable;

  boolean usePersistentWriteAheadLog;

  private RocksDBManager rocksDBManager;

  private long getPartitionIndex(AtomicInteger atomicInteger, String tableName) {
    if (highVolumeTables.contains(tableName.toUpperCase())) {
      // Partition our higher volume tables
      return atomicInteger.incrementAndGet() % maxShardsPerTable;
    }
    return 0;
  }

  /** Default constructor */
  public BufferManager(
      long maxBufferRowCount, long maxShardsPerTable, boolean usePersistentWriteAheadLog) {
    tableToBuffer = new ConcurrentHashMap<>();
    tableToPartitionIndex = new ConcurrentHashMap<>();
    this.maxBufferRowCount = maxBufferRowCount;
    this.maxShardsPerTable = maxShardsPerTable;
    this.usePersistentWriteAheadLog = usePersistentWriteAheadLog;
    if (usePersistentWriteAheadLog) {
      rocksDBManager = new RocksDBManager();
      rocksDBManager.initialize();
    }
  }

  public Buffer getBuffer(final String database, final String schema, final String table) {
    final TableKey key = new TableKey(database, schema, table);
    AtomicInteger counter = tableToPartitionIndex.computeIfAbsent(key, k -> new AtomicInteger(0));
    long partitionIndex = getPartitionIndex(counter, table);
    TablePartitionKey pk = new TablePartitionKey(database, schema, table, partitionIndex);
    return tableToBuffer.computeIfAbsent(
        pk,
        k ->
            new Buffer(
                database,
                schema,
                table,
                maxBufferRowCount,
                partitionIndex,
                usePersistentWriteAheadLog,
                rocksDBManager));
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
