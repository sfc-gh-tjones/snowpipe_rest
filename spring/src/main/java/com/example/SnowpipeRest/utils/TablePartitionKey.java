package com.example.SnowpipeRest.utils;

import java.util.Objects;

import static com.example.SnowpipeRest.utils.Utils.LATE_ARRIVING_PARTITION_INDEX;

public class TablePartitionKey {

  private String database;
  private String schema;
  private String table;
  private long partitionIndex;

  public TablePartitionKey(String database, String schema, String table, long partitionIndex) {
    this.database = database;
    this.schema = schema;
    this.table = table;
    this.partitionIndex = partitionIndex;
  }

  public String getDatabase() {
    return database;
  }

  public String getSchema() {
    return schema;
  }

  public String getTable() {
    return table;
  }

  public long getPartitionIndex() {
    return partitionIndex;
  }

  public boolean isLateArrivingPartition() {
    return partitionIndex == LATE_ARRIVING_PARTITION_INDEX;
  }

  @Override
  public int hashCode() {
    return Objects.hash(database, schema, table, partitionIndex);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null || getClass() != obj.getClass()) return false;
    TablePartitionKey other = (TablePartitionKey) obj;
    return database.equals(other.database)
        && Objects.equals(schema, other.schema)
        && Objects.equals(table, other.table)
        && Objects.equals(partitionIndex, other.partitionIndex);
  }
}
