package com.example.SnowpipeRest.utils;

import java.util.Objects;

public class TableKey {

  private String database;
  private String schema;
  private String table;

  public TableKey(String database, String schema, String table) {
    this.database = database;
    this.schema = schema;
    this.table = table;
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

  @Override
  public int hashCode() {
    return Objects.hash(database, schema, table);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null || getClass() != obj.getClass()) return false;
    TableKey other = (TableKey) obj;
    return database.equals(other.database)
        && Objects.equals(schema, other.schema)
        && Objects.equals(table, other.table);
  }
}
