package com.example.SnowpipeRest.utils;

import java.util.Objects;

public record TableKey(String database, String schema, String table) {

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
