package com.example.SnowpipeRest.buffer;

import com.example.SnowpipeRest.utils.Utils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Optional;

public class RocksDBTest {

  @Disabled
  @Test
  public void testRocksDBInit() {
    RocksDBManager manager = new RocksDBManager();
    manager.initialize();
  }

  @Disabled
  @Test
  public void testRocksDBPut() {
    RocksDBManager manager = new RocksDBManager();
    manager.initialize();
    String database = "db";
    String schema = "sch";
    String table = "mytable";
    int partitionIndex = 1;
    long offset = 1;
    String testKey = Utils.getKeyForWAL(database, schema, table, partitionIndex, offset);
    String testPayload = "'[{\"a\": 1, \"b\": \"one\"}]";
    boolean wroteData = manager.writeToDB(testKey, testPayload);
    Assertions.assertTrue(wroteData);
    Optional<String> data = manager.readFromDB(testKey);
    Assertions.assertTrue(data.isPresent());
    Assertions.assertEquals(testPayload, data.get());
  }
}
