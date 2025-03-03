package com.example.SnowpipeRest.buffer;

import com.example.SnowpipeRest.utils.EnqueueResponse;
import net.snowflake.ingest.utils.Pair;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

public class BufferTest {

  @Test
  public void testNoOutstandingRows() {
    Buffer buffer = new Buffer("my_db", "my_sch", "my_table", 1, 1, false, null);
    assertFalse(buffer.hasOutstandingRows());
  }

  @Test
  public void testNoOutstandingRowsWal() {
    RocksDBManager rocksDBManager = new RocksDBManager();
    Buffer buffer = new Buffer("my_db", "my_sch", "my_table", 1, 1, true, rocksDBManager);
    rocksDBManager.tearDown();
  }

  @Test
  public void testHasOutstandingRowsAllAccepted() {
    Buffer buffer = new Buffer("my_db", "my_sch", "my_table", 2, 1, false, null);
    assertFalse(buffer.hasOutstandingRows());
    String requestBody =
        "[{\"some_int\": 1, \"some_string\": \"one\"}, {\"some_int\": 2, \"some_string\": \"two\"}]";
    EnqueueResponse resp = buffer.expandRowsEnqueueData(requestBody);
    assertEquals(2, resp.getRowsEnqueued());
    assertEquals(0, resp.getRowsRejected());
    assertTrue(buffer.hasOutstandingRows());
  }

  @Test
  public void testHasOutstandingRowsAllAcceptedWAL() {
    RocksDBManager rocksDBManager = new RocksDBManager();
    Buffer buffer = new Buffer("my_db", "my_sch", "my_table", 2, 1, true, rocksDBManager);
    assertFalse(buffer.hasOutstandingRows());
    String requestBody =
        "[{\"some_int\": 1, \"some_string\": \"one\"}, {\"some_int\": 2, \"some_string\": \"two\"}]";
    EnqueueResponse resp = buffer.expandRowsEnqueueData(requestBody);
    assertEquals(2, resp.getRowsEnqueued());
    assertEquals(0, resp.getRowsRejected());
    assertTrue(buffer.hasOutstandingRows());
    rocksDBManager.tearDown();
  }

  @Test
  public void testHasOutstandingRowsPartiallyRejected() {
    Buffer buffer = new Buffer("my_db", "my_sch", "my_table", 1, 1, false, null);
    assertFalse(buffer.hasOutstandingRows());
    String requestBody =
        "[{\"some_int\": 1, \"some_string\": \"one\"}, {\"some_int\": 2, \"some_string\": \"two\"}]";
    EnqueueResponse resp = buffer.expandRowsEnqueueData(requestBody);
    assertEquals(1, resp.getRowsEnqueued());
    assertEquals(1, resp.getRowsRejected());
    assertTrue(buffer.hasOutstandingRows());
  }

  @Test
  public void testGarbageDataIn() {
    Buffer buffer = new Buffer("my_db", "my_sch", "my_table", 1, 1, false, null);
    String requestBody = "DRAINNNNNNNNN";
    EnqueueResponse resp = buffer.expandRowsEnqueueData(requestBody);
    assertEquals("Unable to parse request body", resp.getMessage());
    assertEquals(0, resp.getRowsEnqueued());
    assertEquals(0, resp.getRowsRejected());
  }

  @Test
  public void testGetAndAdvanceLatestUncommittedRow() {
    Buffer buffer = new Buffer("my_db", "my_sch", "my_table", 2, 1, false, null);
    assertFalse(buffer.hasOutstandingRows());
    String requestBody =
        "[{\"some_int\": 1, \"some_string\": \"one\"}, {\"some_int\": 2, \"some_string\": \"two\"}]";
    EnqueueResponse resp = buffer.expandRowsEnqueueData(requestBody);
    assertEquals(2, resp.getRowsEnqueued());

    Optional<Pair<Long, Map<String, Object>>> row = buffer.getAndAdvanceLatestUncommittedRow();
    assertTrue(row.isPresent());
    assertTrue(buffer.hasOutstandingRows());
    assertEquals(row.get().getFirst(), 0);
    assertEquals(row.get().getSecond().get("some_int"), 1);
    assertEquals(row.get().getSecond().get("some_string"), "one");

    row = buffer.getAndAdvanceLatestUncommittedRow();
    assertTrue(row.isPresent());
    assertFalse(buffer.hasOutstandingRows());
    assertEquals(row.get().getFirst(), 1);
    assertEquals(row.get().getSecond().get("some_int"), 2);
    assertEquals(row.get().getSecond().get("some_string"), "two");
  }

  @Test
  public void testGetAndAdvanceLatestUncommittedRowWAL() {
    RocksDBManager rocksDBManager = new RocksDBManager();
    Buffer buffer = new Buffer("my_db", "my_sch", "my_table", 2, 1, true, rocksDBManager);
    assertFalse(buffer.hasOutstandingRows());
    String requestBody =
        "[{\"some_int\": 1, \"some_string\": \"one\"}, {\"some_int\": 2, \"some_string\": \"two\"}]";
    EnqueueResponse resp = buffer.expandRowsEnqueueData(requestBody);
    assertEquals(2, resp.getRowsEnqueued());

    Optional<Pair<Long, Map<String, Object>>> row = buffer.getAndAdvanceLatestUncommittedRow();
    assertTrue(row.isPresent());
    assertTrue(buffer.hasOutstandingRows());
    assertEquals(row.get().getFirst(), 0);
    assertEquals(row.get().getSecond().get("some_int"), 1);
    assertEquals(row.get().getSecond().get("some_string"), "one");

    row = buffer.getAndAdvanceLatestUncommittedRow();
    assertTrue(row.isPresent());
    assertFalse(buffer.hasOutstandingRows());
    assertEquals(row.get().getFirst(), 1);
    assertEquals(row.get().getSecond().get("some_int"), 2);
    assertEquals(row.get().getSecond().get("some_string"), "two");
    rocksDBManager.tearDown();
  }
}
