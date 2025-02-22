package com.example.SnowpipeRest.buffer;

import com.example.SnowpipeRest.snowflake.ChannelManager;
import com.example.SnowpipeRest.utils.TableKey;
import com.example.SnowpipeRest.utils.Utils;
import net.snowflake.ingest.utils.Pair;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;

public class DrainManagerTest {

  private void verifyRowsForChannel(TestChannel channel, int maxRows) {
    for (int i = 0; i < maxRows; i++) {
      Pair<Map<String, Object>, String> row = channel.insertedRows.get(i);
      assertEquals(i, Utils.getBufferIndexFromOffsetToken(row.getSecond()));
    }
  }

  @Test
  public void testDrainManagerLifecycle() {
    BufferManager bufferManager = new BufferManager(100);
    DrainManager drainManager = new DrainManager(1234, bufferManager, 1, 0, 0, 120);
    assertEquals(0, drainManager.getTableWorkSet().size());
    assertEquals(0, drainManager.getTableWorkQueue().size());
    drainManager.shutdown();
  }

  @Test
  public void testDrainOfSingleChannel() throws InterruptedException {
    TestChannelManager channelManager = new TestChannelManager(null, false, false);
    ChannelManager.setInstance(channelManager);

    BufferManager bufferManager = new BufferManager(100);
    DrainManager drainManager = new DrainManager(1234, bufferManager, 1, 1000, 10, 120);

    final String databaseName = "my_db";
    final String schemaName = "my_sch";
    final String tableName = "my_table";
    final String requestBody =
        "[{\"some_int\": 1, \"some_string\": \"one\"}, {\"some_int\": 2, \"some_string\": \"two\"}]";
    bufferManager.getBuffer(databaseName, schemaName, tableName).expandRowsEnqueueData(requestBody);

    ExecutorService executor = Executors.newSingleThreadExecutor();
    executor.execute(drainManager);
    Thread.sleep(5000);
    executor.shutdown();

    assertEquals(1, channelManager.channels.keySet().size());
    TestChannel channel =
        (TestChannel) channelManager.channels.get(new TableKey("my_db", "my_sch", "my_table"));
    assertNotNull(channel);
    assertEquals(2, channel.insertedRows.size());
    verifyRowsForChannel(channel, 2);
  }

  @Test
  public void testDrainOfSingleChannelSecondAppend() throws InterruptedException {
    TestChannelManager channelManager = new TestChannelManager(null, false, false);
    ChannelManager.setInstance(channelManager);

    BufferManager bufferManager = new BufferManager(100);
    DrainManager drainManager = new DrainManager(1234, bufferManager, 1, 1000, 10, 120);

    final String databaseName = "my_db";
    final String schemaName = "my_sch";
    final String tableName = "my_table";
    final String requestBody =
        "[{\"some_int\": 1, \"some_string\": \"one\"}, {\"some_int\": 2, \"some_string\": \"two\"}]";
    bufferManager.getBuffer(databaseName, schemaName, tableName).expandRowsEnqueueData(requestBody);

    ExecutorService executor = Executors.newSingleThreadExecutor();
    executor.execute(drainManager);
    bufferManager.getBuffer(databaseName, schemaName, tableName).expandRowsEnqueueData(requestBody);
    Thread.sleep(5000);
    executor.shutdown();

    assertEquals(1, channelManager.channels.keySet().size());
    TestChannel channel =
        (TestChannel) channelManager.channels.get(new TableKey("my_db", "my_sch", "my_table"));
    assertNotNull(channel);
    assertEquals(4, channel.insertedRows.size());
    verifyRowsForChannel(channel, 4);
  }

  @Test
  public void testDrainOfSingleChannelTwice() throws InterruptedException {
    TestChannelManager channelManager = new TestChannelManager(null, false, false);
    ChannelManager.setInstance(channelManager);

    BufferManager bufferManager = new BufferManager(100);
    DrainManager drainManager = new DrainManager(1234, bufferManager, 1, 1000, 10, 120);

    final String databaseName = "my_db";
    final String schemaName = "my_sch";
    final String tableName = "my_table";
    final String requestBody =
        "[{\"some_int\": 1, \"some_string\": \"one\"}, {\"some_int\": 2, \"some_string\": \"two\"}]";
    bufferManager.getBuffer(databaseName, schemaName, tableName).expandRowsEnqueueData(requestBody);

    ExecutorService executor = Executors.newSingleThreadExecutor();
    executor.execute(drainManager);
    Thread.sleep(5000);
    executor.shutdown();

    assertEquals(1, channelManager.channels.keySet().size());
    TestChannel channel =
        (TestChannel) channelManager.channels.get(new TableKey("my_db", "my_sch", "my_table"));
    assertNotNull(channel);
    assertEquals(2, channel.insertedRows.size());
    verifyRowsForChannel(channel, 2);

    // Now insert two more rows and run the executor again
    bufferManager.getBuffer(databaseName, schemaName, tableName).expandRowsEnqueueData(requestBody);
    assertTrue(bufferManager.getBuffer(databaseName, schemaName, tableName).hasOutstandingRows());
    executor = Executors.newSingleThreadExecutor();
    executor.execute(drainManager);
    Thread.sleep(5000);
    executor.shutdown();

    assertEquals(1, channelManager.channels.keySet().size());
    channel =
        (TestChannel) channelManager.channels.get(new TableKey("my_db", "my_sch", "my_table"));
    assertNotNull(channel);
    assertEquals(4, channel.insertedRows.size());
    verifyRowsForChannel(channel, 4);
  }

  // Second task will be initially rejected but then later enqueued
  @Test
  public void testTwoTablesOneThread() throws InterruptedException {
    TestChannelManager channelManager = new TestChannelManager(null, false, false);
    ChannelManager.setInstance(channelManager);

    BufferManager bufferManager = new BufferManager(100);
    DrainManager drainManager = new DrainManager(1234, bufferManager, 1, 1000, 10, 120);

    final String databaseName = "my_db";
    final String schemaName = "my_sch";
    final String tableName = "my_table";
    final String requestBody =
        "[{\"some_int\": 1, \"some_string\": \"one\"}, {\"some_int\": 2, \"some_string\": \"two\"}]";
    bufferManager.getBuffer(databaseName, schemaName, tableName).expandRowsEnqueueData(requestBody);

    final String databaseName2 = "my_db_2";
    final String schemaName2 = "my_sch_2";
    final String tableName2 = "my_table_2";
    bufferManager
        .getBuffer(databaseName2, schemaName2, tableName2)
        .expandRowsEnqueueData(requestBody);

    ExecutorService executor = Executors.newSingleThreadExecutor();
    executor.execute(drainManager);
    Thread.sleep(5000);
    executor.shutdown();

    assertEquals(2, channelManager.channels.keySet().size());

    TestChannel channel =
        (TestChannel) channelManager.channels.get(new TableKey("my_db", "my_sch", "my_table"));
    assertNotNull(channel);
    assertEquals(2, channel.insertedRows.size());
    verifyRowsForChannel(channel, 2);

    TestChannel channel2 =
        (TestChannel)
            channelManager.channels.get(new TableKey("my_db_2", "my_sch_2", "my_table_2"));
    assertNotNull(channel2);
    assertEquals(2, channel2.insertedRows.size());
    verifyRowsForChannel(channel2, 2);
  }

  @Test
  public void testMultipleTablesMultipleThreads() throws InterruptedException {
    TestChannelManager channelManager = new TestChannelManager(null, false, false);
    ChannelManager.setInstance(channelManager);

    BufferManager bufferManager = new BufferManager(2000);
    DrainManager drainManager = new DrainManager(1234, bufferManager, 10, 1000, 1000, 120);
    final String requestBody =
        "[{\"some_int\": 1, \"some_string\": \"one\"}, {\"some_int\": 2, \"some_string\": \"two\"}]";
    for (int i = 0; i < 10; i++) {
      String databaseName = "my_db_" + i;
      String schemaName = "my_schema_" + i;
      String tableName = "my_schema_" + i;
      for (int j = 0; j < 1000; j++) {
        bufferManager
            .getBuffer(databaseName, schemaName, tableName)
            .expandRowsEnqueueData(requestBody);
      }
    }

    ExecutorService executor = Executors.newSingleThreadExecutor();
    executor.execute(drainManager);
    Thread.sleep(5000);
    executor.shutdown();

    assertEquals(10, channelManager.channels.keySet().size());

    for (int i = 0; i < 10; i++) {
      String databaseName = "my_db_" + i;
      String schemaName = "my_schema_" + i;
      String tableName = "my_schema_" + i;
      TestChannel channel =
          (TestChannel)
              channelManager.channels.get(new TableKey(databaseName, schemaName, tableName));
      assertNotNull(channel);
      assertEquals(2000, channel.insertedRows.size());
      verifyRowsForChannel(channel, 2000);
    }
  }
}
