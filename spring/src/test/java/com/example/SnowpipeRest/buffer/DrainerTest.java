package com.example.SnowpipeRest.buffer;

import com.example.SnowpipeRest.snowflake.ChannelManager;
import com.example.SnowpipeRest.utils.TablePartitionKey;
import com.example.SnowpipeRest.utils.Utils;
import net.snowflake.ingest.utils.Pair;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class DrainerTest {

  @Test
  public void testDrainAllRows() {
    TestChannelManager channelManager = new TestChannelManager(null, false, false);
    ChannelManager.setInstance(channelManager);

    Buffer buffer = new Buffer("my_db", "my_sch", "my_table", 2, 1);
    String requestBody =
        "[{\"some_int\": 1, \"some_string\": \"one\"}, {\"some_int\": 2, \"some_string\": \"two\"}]";
    buffer.expandRowsEnqueueData(requestBody);

    Drainer drainer = new Drainer(1234, buffer, 1000, 10, 120);
    Drainer.TerminationReason reason = drainer.drain();
    assertEquals(Drainer.TerminationReason.SUCCESS, reason);

    assertEquals(1, channelManager.channels.keySet().size());
    TestChannel channel =
        (TestChannel)
            channelManager.channels.get(new TablePartitionKey("my_db", "my_sch", "my_table", 1));
    assertNotNull(channel);
    assertEquals(2, channel.insertedRows.size());

    Pair<Map<String, Object>, String> row = channel.insertedRows.get(0);
    assertEquals(0, Utils.getBufferIndexFromOffsetToken(row.getSecond()));
    row = channel.insertedRows.get(1);
    assertEquals(1, Utils.getBufferIndexFromOffsetToken(row.getSecond()));

    assertFalse(buffer.hasOutstandingRows());
  }

  @Test
  public void testDrainHitMaxRowCount() {
    TestChannelManager channelManager = new TestChannelManager(null, false, false);
    ChannelManager.setInstance(channelManager);

    Buffer buffer = new Buffer("my_db", "my_sch", "my_table", 2, 1);
    String requestBody =
        "[{\"some_int\": 1, \"some_string\": \"one\"}, {\"some_int\": 2, \"some_string\": \"two\"}]";
    buffer.expandRowsEnqueueData(requestBody);

    Drainer drainer = new Drainer(1234, buffer, 1000, 1, 120);
    Drainer.TerminationReason reason = drainer.drain();
    assertEquals(Drainer.TerminationReason.SUCCESS, reason);

    assertEquals(1, channelManager.channels.keySet().size());
    TestChannel channel =
        (TestChannel)
            channelManager.channels.get(new TablePartitionKey("my_db", "my_sch", "my_table", 1));
    assertNotNull(channel);
    assertEquals(1, channel.insertedRows.size());

    assertTrue(buffer.hasOutstandingRows());
  }

  @Test
  public void testDrainSFExceptionOnInsert() {
    TestChannelManager channelManager = new TestChannelManager(null, true, false);
    ChannelManager.setInstance(channelManager);

    Buffer buffer = new Buffer("my_db", "my_sch", "my_table", 2, 1);
    String requestBody =
        "[{\"some_int\": 1, \"some_string\": \"one\"}, {\"some_int\": 2, \"some_string\": \"two\"}]";
    buffer.expandRowsEnqueueData(requestBody);

    Drainer drainer = new Drainer(1234, buffer, 1000, 1, 120);
    Drainer.TerminationReason reason = drainer.drain();
    assertEquals(Drainer.TerminationReason.CHANNEL_ERROR, reason);

    assertEquals(1, channelManager.channels.keySet().size());
    TestChannel channel =
        (TestChannel)
            channelManager.channels.get(new TablePartitionKey("my_db", "my_sch", "my_table", 1));
    assertNotNull(channel);
    assertEquals(0, channel.insertedRows.size());

    assertTrue(buffer.hasOutstandingRows());
  }

  @Test
  public void testDrainSFExceptionOnInsertSendMoreRows() {
    TestChannelManager channelManager = new TestChannelManager(null, true, false);
    ChannelManager.setInstance(channelManager);

    Buffer buffer = new Buffer("my_db", "my_sch", "my_table", 2, 1);
    String requestBody =
        "[{\"some_int\": 1, \"some_string\": \"one\"}, {\"some_int\": 2, \"some_string\": \"two\"}]";
    buffer.expandRowsEnqueueData(requestBody);

    Drainer drainer = new Drainer(1234, buffer, 1000, 1, 120);
    Drainer.TerminationReason reason = drainer.drain();
    assertEquals(Drainer.TerminationReason.CHANNEL_ERROR, reason);

    assertEquals(1, channelManager.channels.keySet().size());
    TestChannel channel =
        (TestChannel)
            channelManager.channels.get(new TablePartitionKey("my_db", "my_sch", "my_table", 1));
    assertNotNull(channel);
    assertEquals(0, channel.insertedRows.size());
    assertTrue(buffer.hasOutstandingRows());

    channelManager = new TestChannelManager(null, false, false);
    ChannelManager.setInstance(channelManager);
    reason = drainer.drain();
    assertEquals(Drainer.TerminationReason.SUCCESS, reason);
    assertFalse(buffer.hasOutstandingRows());
    // Will be 0 due to enqueueing
    assertEquals(0, channel.insertedRows.size());
  }

  @Test
  public void testDrainErrorsEncounteredDuringInsert() {
    TestChannelManager channelManager = new TestChannelManager(null, false, true);
    ChannelManager.setInstance(channelManager);

    Buffer buffer = new Buffer("my_db", "my_sch", "my_table", 2, 1);
    String requestBody =
        "[{\"some_int\": 1, \"some_string\": \"one\"}, {\"some_int\": 2, \"some_string\": \"two\"}]";
    buffer.expandRowsEnqueueData(requestBody);

    Drainer drainer = new Drainer(1234, buffer, 1000, 10, 120);
    Drainer.TerminationReason reason = drainer.drain();
    assertEquals(Drainer.TerminationReason.SUCCESS, reason);

    assertEquals(1, channelManager.channels.keySet().size());
    TestChannel channel =
        (TestChannel)
            channelManager.channels.get(new TablePartitionKey("my_db", "my_sch", "my_table", 1));
    assertNotNull(channel);
    assertEquals(2, channel.insertedRows.size());
    assertFalse(buffer.hasOutstandingRows());
  }

  @Test
  public void testWaitForChannelToDrainInvalidEpoch() {
    TestChannel channel = new TestChannel(false, false);
    channel.setLatestCommittedOffsetToken("0-4567");
    Utils.DrainReason reason = Utils.waitForChannelToDrain(1, channel, 1234, "0-1234");
    assertEquals(Utils.DrainReason.INVALID_EPOCH, reason);
  }

  @Test
  public void testWaitForChannelToDrainOffsetsMatch() {
    TestChannel channel = new TestChannel(false, false);
    channel.setLatestCommittedOffsetToken("0-1234");
    Utils.DrainReason reason = Utils.waitForChannelToDrain(1, channel, 1234, "0-1234");
    assertEquals(Utils.DrainReason.OFFSET_MATCHED, reason);
  }

  @Test
  public void testWaitForChannelToDrainOffsetsMismatch() {
    TestChannel channel = new TestChannel(false, false);
    channel.setLatestCommittedOffsetToken("1-1234");
    Utils.DrainReason reason = Utils.waitForChannelToDrain(1, channel, 1234, "0-1234");
    assertEquals(Utils.DrainReason.OFFSET_NEVER_MATCHED, reason);
  }
}
