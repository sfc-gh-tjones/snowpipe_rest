package com.example.SnowpipeRest.buffer;

import com.example.SnowpipeRest.utils.EnqueueResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.snowflake.ingest.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

/** A buffer implementation. In-memory for now but may be backed by persistent, local storage. */
public class Buffer {

  private static final Logger LOGGER = LoggerFactory.getLogger(Buffer.class);

  private static final ObjectMapper mapper = new ObjectMapper();

  // Destination table attributes
  private final String database;
  private final String schema;
  private final String table;

  // maximum number of rows allowed in the buffer
  // TODO: measure bytes, but since we have an approximation of this use case we can sort of cheat
  private final long maxRowCount;

  // Our offset into the buffer if we need to replay events
  private long offsetCounter;

  // Our actual row buffer. Map of offset to data
  private final Queue<Pair<Long, Map<String, Object>>> rowBuffer;

  /**
   * Default constructor
   *
   * @param maxRowCount the max number of rows that we will accept in this buffer
   */
  Buffer(String database, String schema, String table, long maxRowCount) {
    this.database = database;
    this.schema = schema;
    this.table = table;

    this.maxRowCount = maxRowCount;
    this.offsetCounter = 0;

    this.rowBuffer = new ConcurrentLinkedQueue<>();
  }

  /**
   * @return whether there are rows to be processed in this buffer
   */
  public boolean hasOutstandingRows() {
    return !this.rowBuffer.isEmpty();
  }

  /**
   * Returns rows if valid input, empty if otherwise
   *
   * @param requestBody the application provided row in a serialized format
   * @return rows if we can parse them, empty otherwise
   */
  Optional<List<Map<String, Object>>> getRowsFromRequestBody(String requestBody) {
    List<Map<String, Object>> rows;
    try {
      JsonNode jsonNode = mapper.readTree(requestBody);
      rows = mapper.convertValue(jsonNode, new TypeReference<>() {});
    } catch (JsonProcessingException je) {
      return Optional.empty();
    }
    return Optional.of(rows);
  }

  /**
   * This is terribly (and embarrassingly) inefficient, but it'll work for now. Basically increment
   * the latest uncommitted row and advance the pointer. For our V0 dequeue as well but for V1
   * actually just advance a pointer
   */
  public Optional<Pair<Long, Map<String, Object>>> getAndAdvanceLatestUncommittedRow() {
    Pair<Long, Map<String, Object>> item = rowBuffer.poll();
    return item == null ? Optional.empty() : Optional.of(item);
  }

  /** Adds a row to a buffer, checking size to ensure that we can accept it */
  private synchronized boolean addRow(Map<String, Object> row) {
    if (rowBuffer.size() >= maxRowCount) {
      LOGGER.trace("Rejecting row due to maximum size reached");
      return false;
    }
    rowBuffer.add(new Pair<>(offsetCounter, row));
    offsetCounter += 1;
    return true;
  }

  /**
   * Given a request body expand to rows and append to a queue
   *
   * @param requestBody user supplied string that represents one or more rows
   */
  public EnqueueResponse expandRowsEnqueueData(String requestBody) {
    Optional<List<Map<String, Object>>> rows = getRowsFromRequestBody(requestBody);
    if (rows.isEmpty()) {
      return new EnqueueResponse.EnqueueResponseBuilder()
          .setMessage("Unable to parse request body")
          .build();
    }
    int rowsEnqueued = 0;
    int rowsRejected = 0;
    int rowsToInsert = rows.get().size();
    for (int i = 0; i < rowsToInsert; i++) {
      Map<String, Object> row = rows.get().get(i);
      if (!addRow(row)) {
        // Reject the batch outright as subsequent adds likely won't succeed
        rowsRejected = rowsToInsert - i;
        break;
      }
      rowsEnqueued++;
    }
    return new EnqueueResponse.EnqueueResponseBuilder()
        .setRowsEnqueued(rowsEnqueued)
        .setRowsRejected(rowsRejected)
        .build();
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
    return Objects.equals(database, ((Buffer) obj).database)
        && Objects.equals(schema, ((Buffer) obj).schema)
        && Objects.equals(table, ((Buffer) obj).table);
  }
}
