package com.example.SnowpipeRest.buffer;

import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.internal.ColumnProperties;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.Pair;
import net.snowflake.ingest.utils.SFException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** Test Channel so we don't directly hit Snowflake in a unit test */
public class TestChannel implements SnowflakeStreamingIngestChannel {

  List<Pair<Map<String, Object>, String>> insertedRows;

  private final boolean throwSFExceptionOnInsert;

  private final boolean returnResponseWithErrors;

  private String latestPersistedOffsetToken;

  // Default constructor
  public TestChannel(boolean throwSFExceptionOnInsert, boolean returnResponseWithErrors) {
    insertedRows = new ArrayList<>();
    this.throwSFExceptionOnInsert = throwSFExceptionOnInsert;
    this.returnResponseWithErrors = returnResponseWithErrors;
  }

  public void setLatestCommittedOffsetToken(String offsetToken) {
    this.latestPersistedOffsetToken = offsetToken;
  }

  @Override
  public String getFullyQualifiedName() {
    return "";
  }

  @Override
  public String getName() {
    return "";
  }

  @Override
  public String getDBName() {
    return "";
  }

  @Override
  public String getSchemaName() {
    return "";
  }

  @Override
  public String getTableName() {
    return "";
  }

  @Override
  public String getFullyQualifiedTableName() {
    return "";
  }

  @Override
  public boolean isValid() {
    return false;
  }

  @Override
  public boolean isClosed() {
    return false;
  }

  @Override
  public CompletableFuture<Void> close() {
    return null;
  }

  @Override
  public CompletableFuture<Void> close(boolean drop) {
    return null;
  }

  @Override
  public InsertValidationResponse insertRow(Map<String, Object> row, String offsetToken) {
    if (throwSFExceptionOnInsert) {
      throw new SFException(ErrorCode.INTERNAL_ERROR);
    }
    this.insertedRows.add(new Pair<>(row, offsetToken));
    InsertValidationResponse insertResponse = new InsertValidationResponse();
    if (returnResponseWithErrors) {
      InsertValidationResponse.InsertError insertError =
          new InsertValidationResponse.InsertError(null, 1);
      insertResponse.addError(insertError);
    }
    return insertResponse;
  }

  @Override
  public InsertValidationResponse insertRows(
      Iterable<Map<String, Object>> rows, String startOffsetToken, String endOffsetToken) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public InsertValidationResponse insertRows(
      Iterable<Map<String, Object>> rows, String offsetToken) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public String getLatestCommittedOffsetToken() {
    if (latestPersistedOffsetToken != null) {
      return latestPersistedOffsetToken;
    }
    return insertedRows.isEmpty() ? null : insertedRows.getLast().getSecond();
  }

  @Override
  public Map<String, ColumnProperties> getTableSchema() {
    return Map.of();
  }
}
