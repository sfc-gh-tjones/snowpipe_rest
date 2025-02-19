package com.example.SnowpipeRest.utils;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class EnqueueResponse {
  public String message;
  public int rowsEnqueued;
  public int rowsRejected;

  public EnqueueResponse() {}

  @JsonProperty("message")
  public String getMessage() {
    return message;
  }

  @JsonProperty("rows_enqueued")
  public int getRowsEnqueued() {
    return rowsEnqueued;
  }

  @JsonProperty("rows_rejected")
  public int getRowsRejected() {
    return rowsRejected;
  }

  public static class EnqueueResponseBuilder {

    private EnqueueResponse enqueueResponse;

    public EnqueueResponseBuilder() {
      enqueueResponse = new EnqueueResponse();
    }

    public EnqueueResponseBuilder setMessage(String message) {
      this.enqueueResponse.message = message;
      return this;
    }

    public EnqueueResponseBuilder setRowsEnqueued(int rowsEnqueued) {
      this.enqueueResponse.rowsEnqueued = rowsEnqueued;
      return this;
    }

    public EnqueueResponseBuilder setRowsRejected(int rowsRejected) {
      this.enqueueResponse.rowsRejected = rowsRejected;
      return this;
    }

    public EnqueueResponse build() {
      return enqueueResponse;
    }
  }
}
