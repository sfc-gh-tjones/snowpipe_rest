package com.example.SnowpipeRest.rest;

import com.example.SnowpipeRest.utils.EnqueueResponse;
import com.example.SnowpipeRest.utils.IngestEngineConfig;
import com.example.SnowpipeRest.utils.InvalidPayloadResponse;
import com.example.SnowpipeRest.utils.TableNotFoundResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.net.http.HttpResponse;

@RestController
@RequestMapping("/snowpipe")
public class Resource {

  static IngestEngine ingestEngine;

  private void lazyLoadIngestEngine() {
    if (ingestEngine == null) {
      synchronized (IngestEngine.class) {
        if (ingestEngine == null) {
          IngestEngineConfig config = new IngestEngineConfig();
          ingestEngine =
              new IngestEngine(
                  config.getMaxBufferRowCount(),
                  config.getNumThreads(),
                  config.getMaxDurationToDrainMs(),
                  config.getMaxRecordsToDrain(),
                  config.getMaxSecondsToWaitToDrain(),
                  config.getMaxShardsPerTable());
        }
      }
    }
  }

  @PutMapping("/insert/{database}/{schema}/{table}")
  @ResponseBody
  public ResponseEntity<EnqueueResponse> insert(
      @PathVariable String database,
      @PathVariable String schema,
      @PathVariable String table,
      @RequestBody String body) {
    lazyLoadIngestEngine();
    EnqueueResponse response = ingestEngine.enqueueData(database, schema, table, body);
    if (response.getRowsRejected() > 0) {
      return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(response);
    }
    return ResponseEntity.status(HttpStatus.OK).body(response);
  }

  @ExceptionHandler(TableNotFoundResponse.class)
  @ResponseStatus(HttpStatus.NOT_FOUND)
  public ResponseEntity<String> handleTableNotFound(TableNotFoundResponse e) {
    return ResponseEntity.status(HttpStatus.NOT_FOUND).body(e.getMessage());
  }

  @ExceptionHandler(InvalidPayloadResponse.class)
  @ResponseStatus(HttpStatus.NOT_FOUND)
  public ResponseEntity<String> handleBadJson(InvalidPayloadResponse e) {
    return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(e.getMessage());
  }
}
