package com.example.SnowpipeRest.utils;

import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

public class TableNotFoundResponse extends ResponseStatusException {
  public TableNotFoundResponse(String message) {
    super(HttpStatus.NOT_FOUND, message);
  }
}
