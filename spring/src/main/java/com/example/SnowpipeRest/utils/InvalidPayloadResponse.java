package com.example.SnowpipeRest.utils;

import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

public class InvalidPayloadResponse extends ResponseStatusException {
  public InvalidPayloadResponse(String message) {
    super(HttpStatus.BAD_REQUEST, message);
  }
}
