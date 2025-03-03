package com.example.SnowpipeRest.buffer;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Optional;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RocksDBManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(RocksDBManager.class);

  private static final String rocksDbDir = "rocks-db";
  private static final String walPath = "rocks-wal";

  RocksDB db;
  private File dbDir;

  public RocksDBManager() {
    initialize();
  }

  void tearDown() {
    db.close();
  }

  void initialize() {
    RocksDB.loadLibrary();
    long ttlInSeconds = 60 * 5;
    long walSizeMaxMb = 1000 * 20; // 20GB wal
    final Options options =
        new Options()
            .setCreateIfMissing(true)
            .setWalDir(walPath)
            .setUseFsync(true)
            .setWalSizeLimitMB(walSizeMaxMb)
            .setTtl(ttlInSeconds);
    try {
      dbDir = new File("/tmp/snowpiperest/", rocksDbDir);
    } catch (NullPointerException e) {
      LOGGER.error("Unable to create file to RocksDB Dir");
      LOGGER.error(e.getMessage());
      return;
    }
    try {
      Files.createDirectories(dbDir.getParentFile().toPath());
      Files.createDirectories(dbDir.getAbsoluteFile().toPath());
      db = RocksDB.open(options, dbDir.getAbsolutePath());
    } catch (IOException | RocksDBException ex) {
      LOGGER.error(
          "Error initializing RocksDB, check configurations and permissions, exception: {}, message: {}, stackTrace: {}",
          ex.getCause(),
          ex.getMessage(),
          ex.getStackTrace());
    }
    if (db == null) {
      LOGGER.error("Unable to get a handle on RocksDB");
      throw new RuntimeException("Unable to get a handle on RocksDB");
    }
    LOGGER.info("RocksDB initialized and ready to use");
  }

  public void purge(String latestPersistedKey) {
    try {
      db.deleteRange(null, latestPersistedKey.getBytes(StandardCharsets.UTF_8));
    } catch (RocksDBException e) {
      LOGGER.error("Unable to purge RocksDB starting with endKey={}", latestPersistedKey);
    }
  }

  /**
   * Writes the key to the DB
   *
   * @param key the key that we want to write. Takes the form of `table-identifier-partition-offset`
   * @param value the value that we want to write. This is the unserialized rows
   */
  public boolean writeToDB(String key, String value) {
    if (key == null || key.isEmpty()) {
      LOGGER.error("Key is null or empty");
      return false;
    }
    if (value == null || value.isEmpty()) {
      LOGGER.error("Value is null or empty");
      return false;
    }
    try {
      byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
      if (keyBytes.length == 0) {
        LOGGER.error("Empty key byte array ");
        return false;
      }
      byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
      if (bytes.length == 0) {
        LOGGER.error("Empty byte array ");
        return false;
      }
      db.put(keyBytes, bytes);
      return true;
    } catch (RocksDBException e) {
      LOGGER.error("Unable to write to RocksDB", e);
    }
    return false;
  }

  /**
   * Reads from the database.
   *
   * @param key
   * @return
   */
  public Optional<String> readFromDB(String key) {
    if (key == null || key.isEmpty()) {
      LOGGER.error("Key is null or empty");
      return Optional.empty();
    }
    try {
      byte[] persistedVal = db.get(key.getBytes(StandardCharsets.UTF_8));
      if (persistedVal == null || persistedVal.length == 0) {
        return Optional.empty();
      }
      return Optional.of(new String(persistedVal));
    } catch (RocksDBException e) {
      LOGGER.error("Unable to read from RocksDB. key={}", key, e);
      return Optional.empty();
    }
  }
}
