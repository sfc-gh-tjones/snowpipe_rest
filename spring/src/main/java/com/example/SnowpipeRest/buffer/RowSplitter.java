package com.example.SnowpipeRest.buffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.*;
import java.time.format.DateTimeParseException;
import java.util.*;

/**
 * Utility class to split rows in a buffer into two lists:
 * - one for rows older than LATE_THRESHOLD_DURATION and one for regular rows.
 * - The timestamp column config is defined in the lateArrivingTableColumns map.
 * - Key is the table name and value is the timestamp column name.
 * - If there are parsing errors or the timestamp value is null, the row is treated as regular.
 */
public class RowSplitter {

  private static final Logger LOGGER = LoggerFactory.getLogger(RowSplitter.class);


  static Map<String, String> lateArrivingTableColumns = new HashMap<>();

  static {
    lateArrivingTableColumns.put("EDR_DATA", "generatedTime");
    lateArrivingTableColumns.put("AUTH_LOGS", "generatedTime");
    // Add other table/column mappings here
  }

  // Define the threshold (e.g., 24 hours)
  private static final Duration LATE_THRESHOLD_DURATION = Duration.ofHours(12);

  // Result holder class
  public record SplitResult(List<Map<String, Object>> lateRows, List<Map<String, Object>> regularRows) {
    public SplitResult(List<Map<String, Object>> lateRows, List<Map<String, Object>> regularRows) {
      this.lateRows = Objects.requireNonNull(lateRows);
      this.regularRows = Objects.requireNonNull(regularRows);
    }

    @Override
    public List<Map<String, Object>> lateRows() {
      return lateRows;
    }

    @Override
    public List<Map<String, Object>> regularRows() {
      return regularRows;
    }
  }

  public static SplitResult splitLateArrivingRows(Optional<List<Map<String, Object>>> rowsOptional, String tableName) {
    List<Map<String, Object>> lateRows = new ArrayList<>();
    List<Map<String, Object>> regularRows = new ArrayList<>();

    // Handle empty Optional or empty list
    if (!rowsOptional.isPresent() || rowsOptional.get().isEmpty()) {
      return new SplitResult(lateRows, regularRows);
    }

    List<Map<String, Object>> allRows = rowsOptional.get();
    String timestampColumnName = lateArrivingTableColumns.get(tableName);

    // If table name not configured for late check, all rows are regular
    if (timestampColumnName == null) {
      regularRows.addAll(allRows);
      return new SplitResult(lateRows, regularRows);
    }

    // Calculate the time threshold for lateness
    Instant lateThreshold = Instant.now().minus(LATE_THRESHOLD_DURATION);

    for (Map<String, Object> row : allRows) {
      Object timestampObj = row.get(timestampColumnName);
      Instant eventTime = null;

      // Try to convert the timestamp object to Instant
      try {
        if (timestampObj == null) {
          // Treat rows with null timestamp as regular
          LOGGER.info("Timestamp column is null for table {} timestamp column {}.", tableName, timestampColumnName);
          regularRows.add(row);
          continue; // Move to next row
        } else if (timestampObj instanceof Long) {
          eventTime = Instant.ofEpochMilli((Long) timestampObj);
        } else if (timestampObj instanceof java.sql.Timestamp) {
          eventTime = ((java.sql.Timestamp) timestampObj).toInstant();
        } else if (timestampObj instanceof java.util.Date) { // Handles java.util.Date
          eventTime = ((java.util.Date) timestampObj).toInstant();
        } else if (timestampObj instanceof Instant) {
          eventTime = (Instant) timestampObj;
        } else if (timestampObj instanceof String) {
          String timestampStr = (String) timestampObj;
          try {
            // Attempt 1: Parse using Instant.parse (handles ISO with offset/zone like 'Z')
            eventTime = Instant.parse(timestampStr);
          } catch (DateTimeParseException exInstant) {
            // Instant.parse failed, try parsing as LocalDateTime (no offset) and assume UTC
            try {
              // Attempt 2: Parse as LocalDateTime (e.g., "2025-03-21T16:59:55")
              LocalDateTime localDateTime = LocalDateTime.parse(timestampStr);
              // Assume UTC if no offset was provided
              eventTime = localDateTime.toInstant(ZoneOffset.UTC);
            } catch (DateTimeParseException exLocal) {
              // All parsing attempts failed
              LOGGER.info("Could not parse timestamp string: {}. Neither ISO with offset, nor LocalDateTime. Treating as regular row.", timestampStr, exLocal);
              regularRows.add(row);
              continue; // Move to the next row
            }
          }
        } else {
          // Log or handle unsupported type - treat as regular for now
          LOGGER.info("Unsupported timestamp type: {} for row: {}. Treating as regular.", timestampObj.getClass().getName(), row);
          regularRows.add(row);
          continue;
        }

        // Perform the lateness check
        if (eventTime != null && eventTime.isBefore(lateThreshold)) {
          lateRows.add(row);
        } else {
          regularRows.add(row);
        }

      } catch (Exception e) { // Catch broader exceptions during conversion/casting
        LOGGER.error("Unexpected error processing timestamp for row: {}. Error: {}. Treating as regular.", row, e.getMessage());
        // Treat rows with conversion errors as regular
        regularRows.add(row);
      }
    }

    return new SplitResult(lateRows, regularRows);
  }
}