package com.example.SnowpipeRest.buffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.*;
import java.time.format.DateTimeParseException;
import java.util.*;
// ... other imports

public class RowSplitter {

    private static final Logger LOGGER = LoggerFactory.getLogger(RowSplitter.class);


    static Map<String, String> lateArrivingTableColumns = new HashMap<>();
    static {
        lateArrivingTableColumns.put("EDR_DATA", "GENERATEDTIME");
        // Add other table/column mappings here
    }

    // Define the threshold (e.g., 24 hours)
    private static final Duration LATE_THRESHOLD_DURATION = Duration.ofHours(24);

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
                    // Attempt to parse common ISO format
                    try {
                        eventTime = Instant.parse((String) timestampObj);
                    } catch (DateTimeParseException e) {
                        LOGGER.error("Could not parse timestamp string: {} for row: {}. Treating as regular.", timestampObj, row);
                        regularRows.add(row);
                        continue;
                    }
                }
                else {
                    // Log or handle unsupported type - treat as regular for now
                    LOGGER.error("Unsupported timestamp type: {} for row: {}. Treating as regular.", timestampObj.getClass().getName(), row);
                    regularRows.add(row);
                    continue;
                }

                // Perform the lateness check
                if (eventTime != null && eventTime.isBefore(lateThreshold)) {
                    // System.out.println("LATE: " + eventTime + " // Row: " + row); // Debug
                    lateRows.add(row);
                } else {
                    // System.out.println("REGULAR: " + eventTime + " // Row: " + row); // Debug
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