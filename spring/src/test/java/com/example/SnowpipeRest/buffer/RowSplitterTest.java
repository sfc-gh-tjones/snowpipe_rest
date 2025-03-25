package com.example.SnowpipeRest.buffer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.sql.Timestamp; // Needed for explicit java.sql.Timestamp testing

import static org.junit.jupiter.api.Assertions.*;

class RowSplitterTest {

    // Constants matching RowSplitter configuration for clarity
    private static final String CONFIGURED_TABLE = "EDR_DATA";
    private static final String TIMESTAMP_COLUMN = "GENERATEDTIME";
    private static final String UNCONFIGURED_TABLE = "OTHER_TABLE";
    // Use the same threshold as defined in RowSplitter for consistency in tests
    private static final Duration LATE_THRESHOLD_DURATION = Duration.ofHours(24);

    private Instant now;
    private Instant lateTime;      // Clearly older than threshold
    private Instant regularTime;   // Clearly newer than threshold
    private Instant edgeLateTime;  // Just barely older than threshold
    private Instant edgeRegularTime; // Just barely newer than threshold

    @BeforeEach
    void setUp() {
        // Set up consistent time points for each test run
        now = Instant.now();
        Instant thresholdTime = now.minus(LATE_THRESHOLD_DURATION);

        lateTime = thresholdTime.minus(Duration.ofMinutes(10));       // 10 mins before threshold
        regularTime = thresholdTime.plus(Duration.ofMinutes(10));      // 10 mins after threshold
        edgeLateTime = thresholdTime.minus(Duration.ofSeconds(1));     // 1 sec before threshold
        edgeRegularTime = thresholdTime.plus(Duration.ofSeconds(1));   // 1 sec after threshold (or exactly threshold if resolution limited)

        // Note: Due to potential system clock granularity, edge cases might be tricky.
        // Using slightly larger margins (like minutes above) is often safer for tests.
    }

    // Helper to create a row map
    // Use a special value "MISSING" to indicate the timestamp column should not be added
    private Map<String, Object> createRow(int id, Object timestampValue) {
        Map<String, Object> row = new HashMap<>();
        row.put("ID", id);
        row.put("DATA", "Data for " + id);
        if (!"MISSING".equals(timestampValue)) {
            row.put(TIMESTAMP_COLUMN, timestampValue);
        }
        return row;
    }

    // === Input Handling Tests ===

    @Test
    @DisplayName("Should return empty lists when Optional input is empty")
    void testSplitRows_whenOptionalIsEmpty_shouldReturnEmptyLists() {
        Optional<List<Map<String, Object>>> emptyOptional = Optional.empty();
        RowSplitter.SplitResult result = RowSplitter.splitLateArrivingRows(emptyOptional, CONFIGURED_TABLE);

        assertNotNull(result, "Result should not be null");
        assertTrue(result.lateRows().isEmpty(), "Late rows should be empty for empty optional");
        assertTrue(result.regularRows().isEmpty(), "Regular rows should be empty for empty optional");
    }

    @Test
    @DisplayName("Should return empty lists when Optional contains null list")
    void testSplitRows_whenOptionalContainsNullList_shouldReturnEmptyLists() {
        Optional<List<Map<String, Object>>> optionalWithNull = Optional.ofNullable(null);
        RowSplitter.SplitResult result = RowSplitter.splitLateArrivingRows(optionalWithNull, CONFIGURED_TABLE);

        assertNotNull(result, "Result should not be null");
        assertTrue(result.lateRows().isEmpty(), "Late rows should be empty for null list");
        assertTrue(result.regularRows().isEmpty(), "Regular rows should be empty for null list");
    }

    @Test
    @DisplayName("Should return empty lists when input list is empty")
    void testSplitRows_whenListIsEmpty_shouldReturnEmptyLists() {
        Optional<List<Map<String, Object>>> optionalWithEmptyList = Optional.of(Collections.emptyList());
        RowSplitter.SplitResult result = RowSplitter.splitLateArrivingRows(optionalWithEmptyList, CONFIGURED_TABLE);

        assertNotNull(result, "Result should not be null");
        assertTrue(result.lateRows().isEmpty(), "Late rows should be empty for empty list");
        assertTrue(result.regularRows().isEmpty(), "Regular rows should be empty for empty list");
    }

    // === Table Configuration Tests ===

    @Test
    @DisplayName("Should treat all rows as regular when table is not configured")
    void testSplitRows_whenTableNotConfigured_shouldAllBeRegular() {
        List<Map<String, Object>> rows = Arrays.asList(
                createRow(1, lateTime.toEpochMilli()), // Potentially late if table were configured
                createRow(2, regularTime.toEpochMilli())
        );
        Optional<List<Map<String, Object>>> optionalRows = Optional.of(rows);

        RowSplitter.SplitResult result = RowSplitter.splitLateArrivingRows(optionalRows, UNCONFIGURED_TABLE);

        assertNotNull(result, "Result should not be null");
        assertTrue(result.lateRows().isEmpty(), "Late rows should be empty for unconfigured table");
        assertEquals(2, result.regularRows().size(), "Regular rows should contain all input rows");
        // Optional: Check if the specific rows are in the regular list
        assertTrue(result.regularRows().contains(rows.get(0)));
        assertTrue(result.regularRows().contains(rows.get(1)));
    }


    // === Timestamp Value and Type Tests (for Configured Table) ===

    @Test
    @DisplayName("Should classify row as LATE with Long timestamp before threshold")
    void testSplitRows_withLateRow_LongTimestamp() {
        List<Map<String, Object>> rows = Collections.singletonList(createRow(10, lateTime.toEpochMilli()));
        RowSplitter.SplitResult result = RowSplitter.splitLateArrivingRows(Optional.of(rows), CONFIGURED_TABLE);
        assertEquals(1, result.lateRows().size());
        assertEquals(0, result.regularRows().size());
        assertEquals(10, result.lateRows().get(0).get("ID"));
    }

    @Test
    @DisplayName("Should classify row as REGULAR with Long timestamp after threshold")
    void testSplitRows_withRegularRow_LongTimestamp() {
        List<Map<String, Object>> rows = Collections.singletonList(createRow(11, regularTime.toEpochMilli()));
        RowSplitter.SplitResult result = RowSplitter.splitLateArrivingRows(Optional.of(rows), CONFIGURED_TABLE);
        assertEquals(0, result.lateRows().size());
        assertEquals(1, result.regularRows().size());
        assertEquals(11, result.regularRows().get(0).get("ID"));
    }

    @Test
    @DisplayName("Should classify row as LATE with Date timestamp before threshold")
    void testSplitRows_withLateRow_DateTimestamp() {
        List<Map<String, Object>> rows = Collections.singletonList(createRow(20, Date.from(lateTime)));
        RowSplitter.SplitResult result = RowSplitter.splitLateArrivingRows(Optional.of(rows), CONFIGURED_TABLE);
        assertEquals(1, result.lateRows().size());
        assertEquals(0, result.regularRows().size());
        assertEquals(20, result.lateRows().get(0).get("ID"));
    }

    @Test
    @DisplayName("Should classify row as REGULAR with Date timestamp after threshold")
    void testSplitRows_withRegularRow_DateTimestamp() {
        List<Map<String, Object>> rows = Collections.singletonList(createRow(21, Date.from(regularTime)));
        RowSplitter.SplitResult result = RowSplitter.splitLateArrivingRows(Optional.of(rows), CONFIGURED_TABLE);
        assertEquals(0, result.lateRows().size());
        assertEquals(1, result.regularRows().size());
        assertEquals(21, result.regularRows().get(0).get("ID"));
    }

    @Test
    @DisplayName("Should classify row as LATE with SQL Timestamp before threshold")
    void testSplitRows_withLateRow_SqlTimestamp() {
        List<Map<String, Object>> rows = Collections.singletonList(createRow(30, Timestamp.from(lateTime)));
        RowSplitter.SplitResult result = RowSplitter.splitLateArrivingRows(Optional.of(rows), CONFIGURED_TABLE);
        assertEquals(1, result.lateRows().size());
        assertEquals(0, result.regularRows().size());
        assertEquals(30, result.lateRows().get(0).get("ID"));
    }

    @Test
    @DisplayName("Should classify row as REGULAR with SQL Timestamp after threshold")
    void testSplitRows_withRegularRow_SqlTimestamp() {
        List<Map<String, Object>> rows = Collections.singletonList(createRow(31, Timestamp.from(regularTime)));
        RowSplitter.SplitResult result = RowSplitter.splitLateArrivingRows(Optional.of(rows), CONFIGURED_TABLE);
        assertEquals(0, result.lateRows().size());
        assertEquals(1, result.regularRows().size());
        assertEquals(31, result.regularRows().get(0).get("ID"));
    }

    @Test
    @DisplayName("Should classify row as LATE with Instant timestamp before threshold")
    void testSplitRows_withLateRow_InstantTimestamp() {
        List<Map<String, Object>> rows = Collections.singletonList(createRow(40, lateTime));
        RowSplitter.SplitResult result = RowSplitter.splitLateArrivingRows(Optional.of(rows), CONFIGURED_TABLE);
        assertEquals(1, result.lateRows().size());
        assertEquals(0, result.regularRows().size());
        assertEquals(40, result.lateRows().get(0).get("ID"));
    }

    @Test
    @DisplayName("Should classify row as REGULAR with Instant timestamp after threshold")
    void testSplitRows_withRegularRow_InstantTimestamp() {
        List<Map<String, Object>> rows = Collections.singletonList(createRow(41, regularTime));
        RowSplitter.SplitResult result = RowSplitter.splitLateArrivingRows(Optional.of(rows), CONFIGURED_TABLE);
        assertEquals(0, result.lateRows().size());
        assertEquals(1, result.regularRows().size());
        assertEquals(41, result.regularRows().get(0).get("ID"));
    }

    @Test
    @DisplayName("Should classify row as LATE with valid String timestamp before threshold")
    void testSplitRows_withLateRow_StringTimestamp() {
        List<Map<String, Object>> rows = Collections.singletonList(createRow(50, lateTime.toString()));
        RowSplitter.SplitResult result = RowSplitter.splitLateArrivingRows(Optional.of(rows), CONFIGURED_TABLE);
        assertEquals(1, result.lateRows().size());
        assertEquals(0, result.regularRows().size());
        assertEquals(50, result.lateRows().get(0).get("ID"));
    }

    @Test
    @DisplayName("Should classify row as REGULAR with valid String timestamp after threshold")
    void testSplitRows_withRegularRow_StringTimestamp() {
        List<Map<String, Object>> rows = Collections.singletonList(createRow(51, regularTime.toString()));
        RowSplitter.SplitResult result = RowSplitter.splitLateArrivingRows(Optional.of(rows), CONFIGURED_TABLE);
        assertEquals(0, result.lateRows().size());
        assertEquals(1, result.regularRows().size());
        assertEquals(51, result.regularRows().get(0).get("ID"));
    }

    // === Edge Case and Invalid Data Tests ===

    @Test
    @DisplayName("Should classify row as LATE with timestamp just before threshold")
    void testSplitRows_withEdgeLateRow() {
        List<Map<String, Object>> rows = Collections.singletonList(createRow(60, edgeLateTime.toEpochMilli()));
        RowSplitter.SplitResult result = RowSplitter.splitLateArrivingRows(Optional.of(rows), CONFIGURED_TABLE);
        assertEquals(1, result.lateRows().size());
        assertEquals(0, result.regularRows().size());
        assertEquals(60, result.lateRows().get(0).get("ID"));
    }

    @Test
    @DisplayName("Should classify row as REGULAR with timestamp just after threshold")
    void testSplitRows_withEdgeRegularRow() {
        // Add a small buffer to ensure it's definitely after, accounting for potential truncation/granularity
        Instant slightlyAfterThreshold = now.minus(LATE_THRESHOLD_DURATION).plusMillis(10);
        List<Map<String, Object>> rows = Collections.singletonList(createRow(61, slightlyAfterThreshold.toEpochMilli()));
        RowSplitter.SplitResult result = RowSplitter.splitLateArrivingRows(Optional.of(rows), CONFIGURED_TABLE);
        assertEquals(0, result.lateRows().size());
        assertEquals(1, result.regularRows().size());
        assertEquals(61, result.regularRows().get(0).get("ID"));
    }

    @Test
    @DisplayName("Should classify row as REGULAR when timestamp is null")
    void testSplitRows_withNullTimestamp_shouldBeRegular() {
        List<Map<String, Object>> rows = Collections.singletonList(createRow(70, null));
        RowSplitter.SplitResult result = RowSplitter.splitLateArrivingRows(Optional.of(rows), CONFIGURED_TABLE);
        assertEquals(0, result.lateRows().size());
        assertEquals(1, result.regularRows().size());
        assertEquals(70, result.regularRows().get(0).get("ID"));
    }

    @Test
    @DisplayName("Should classify row as REGULAR when timestamp column is missing")
    void testSplitRows_withMissingTimestampColumn_shouldBeRegular() {
        List<Map<String, Object>> rows = Collections.singletonList(createRow(80, "MISSING")); // Helper uses "MISSING" to omit column
        RowSplitter.SplitResult result = RowSplitter.splitLateArrivingRows(Optional.of(rows), CONFIGURED_TABLE);
        assertEquals(0, result.lateRows().size());
        assertEquals(1, result.regularRows().size());
        assertEquals(80, result.regularRows().get(0).get("ID"));
    }

    @Test
    @DisplayName("Should classify row as REGULAR with unparseable String timestamp")
    void testSplitRows_withUnparseableStringTimestamp_shouldBeRegular() {
        List<Map<String, Object>> rows = Collections.singletonList(createRow(90, "not-a-valid-date-string"));
        RowSplitter.SplitResult result = RowSplitter.splitLateArrivingRows(Optional.of(rows), CONFIGURED_TABLE);
        // Consider capturing stderr to verify warning, but main check is classification
        assertEquals(0, result.lateRows().size());
        assertEquals(1, result.regularRows().size());
        assertEquals(90, result.regularRows().get(0).get("ID"));
    }

    @Test
    @DisplayName("Should classify row as REGULAR with unsupported timestamp type")
    void testSplitRows_withUnsupportedTimestampType_shouldBeRegular() {
        List<Map<String, Object>> rows = Collections.singletonList(createRow(100, 12345)); // Integer type
        RowSplitter.SplitResult result = RowSplitter.splitLateArrivingRows(Optional.of(rows), CONFIGURED_TABLE);
        // Consider capturing stderr to verify warning
        assertEquals(0, result.lateRows().size());
        assertEquals(1, result.regularRows().size());
        assertEquals(100, result.regularRows().get(0).get("ID"));
    }

    // === Mixed Scenario Test ===

    @Test
    @DisplayName("Should correctly split a list with mixed row types")
    void testSplitRows_withMixedRows_shouldSplitCorrectly() {
        List<Map<String, Object>> mixedRows = Arrays.asList(
                createRow(201, lateTime.toEpochMilli()),      // Late (Long)
                createRow(202, regularTime.toString()),      // Regular (String)
                createRow(203, null),                         // Regular (Null)
                createRow(204, "MISSING"),                    // Regular (Missing Column)
                createRow(205, Date.from(lateTime.minus(Duration.ofDays(1)))), // Late (Date, extra late)
                createRow(206, "invalid-string"),             // Regular (Invalid String)
                createRow(207, 999),                          // Regular (Unsupported Type Integer)
                createRow(208, Timestamp.from(regularTime.plus(Duration.ofHours(1)))) // Regular (SQL Timestamp, extra regular)
        );
        Optional<List<Map<String, Object>>> optionalRows = Optional.of(mixedRows);

        RowSplitter.SplitResult result = RowSplitter.splitLateArrivingRows(optionalRows, CONFIGURED_TABLE);

        assertNotNull(result);
        assertEquals(2, result.lateRows().size(), "Expected 2 late rows");
        assertEquals(6, result.regularRows().size(), "Expected 6 regular rows");

        // Verify IDs in late list
        Set<Object> lateIds = new HashSet<>();
        result.lateRows().forEach(row -> lateIds.add(row.get("ID")));
        assertTrue(lateIds.contains(201));
        assertTrue(lateIds.contains(205));

        // Verify IDs in regular list
        Set<Object> regularIds = new HashSet<>();
        result.regularRows().forEach(row -> regularIds.add(row.get("ID")));
        assertTrue(regularIds.contains(202));
        assertTrue(regularIds.contains(203));
        assertTrue(regularIds.contains(204));
        assertTrue(regularIds.contains(206));
        assertTrue(regularIds.contains(207));
        assertTrue(regularIds.contains(208));
    }
}