# Partial Insert for Log Tables

| RFC ID | 01 |
|--------|-----|
| Title | Partial Insert for Log Tables |
| Status | Draft |
| Author | Fluss Community |
| Created | 2026-03-16 |
| Target Modules | fluss-client, fluss-common |

## 1. Summary

This RFC proposes a "partial insert" feature for Fluss log tables, enabling clients to send rows containing only a subset of columns. This reduces network bandwidth and storage costs when data naturally has sparse columns or when only specific columns are available during ingestion.

## 2. Motivation

### 2.1 Problem Statement

In real-time data ingestion scenarios, data often arrives with varying column completeness:

1. **Full row data**: All columns are present (e.g., 100 columns)
2. **Partial row data**: Only a subset of columns have values (e.g., 10 columns), remaining columns would be null

Currently, Fluss's `AppendWriter` requires all columns to be sent, forcing clients to:
- Send null values for all missing columns
- Waste network bandwidth transmitting null values
- Increase storage footprint with unnecessary null data

### 2.2 Use Cases

1. **IoT Sensor Data**: Sensors report only a subset of metrics at different times
2. **Event Logging**: Events from different sources have varying attribute sets
3. **Progressive Data Enrichment**: Initial data ingestion with core fields, enrichment happens downstream
4. **Schema Evolution**: New columns added to tables where historical data doesn't have values

### 2.3 Goals

- Client specifies target columns at writer creation time (per-writer configuration)
- Non-target columns are treated as null when reading
- Non-target columns must be nullable (validation at writer creation)
- Bucket key columns must be included in target columns
- Target columns transmitted via batch header metadata (no protocol change)
- Target column indices resolved via schema ID for schema evolution safety
- Backward compatible with existing clients and servers

### 2.4 Non-Goals

- Per-call target column specification
- Partial update of existing rows (use primary key tables)
- Schema inference (columns must be explicitly specified)
- Column-level compression optimization

## 3. Technical Design

### 3.1 Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Client Side                                     │
├─────────────────────────────────────────────────────────────────────────────┤
│  Table.getAppendWriter(targetColumns)                                       │
│         │                                                                    │
│         ▼                                                                    │
│  AppendWriterImpl.append(row)                                               │
│         │    - row has targetColumns.length fields                          │
│         │    - validation at writer creation                                 │
│         ▼                                                                    │
│  WriteRecord.forXxxAppend(row, targetColumns)                               │
│         │                                                                    │
│         ▼                                                                    │
│  LogWriteBatch.build()                                                      │
│         │    - Encode row with projected schema                             │
│         │    - Set PARTIAL_COLUMNS_FLAG in batch header                     │
│         │    - Store target columns array in header                         │
│         ▼                                                                    │
│  MemoryLogRecords (self-contained)                                          │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼ RPC (no protocol change)
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Server Side                                     │
├─────────────────────────────────────────────────────────────────────────────┤
│  TabletService.produceLog()                                                 │
│         │                                                                    │
│         ▼                                                                    │
│  ReplicaManager.appendRecordsToLog()                                        │
│         │    - No targetColumns parameter needed                            │
│         │    - MemoryLogRecords already contains metadata                   │
│         ▼                                                                    │
│  LogTablet.appendAsLeader()                                                 │
│         │    - Store bytes as-is                                            │
│         ▼                                                                    │
│  Log Segment Files                                                          │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼ Read Path
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Reading                                         │
├─────────────────────────────────────────────────────────────────────────────┤
│  MemoryLogRecords.iterator()                                                │
│         │                                                                    │
│         ▼                                                                    │
│  Parse batch header                                                         │
│         │                                                                    │
│         ▼                                                                    │
│  PARTIAL_COLUMNS_FLAG set?                                                  │
│         │                                                                    │
│    ┌────┴────┐                                                               │
│    │         │                                                               │
│   Yes       No                                                               │
│    │         │                                                               │
│    ▼         ▼                                                               │
│  Extract    Return                                                          │
│  target     normal row                                                      │
│  columns                                                                       │
│    │                                                                          │
│    ▼                                                                          │
│  Get write-time schema via schemaId                                          │
│    │                                                                          │
│    ▼                                                                          │
│  Create PartialRow wrapper                                                   │
│    │    - Map column indices via write-time schema                           │
│    │    - Return null for non-target columns                                 │
│    ▼                                                                          │
│  Full schema row                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 3.2 Data Format

The partial row is encoded using existing formats (INDEXED, COMPACTED, or ARROW) but with a **projected schema** containing only the target columns. The batch header stores the `targetColumns` array.

**Example:**

Table schema: `(id INT, name STRING, age INT, email STRING)`

```
Full row insert:
  targetColumns = null (not set)
  row data: [1, "Alice", 30, "alice@example.com"]

Partial insert (columns 0, 1):
  targetColumns = [0, 1]  // id, name
  row data: [2, "Bob"]    // Only 2 fields

When reading the partial row:
  Column 0 (id):    Returns 2
  Column 1 (name):  Returns "Bob"
  Column 2 (age):   Returns null (not present)
  Column 3 (email): Returns null (not present)
```

### 3.3 Schema Evolution Handling

Target columns are stored as indices but resolved via the write-time schema:

1. Batch contains `schemaId` and `targetColumns = [0, 1]`
2. Reader fetches write-time schema via `schemaId`
3. Resolve indices to names: `["id", "name"]`
4. Map names to current schema positions
5. Create `PartialRow` with correct column mapping

This ensures correctness when columns are added or reordered in future schema versions.

## 4. API Design

### 4.1 Table Interface

```java
public interface Table {
    // Existing
    AppendWriter getAppendWriter();

    // NEW: Create a partial insert writer
    /**
     * Creates an AppendWriter that writes only the specified columns.
     *
     * <p>Non-target columns will be treated as null when reading.
     *
     * @param targetColumns the indices of columns to write; must be valid indices
     * @return an AppendWriter for partial inserts
     * @throws IllegalArgumentException if targetColumns contains invalid indices,
     *         duplicates, misses bucket keys, or non-target columns are NOT NULL
     */
    AppendWriter getAppendWriter(int[] targetColumns);
}
```

### 4.2 AppendWriter Interface

Unchanged - uses same `append(InternalRow)` method:

```java
public interface AppendWriter extends TableWriter {
    CompletableFuture<AppendResult> append(InternalRow record);
}
```

For partial writers, `record.getFieldCount()` must equal `targetColumns.length`.

### 4.3 Usage Example

```java
Table table = connection.getTable(TablePath.of("db", "users"));

// Full row insert (existing)
AppendWriter fullWriter = table.getAppendWriter();
fullWriter.append(GenericRow.of(1, "Alice", 30, "alice@example.com"));

// Partial insert - only id and name columns
AppendWriter partialWriter = table.getAppendWriter(new int[]{0, 1});
partialWriter.append(GenericRow.of(2, "Bob"));
// age and email will be null when reading
```

### 4.4 Validation Rules

At writer creation time:

1. **Valid indices**: All target columns must be valid schema indices
2. **No duplicates**: Each column index can appear only once
3. **Bucket keys required**: If table has bucket keys, they must be in target columns
4. **Nullable non-targets**: All non-target columns must be nullable

```java
// Error examples:

// Invalid index
table.getAppendWriter(new int[]{0, 99});
// → IllegalArgumentException: Invalid column index: 99

// Duplicate
table.getAppendWriter(new int[]{0, 1, 0});
// → IllegalArgumentException: Duplicate column index: 0

// Missing bucket key (if 'user_id' is bucket key)
table.getAppendWriter(new int[]{1, 2});
// → IllegalArgumentException: Target columns must include bucket key column 'user_id'

// Non-nullable column excluded
// Schema: (id INT NOT NULL, name STRING, age INT NOT NULL)
table.getAppendWriter(new int[]{0, 1});
// → IllegalArgumentException: Non-target column 'age' is NOT NULL, but partial insert requires nullable
```

## 5. Record Format

### 5.1 Batch Header Extension

Add attribute flag and target columns to batch header:

```java
public class LogRecordBatchFormat {
    // Existing attributes
    public static final int COMPRESSION_MASK = 0x03;
    public static final int DELETE_RECORD_FLAG = 0x04;

    // NEW: Indicates partial columns present
    public static final int PARTIAL_COLUMNS_FLAG = 0x08;
}
```

### 5.2 Batch Layout

```
Standard Batch Header:
┌─────────────────────────────────────────────────────┐
│ magic (2) | crc (4) | attributes (2) | last_offset  │
│ delta (4) | base_offset (8) | ...                   │
└─────────────────────────────────────────────────────┘

Partial Columns Extension (when PARTIAL_COLUMNS_FLAG set):
┌─────────────────────────────────────────────────────┐
│ [standard header]                                   │
│ target_columns_count (2)                            │
│ target_column_0 (2)                                 │
│ target_column_1 (2)                                 │
│ ...                                                 │
│ [row data - projected schema]                       │
└─────────────────────────────────────────────────────┘
```

### 5.3 PartialRow Wrapper

```java
/**
 * Wraps a partial row to present full schema view.
 * Returns null for non-target columns.
 */
class PartialRow implements InternalRow {
    private final InternalRow delegate;
    private final int[] targetColumns;      // Original target column indices
    private final int[] positionMap;        // Maps schema pos → delegate pos (-1 if absent)
    private final int totalFieldCount;

    PartialRow(InternalRow delegate, int[] targetColumns, int totalFieldCount) {
        this.delegate = delegate;
        this.targetColumns = targetColumns;
        this.totalFieldCount = totalFieldCount;
        this.positionMap = buildPositionMap();
    }

    private int[] buildPositionMap() {
        int[] map = new int[totalFieldCount];
        Arrays.fill(map, -1);
        for (int i = 0; i < targetColumns.length; i++) {
            map[targetColumns[i]] = i;
        }
        return map;
    }

    @Override
    public int getFieldCount() {
        return totalFieldCount;
    }

    @Override
    public boolean isNullAt(int pos) {
        int delegatePos = positionMap[pos];
        if (delegatePos < 0) return true;  // Column not present
        return delegate.isNullAt(delegatePos);
    }

    @Override
    public int getInt(int pos) {
        int delegatePos = positionMap[pos];
        checkArgument(delegatePos >= 0, "Column %d not present in partial row", pos);
        return delegate.getInt(delegatePos);
    }

    // ... other getters follow same pattern
}
```

## 6. Implementation

### 6.1 Client-Side Files

| File | Change |
|------|--------|
| `Table.java` | Add `getAppendWriter(int[] targetColumns)` |
| `AppendWriterImpl.java` | Accept targetColumns, validate, encode projected rows |
| `WriteRecord.java` | Add targetColumns to append factory methods |
| `AbstractRowLogWriteBatch.java` | Remove assertion, handle partial rows |
| `ArrowLogWriteBatch.java` | Remove assertion, handle partial rows |
| `MemoryLogRecordsBuilder.java` | Support PARTIAL_COLUMNS_FLAG and header extension |

### 6.2 Server-Side Files

**No changes required.** The server stores `MemoryLogRecords` as-is. Target columns metadata is embedded in batch header.

### 6.3 Common Files

| File | Change |
|------|--------|
| `LogRecordBatchFormat.java` | Add PARTIAL_COLUMNS_FLAG constant |
| `DefaultLogRecordBatch.java` | Parse target columns, create PartialRow wrapper |

## 7. Backward Compatibility

### 7.1 Protocol Compatibility

No protocol changes required. `ProduceLogRequest` remains unchanged.

### 7.2 Storage Compatibility

| Reader | Data | Behavior |
|--------|------|----------|
| Old | New (partial) | Old reader ignores PARTIAL_COLUMNS_FLAG, may misinterpret row data |
| New | Old (full) | New reader checks flag, not set → normal full row |

**Note:** Old readers may produce incorrect results when reading partial data. This is acceptable as partial insert is a new feature - users should upgrade readers when using partial insert.

### 7.3 Client API Compatibility

Existing `getAppendWriter()` method unchanged. New overload is additive.

## 8. Testing

### 8.1 Unit Tests

| Test Class | Scenarios |
|------------|-----------|
| `AppendWriterImplTest` | Validation: invalid indices, duplicates, missing bucket keys, non-nullable columns |
| `WriteRecordTest` | TargetColumns in append factory methods |
| `IndexedLogWriteBatchTest` | Partial row batch building, projected encoding |
| `CompactedLogWriteBatchTest` | Partial row batch building, projected encoding |
| `ArrowLogWriteBatchTest` | Partial row batch building |
| `MemoryLogRecordsTest` | Serialize/deserialize with targetColumns |
| `PartialRowTest` | Column mapping, null for non-target columns |
| `DefaultLogRecordBatchTest` | Parsing partial columns from batch header |

### 8.2 Integration Tests

| Scenario | Description |
|----------|-------------|
| Basic partial insert | Insert with subset of columns, read back all columns |
| Multiple column subsets | Different writers with different target columns on same table |
| Null in target column | Target column contains null value |
| Mixed full and partial | Same table, different writers (full and partial) |
| Bucket key validation | Error when bucket key not in target columns |
| Non-nullable validation | Error when non-target column is NOT NULL |
| Schema evolution | Read partial data after schema change (column added) |
| Schema evolution | Read partial data after schema change (column reordered) |

### 8.3 End-to-End Tests

- Flink connector partial insert
- Spark connector partial insert

## 9. Performance

### 9.1 Expected Improvements

| Scenario | Before | After | Improvement |
|----------|--------|-------|-------------|
| 100-col table, 10-col data | Send 100 values | Send 10 values | ~90% network reduction |
| 100-col table, 10-col data | Store 100 null markers | Store 10 values + metadata | ~80% storage reduction |

### 9.2 Overhead

- **Per-batch overhead**: 2 + 2n bytes (count + column indices)
- **Per-record overhead**: 0 bytes (metadata in batch header)
- **Read overhead**: O(1) position map lookup

### 9.3 Benchmarks Required

1. Throughput: full vs partial insert
2. Latency: full vs partial insert
3. Storage size: full vs partial insert
4. Read performance: full row vs partial row

## 10. Open Questions

| Question | Resolution |
|----------|------------|
| Per-call vs per-writer target columns | Per-writer (decided) |
| Non-target column read behavior | Return null (decided) |
| Validation requirements | Non-targets nullable + bucket keys required (decided) |
| Wire format | Embedded in batch header, no protocol change (decided) |
| Schema evolution | Indices + schema ID resolution (decided) |
