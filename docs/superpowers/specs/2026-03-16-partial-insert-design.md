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
│  Table.newAppend()                                                          │
│         │                                                                    │
│         ▼                                                                    │
│  Append.partialInsert(targetColumns)                                        │
│         │    - validation at this point                                     │
│         │    - returns new Append instance                                  │
│         ▼                                                                    │
│  Append.createWriter()                                                      │
│         │                                                                    │
│         ▼                                                                    │
│  AppendWriterImpl.append(row)                                               │
│         │    - row has targetColumns.length fields                          │
│         ▼                                                                    │
│  WriteRecord.forXxxAppend(row, targetColumns)                               │
│         │                                                                    │
│         ▼                                                                    │
│  LogWriteBatch.build()                                                      │
│         │    - Encode row with projected schema                             │
│         │    - Use LOG_MAGIC_VALUE_V1                                       │
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

### 3.4 Schema Evolution Edge Cases

**Column Dropped:**

If a column in `targetColumns` was dropped in the current schema:
```java
// Write-time schema: (id INT, name STRING, age INT)
// targetColumns = [0, 2] → ["id", "age"]

// Current schema: (id INT, name STRING)  // age dropped
// Resolution: "age" not found → skip, only return "id"
```
The reader logs a warning and skips the dropped column. The `PartialRow` returns null for the dropped column position.

**Column Renamed:**

If a column was renamed, the reader attempts name-based resolution:
```java
// Write-time schema: (id INT, name STRING)
// targetColumns = [1] → ["name"]

// Current schema: (id INT, full_name STRING)  // name → full_name
// Resolution: "name" not found in current schema
//             Return null for this column (data cannot be mapped)
```
If a column name from the write-time schema is not found in the current schema, the column data cannot be mapped and returns null. This is safer than position-based fallback which could produce incorrect results if a different column was added at the same position.

**Column Added:**

New columns added after write time:
```java
// Write-time schema: (id INT, name STRING)
// targetColumns = [0, 1]

// Current schema: (id INT, name STRING, email STRING)
// Resolution: indices 0, 1 map correctly
//             email (new column) → null in PartialRow
```
This is the common case and works correctly.

## 4. API Design

### 4.1 Append Interface

Following the existing `Upsert.partialUpdate()` pattern, add `partialInsert()` to the `Append` interface:

```java
public interface Append {
    // Existing methods
    AppendWriter createWriter();
    <T> TypedAppendWriter<T> createTypedWriter(Class<T> pojoClass);

    // NEW: Apply partial insert columns
    /**
     * Apply partial insert columns and returns a new Append instance.
     *
     * <p>For append operations, only the specified columns will be written.
     * Non-target columns will be treated as null when reading.
     *
     * <p>Note: If the table has bucket keys, the specified columns must contain
     * all bucket key columns. All non-target columns must be nullable.
     *
     * @param targetColumns the column indexes to partial insert; must not be null or empty
     * @return a new Append instance with partial insert configured
     */
    Append partialInsert(int[] targetColumns);

    /**
     * @see #partialInsert(int[]) for more details.
     * @param targetColumnNames the column names to partial insert
     */
    Append partialInsert(String... targetColumnNames);
}
```

### 4.2 Table Interface

Unchanged - use `newAppend()` to get an `Append` builder:

```java
public interface Table {
    // Existing
    Append newAppend();
    // ... other methods
}
```

### 4.3 Usage Example

```java
Table table = connection.getTable(TablePath.of("db", "users"));

// Full row insert (existing)
AppendWriter fullWriter = table.newAppend().createWriter();
fullWriter.append(GenericRow.of(1, "Alice", 30, "alice@example.com"));

// Partial insert - only id and name columns (by index)
AppendWriter partialWriter = table.newAppend()
    .partialInsert(new int[]{0, 1})
    .createWriter();
partialWriter.append(GenericRow.of(2, "Bob"));
// age and email will be null when reading

// Partial insert - by column name
AppendWriter partialWriter2 = table.newAppend()
    .partialInsert("id", "name")
    .createWriter();
partialWriter2.append(GenericRow.of(3, "Charlie"));
```

### 4.4 Validation Rules

At writer creation time:

1. **Non-empty**: targetColumns must not be null or empty
2. **Valid indices**: All target columns must be valid schema indices
3. **No duplicates**: Each column index can appear only once
4. **Bucket keys required**: If table has bucket keys, they must be in target columns
5. **Nullable non-targets**: All non-target columns must be nullable

```java
// Error examples:

// Empty array
table.newAppend().partialInsert(new int[]{});
// → IllegalArgumentException: targetColumns must not be empty

// Invalid index
table.newAppend().partialInsert(new int[]{0, 99});
// → IllegalArgumentException: Invalid column index: 99

// Duplicate
table.newAppend().partialInsert(new int[]{0, 1, 0});
// → IllegalArgumentException: Duplicate column index: 0

// Missing bucket key (if 'user_id' is bucket key)
table.newAppend().partialInsert(new int[]{1, 2});
// → IllegalArgumentException: Target columns must include bucket key column 'user_id'

// Non-nullable column excluded
// Schema: (id INT NOT NULL, name STRING, age INT NOT NULL)
table.newAppend().partialInsert(new int[]{0, 1});
// → IllegalArgumentException: Non-target column 'age' is NOT NULL, but partial insert requires nullable
```

### 4.5 AppendWriter Interface

Unchanged - uses same `append(InternalRow)` method:

```java
public interface AppendWriter extends TableWriter {
    CompletableFuture<AppendResult> append(InternalRow record);
}
```

For partial writers, `record.getFieldCount()` must equal `targetColumns.length`. This validation happens at `append()` call time:

```java
// In AppendWriterImpl.append()
public CompletableFuture<AppendResult> append(InternalRow row) {
    if (targetColumns != null) {
        checkArgument(row.getFieldCount() == targetColumns.length,
            "Row field count (%d) must match target columns count (%d)",
            row.getFieldCount(), targetColumns.length);
    } else {
        checkFieldCount(row);  // Existing full row check
    }
    // ...
}

## 5. Record Format

### 5.1 Batch Header Extension

Add attribute flag and target columns to batch header:

```java
public class LogRecordBatchFormat {
    // Existing attributes (in attributes byte)
    // Bit 0: APPEND_ONLY_FLAG_MASK = 0x01
    // Bits 1-2: COMPRESSION_MASK (unused for log records)

    // NEW: Bit 3 indicates partial columns present
    public static final byte PARTIAL_COLUMNS_FLAG = 0x08;
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
| `Append.java` | Add `partialInsert(int[])` and `partialInsert(String...)` methods |
| `TableAppend.java` | Implement `partialInsert()` methods, store targetColumns |
| `AppendWriterImpl.java` | Accept targetColumns, validate, encode projected rows |
| `WriteRecord.java` | Add targetColumns to append factory methods |
| `AbstractRowLogWriteBatch.java` | Remove assertion, handle partial rows |
| `ArrowLogWriteBatch.java` | Remove assertion, handle partial rows |
| `MemoryLogRecordsBuilder.java` | Support V1 magic, PARTIAL_COLUMNS_FLAG and header extension |

### 6.2 Server-Side Files

**No changes required.** The server stores `MemoryLogRecords` as-is. Target columns metadata is embedded in batch header.

### 6.3 Common Files

| File | Change |
|------|--------|
| `LogRecordBatchFormat.java` | Add `LOG_MAGIC_VALUE_V1` constant, PARTIAL_COLUMNS_FLAG |
| `DefaultLogRecordBatch.java` | Support V1 magic, parse target columns, create PartialRow wrapper |
| `PartialRow.java` | New class in `fluss-common/src/main/java/org/apache/fluss/row/` |

## 7. Backward Compatibility

### 7.1 Protocol Compatibility

No protocol changes required. `ProduceLogRequest` remains unchanged.

### 7.2 Storage Compatibility

**New Magic Value Required:**

To prevent old readers from silently misinterpreting partial data, partial insert batches will use `LOG_MAGIC_VALUE_V1`:

```java
// In LogRecordBatchFormat
public static final byte LOG_MAGIC_VALUE_V0 = 0;
public static final byte LOG_MAGIC_VALUE_V1 = 1;  // Used for LeaderEpoch support
```

**Note:** `LOG_MAGIC_VALUE_V1` was already introduced for LeaderEpoch support. `PARTIAL_COLUMNS_FLAG` is an additional attribute that works with V1 batches. Both features require V1, and old V0-only readers will fail on any V1 batch (not just partial inserts) with "unsupported magic version" error.

The current `CURRENT_LOG_MAGIC_VALUE` is still `LOG_MAGIC_VALUE_V0` for backward compatibility. V1 batches are only used for:
- Batches with LeaderEpoch (existing feature)
- Partial insert batches (this feature)

**Compatibility Matrix:**

| Reader | Data | Behavior |
|--------|------|----------|
| Old (V0 only) | New (V1 partial) | **Error**: "unsupported magic version" - fails fast, no silent corruption |
| New (V0 + V1) | Old (V0 full) | Works - V0 batch has no PARTIAL_COLUMNS_FLAG, normal full row |
| New (V0 + V1) | New (V1 partial) | Works - parses partial columns, creates PartialRow wrapper |

**Migration Path:**

1. **Phase 1**: New clients write V0 batches by default, V1 batches only for partial inserts
2. **Phase 2**: After all readers are upgraded, can optionally switch default to V1
3. **Rollback**: If needed, disable partial insert feature; new readers still read both V0 and V1

This approach ensures:
- Old readers fail explicitly rather than silently corrupting data
- Rolling upgrades work correctly (upgrade readers first, then enable partial insert)
- No data corruption risk during migration

### 7.3 Client API Compatibility

Existing `newAppend().createWriter()` pattern unchanged. New `partialInsert()` method is additive on `Append` interface.

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
| Per-call vs per-writer target columns | Per-writer via `Append.partialInsert()` (decided) |
| Non-target column read behavior | Return null (decided) |
| Validation requirements | Non-targets nullable + bucket keys required (decided) |
| Wire format | Embedded in batch header, no protocol change (decided) |
| Schema evolution | Indices + schema ID resolution (decided) |
| Backward compatibility | Use LOG_MAGIC_VALUE_V1 for partial batches (decided) |
| API pattern | Follow `Upsert.partialUpdate()` builder pattern (decided) |
| Empty target columns validation | Throw IllegalArgumentException (decided) |

## 11. Appendix: Arrow Format Handling

For Arrow format, partial inserts work naturally:

1. Arrow schema in the batch is the projected schema (only target columns)
2. Column indices in `targetColumns` array refer to the original table schema
3. During read, the Arrow batch is read with projected schema
4. `PartialRow` wrapper maps projected positions to full schema positions

No special handling needed beyond what's described for other formats.
