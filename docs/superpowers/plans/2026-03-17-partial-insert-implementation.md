# Partial Insert Feature - Implementation Plan

## Overview

This plan outlines the step-by-step implementation of the partial insert feature for Fluss log tables based on the approved design spec in `docs/superpowers/specs/2026-03-16-partial-insert-design.md`.

**Key Design Decisions:**
- No protocol changes - target columns embedded in batch header
- Use `Append.partialInsert()` builder pattern matching `Upsert.partialUpdate()`
- Use `LOG_MAGIC_VALUE_V1` (already exists for LeaderEpoch)
- `PartialRow` wrapper class for reading

---

## Phase 1: Common Layer - Record Format

### Step 1.1: Add PARTIAL_COLUMNS_FLAG to LogRecordBatchFormat

**File:** `fluss-common/src/main/java/org/apache/fluss/record/LogRecordBatchFormat.java`

Add the attribute flag constant:
```java
// Bit 3 indicates partial columns present
public static final byte PARTIAL_COLUMNS_FLAG = 0x08;
```

Update `recordBatchHeaderSize()` to account for optional target columns:
- Add method overload that accepts whether partial columns are present
- Partial columns add: 2 bytes (count) + 2 bytes per column index

### Step 1.2: Create PartialRow Class

**File:** `fluss-common/src/main/java/org/apache/fluss/row/PartialRow.java` (new file)

Implement `InternalRow` wrapper that:
- Stores reference to delegate row (partial data)
- Stores `int[] targetColumns` (original column indices)
- Builds `int[] positionMap` for O(1) lookup
- Returns null for non-target columns
- Delegates to underlying row for target columns

Key methods to implement:
- `getFieldCount()` - returns total schema field count
- `isNullAt(int pos)` - returns true if column not present or delegate is null
- All typed getters (`getInt`, `getLong`, `getString`, etc.) - map position, delegate call
- `RowKind` methods - delegate directly

### Step 1.3: Update DefaultLogRecordBatch for Reading

**File:** `fluss-common/src/main/java/org/apache/fluss/record/DefaultLogRecordBatch.java`

Changes:
1. Parse `PARTIAL_COLUMNS_FLAG` from attributes
2. If flag set, read target columns array after standard header
3. Store target columns for use during record iteration
4. In `records(ReadContext context)`, wrap returned rows with `PartialRow` if needed

Add methods:
```java
private int[] targetColumns;  // Parsed from batch header
private boolean hasPartialColumns() {
    return (attributes & PARTIAL_COLUMNS_FLAG) != 0;
}
private int[] readTargetColumns(ByteBuffer buffer) {
    // Read count (2 bytes), then column indices (2 bytes each)
}
```

### Step 1.4: Update MemoryLogRecordsBuilder for Writing

**File:** `fluss-common/src/main/java/org/apache/fluss/record/MemoryLogRecordsBuilder.java`

Changes:
1. Add `targetColumns` field
2. Add constructor/factory accepting `targetColumns`
3. In `build()`, if `targetColumns` present:
   - Set `PARTIAL_COLUMNS_FLAG` in attributes
   - Write target columns array to batch header
   - Use `LOG_MAGIC_VALUE_V1` instead of V0

### Step 1.5: Unit Tests for Common Layer

Create test classes:
- `PartialRowTest` - test position mapping, null returns for missing columns
- `DefaultLogRecordBatchPartialTest` - test parsing batches with partial columns
- `MemoryLogRecordsBuilderPartialTest` - test building batches with target columns

---

## Phase 2: Client API - Append Interface

### Step 2.1: Update Append Interface

**File:** `fluss-client/src/main/java/org/apache/fluss/client/table/writer/Append.java`

Add methods:
```java
/**
 * Apply partial insert columns and returns a new Append instance.
 */
Append partialInsert(int[] targetColumns);

/**
 * Apply partial insert columns by name.
 */
Append partialInsert(String... targetColumnNames);
```

### Step 2.2: Implement TableAppend

**File:** `fluss-client/src/main/java/org/apache/fluss/client/table/TableAppend.java`

Changes:
1. Add `@Nullable int[] targetColumns` field
2. Implement `partialInsert(int[])`:
   - Validate not null or empty
   - Validate all indices are valid
   - Validate no duplicates
   - Return new `TableAppend` instance with targetColumns set
3. Implement `partialInsert(String...)`:
   - Convert column names to indices
   - Delegate to `partialInsert(int[])`
4. Pass `targetColumns` to `AppendWriterImpl` constructor in `createWriter()`

### Step 2.3: Validation Logic

Implement validation in `TableAppend.partialInsert()`:
```java
private void validateTargetColumns(int[] targetColumns) {
    // 1. Not null or empty
    checkArgument(targetColumns != null && targetColumns.length > 0,
        "targetColumns must not be null or empty");

    // 2. Valid indices
    int fieldCount = tableInfo.getRowType().getFieldCount();
    for (int col : targetColumns) {
        checkArgument(col >= 0 && col < fieldCount,
            "Invalid column index: %d, must be 0-%d", col, fieldCount - 1);
    }

    // 3. No duplicates
    Set<Integer> seen = new HashSet<>();
    for (int col : targetColumns) {
        checkArgument(seen.add(col), "Duplicate column index: %d", col);
    }

    // 4. Bucket keys must be included
    for (String bucketKey : tableInfo.getBucketKeys()) {
        int idx = tableInfo.getRowType().getFieldIndex(bucketKey);
        checkArgument(contains(targetColumns, idx),
            "Target columns must include bucket key column '%s'", bucketKey);
    }

    // 5. Non-target columns must be nullable
    BitSet targetSet = toBitSet(targetColumns);
    RowType rowType = tableInfo.getRowType();
    for (int i = 0; i < fieldCount; i++) {
        if (!targetSet.get(i) && !rowType.getTypeAt(i).isNullable()) {
            throw new IllegalArgumentException(String.format(
                "Non-target column '%s' is NOT NULL, but partial insert requires nullable",
                rowType.getFieldNames().get(i)));
        }
    }
}
```

---

## Phase 3: Client - WriteRecord and Write Batches

### Step 3.1: Update WriteRecord

**File:** `fluss-client/src/main/java/org/apache/fluss/client/write/WriteRecord.java`

Add overloaded factory methods with `targetColumns`:
```java
public static WriteRecord forIndexedAppend(
    TableInfo tableInfo,
    PhysicalTablePath tablePath,
    IndexedRow row,
    @Nullable byte[] bucketKey,
    @Nullable int[] targetColumns) {
    // ...
}

public static WriteRecord forCompactedAppend(
    TableInfo tableInfo,
    PhysicalTablePath tablePath,
    CompactedRow row,
    @Nullable byte[] bucketKey,
    @Nullable int[] targetColumns) {
    // ...
}

public static WriteRecord forArrowAppend(
    TableInfo tableInfo,
    PhysicalTablePath tablePath,
    InternalRow row,
    @Nullable byte[] bucketKey,
    @Nullable int[] targetColumns) {
    // ...
}
```

### Step 3.2: Update AbstractRowLogWriteBatch

**File:** `fluss-client/src/main/java/org/apache/fluss/client/write/AbstractRowLogWriteBatch.java`

Changes:
1. Remove assertion: `checkArgument(writeRecord.getTargetColumns() == null, ...)`
2. Pass `targetColumns` to records builder
3. Handle partial row encoding with projected schema

### Step 3.3: Update ArrowLogWriteBatch

**File:** `fluss-client/src/main/java/org/apache/fluss/client/write/ArrowLogWriteBatch.java`

Changes:
1. Remove assertion blocking `targetColumns`
2. Handle partial row encoding for Arrow format

### Step 3.4: Update AppendWriterImpl

**File:** `fluss-client/src/main/java/org/apache/fluss/client/table/writer/AppendWriterImpl.java`

Changes:
1. Add `@Nullable int[] targetColumns` field
2. Update constructor to accept and store `targetColumns`
3. Modify `append(InternalRow row)`:
   ```java
   public CompletableFuture<AppendResult> append(InternalRow row) {
       if (targetColumns != null) {
           checkArgument(row.getFieldCount() == targetColumns.length,
               "Row field count (%d) must match target columns count (%d)",
               row.getFieldCount(), targetColumns.length);
       } else {
           checkFieldCount(row);
       }
       // ... rest of implementation
   }
   ```
4. Pass `targetColumns` to `WriteRecord.forXxxAppend()` calls

### Step 3.5: Unit Tests for Client Write Path

Create test classes:
- `TableAppendPartialTest` - test validation logic
- `AppendWriterImplPartialTest` - test partial row append
- `WriteRecordPartialTest` - test WriteRecord with targetColumns

---

## Phase 4: Integration Tests

### Step 4.1: Basic Partial Insert Test

**File:** `fluss-client/src/test/java/org/apache/fluss/client/table/PartialInsertITCase.java` (new)

Test scenarios:
1. Create log table with multiple columns
2. Insert partial rows using `partialInsert(int[])`
3. Insert partial rows using `partialInsert(String...)`
4. Read rows back and verify:
   - Target columns have correct values
   - Non-target columns are null

### Step 4.2: Validation Error Tests

Test that appropriate exceptions are thrown for:
- Empty target columns array
- Invalid column index
- Duplicate column index
- Missing bucket key column
- Non-nullable column excluded

### Step 4.3: Mixed Full and Partial Insert Test

Test scenario:
1. Create writer without partial insert (full row)
2. Create writer with partial insert
3. Insert rows with both writers to same table
4. Read all rows and verify correct data

### Step 4.4: Schema Evolution Test

Test scenario:
1. Insert partial rows with schema v1
2. Alter table to add new column (schema v2)
3. Read all rows with new schema
4. Verify old partial rows return null for new column

### Step 4.5: Different Log Formats Test

Test partial insert works with:
- INDEXED format
- COMPACTED format
- ARROW format

---

## Phase 5: Documentation and Finalization

### Step 5.1: Update API Documentation

Add Javadoc to new methods with examples.

### Step 5.2: Run Full Test Suite

```bash
./mvnw clean verify -pl fluss-client,fluss-common
```

### Step 5.3: Spotless Format Check

```bash
./mvnw spotless:check
./mvnw spotless:apply
```

---

## File Changes Summary

| Module | File | Change Type |
|--------|------|-------------|
| fluss-common | `LogRecordBatchFormat.java` | Modify - add flag |
| fluss-common | `PartialRow.java` | New |
| fluss-common | `DefaultLogRecordBatch.java` | Modify - parse partial |
| fluss-common | `MemoryLogRecordsBuilder.java` | Modify - write partial |
| fluss-client | `Append.java` | Modify - add methods |
| fluss-client | `TableAppend.java` | Modify - implement methods |
| fluss-client | `AppendWriterImpl.java` | Modify - accept targetColumns |
| fluss-client | `WriteRecord.java` | Modify - add overloads |
| fluss-client | `AbstractRowLogWriteBatch.java` | Modify - remove assertion |
| fluss-client | `ArrowLogWriteBatch.java` | Modify - remove assertion |

---

## Test Classes Summary

| Test Class | Purpose |
|------------|---------|
| `PartialRowTest` | Unit test for PartialRow wrapper |
| `DefaultLogRecordBatchPartialTest` | Unit test for reading partial batches |
| `MemoryLogRecordsBuilderPartialTest` | Unit test for writing partial batches |
| `TableAppendPartialTest` | Unit test for validation logic |
| `AppendWriterImplPartialTest` | Unit test for partial append |
| `PartialInsertITCase` | Integration test for end-to-end |

---

## Dependencies Between Steps

```
Phase 1 (Common Layer)
    ↓
Phase 2 (Client API)
    ↓
Phase 3 (Write Path)
    ↓
Phase 4 (Integration Tests)
    ↓
Phase 5 (Documentation)
```

---

## Success Criteria

1. ✅ `table.newAppend().partialInsert(cols).createWriter()` works
2. ✅ Partial rows stored with `LOG_MAGIC_VALUE_V1` and `PARTIAL_COLUMNS_FLAG`
3. ✅ Reading partial rows returns null for non-target columns
4. ✅ Validation catches all error conditions
5. ✅ All log formats (INDEXED, COMPACTED, ARROW) supported
6. ✅ All tests pass
7. ✅ Code formatted correctly
