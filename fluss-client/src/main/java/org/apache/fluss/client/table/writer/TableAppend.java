/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.client.table.writer;

import org.apache.fluss.client.write.WriterClient;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.RowType;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** API for configuring and creating {@link AppendWriter}. */
public class TableAppend implements Append {

    private final TablePath tablePath;
    private final TableInfo tableInfo;
    private final WriterClient writerClient;
    private final @Nullable int[] targetColumns;

    public TableAppend(TablePath tablePath, TableInfo tableInfo, WriterClient writerClient) {
        this(tablePath, tableInfo, writerClient, null);
    }

    private TableAppend(
            TablePath tablePath,
            TableInfo tableInfo,
            WriterClient writerClient,
            @Nullable int[] targetColumns) {
        this.tablePath = tablePath;
        this.tableInfo = tableInfo;
        this.writerClient = writerClient;
        this.targetColumns = targetColumns;
    }

    @Override
    public Append partialInsert(int[] targetColumns) {
        checkNotNull(targetColumns, "targetColumns must not be null");
        checkArgument(targetColumns.length > 0, "targetColumns must not be empty");
        int[] targetColumnsCopy = Arrays.copyOf(targetColumns, targetColumns.length);
        validateTargetColumns(targetColumnsCopy);
        return new TableAppend(tablePath, tableInfo, writerClient, targetColumnsCopy);
    }

    @Override
    public Append partialInsert(String... targetColumnNames) {
        checkNotNull(targetColumnNames, "targetColumnNames must not be null");
        checkArgument(targetColumnNames.length > 0, "targetColumnNames must not be empty");
        RowType rowType = tableInfo.getRowType();
        int[] columns = new int[targetColumnNames.length];
        for (int i = 0; i < targetColumnNames.length; i++) {
            columns[i] = rowType.getFieldIndex(targetColumnNames[i]);
            if (columns[i] == -1) {
                throw new IllegalArgumentException(
                        "Can not find target column: "
                                + targetColumnNames[i]
                                + " for table "
                                + tablePath
                                + ".");
            }
        }
        return partialInsert(columns);
    }

    @Override
    public AppendWriter createWriter() {
        return new AppendWriterImpl(tablePath, tableInfo, targetColumns, writerClient);
    }

    @Override
    public <T> TypedAppendWriter<T> createTypedWriter(Class<T> pojoClass) {
        return new TypedAppendWriterImpl<>(createWriter(), pojoClass, tableInfo, targetColumns);
    }

    private void validateTargetColumns(int[] targetColumns) {
        int fieldCount = tableInfo.getRowType().getFieldCount();
        RowType rowType = tableInfo.getRowType();

        // 1. Valid indices and no duplicates
        Set<Integer> seen = new HashSet<>();
        for (int col : targetColumns) {
            if (col < 0 || col >= fieldCount) {
                throw new IllegalArgumentException(
                        "Invalid column index: "
                                + col
                                + " for table "
                                + tablePath
                                + ". The table only has "
                                + fieldCount
                                + " columns.");
            }
            if (!seen.add(col)) {
                throw new IllegalArgumentException("Duplicate column index: " + col);
            }
        }

        // 2. Partition keys must be included
        List<String> partitionKeys = tableInfo.getPartitionKeys();
        BitSet targetSet = new BitSet();
        for (int col : targetColumns) {
            targetSet.set(col);
        }
        for (String partitionKey : partitionKeys) {
            int idx = rowType.getFieldIndex(partitionKey);
            if (!targetSet.get(idx)) {
                throw new IllegalArgumentException(
                        "Target columns must include partition key column '" + partitionKey + "'");
            }
        }

        // 3. Bucket keys must be included
        List<String> bucketKeys = tableInfo.getBucketKeys();
        for (String bucketKey : bucketKeys) {
            int idx = rowType.getFieldIndex(bucketKey);
            if (!targetSet.get(idx)) {
                throw new IllegalArgumentException(
                        "Target columns must include bucket key column '" + bucketKey + "'");
            }
        }

        // 4. Non-target columns must be nullable
        for (int i = 0; i < fieldCount; i++) {
            if (!targetSet.get(i) && !rowType.getTypeAt(i).isNullable()) {
                throw new IllegalArgumentException(
                        String.format(
                                "Non-target column '%s' is NOT NULL, "
                                        + "but partial insert requires all non-target columns to be nullable",
                                rowType.getFieldNames().get(i)));
            }
        }
    }
}
