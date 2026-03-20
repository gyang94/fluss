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

package org.apache.fluss.record;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.Schema.Column;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.InternalRow.FieldGetter;
import org.apache.fluss.row.ProjectedRow;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.ArrowUtils;
import org.apache.fluss.utils.MapUtils;
import org.apache.fluss.utils.Projection;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** A simple implementation for {@link LogRecordBatch.ReadContext}. */
@ThreadSafe
public class LogRecordReadContext implements LogRecordBatch.ReadContext, AutoCloseable {

    // the log format of the table
    private final LogFormat logFormat;
    // the final row schema returned by LogRecordBatch before CompletedFetch applies field getters.
    private final Schema outputSchema;
    private final RowType outputRowType;
    // the full schema of the target schema id.
    private final Schema targetFullSchema;
    // the static schemaId of the table, should support dynamic schema evolution in the future
    private final int targetSchemaId;
    // the Arrow memory buffer allocator for the table, should be null if not ARROW log format
    @Nullable private final BufferAllocator bufferAllocator;
    // the final selected fields of the read data
    private final FieldGetter[] selectedFieldGetters;
    // whether the projection is push downed to the server side and the returned data is pruned.
    private final boolean projectionPushDowned;
    @Nullable private final SchemaGetter schemaGetter;
    private final ConcurrentHashMap<VectorSchemaRootKey, VectorSchemaRoot> vectorSchemaRootMap =
            MapUtils.newConcurrentHashMap();

    public static LogRecordReadContext createReadContext(
            TableInfo tableInfo,
            boolean readFromRemote,
            @Nullable Projection projection,
            SchemaGetter schemaGetter) {
        RowType rowType = tableInfo.getRowType();
        LogFormat logFormat = tableInfo.getTableConfig().getLogFormat();
        // only for arrow log format, the projection can be push downed to the server side
        boolean projectionPushDowned =
                projection != null && logFormat == LogFormat.ARROW && !readFromRemote;
        int schemaId = tableInfo.getSchemaId();
        if (projection == null) {
            // set a default dummy projection to simplify code
            projection = Projection.of(IntStream.range(0, rowType.getFieldCount()).toArray());
        }

        if (logFormat == LogFormat.ARROW) {
            if (projectionPushDowned) {
                // Arrow data returned from server has already been projected in field order.
                Schema outputSchema =
                        projectSchema(tableInfo.getSchema(), projection.getProjectionInOrder());
                // Reorder the projected payload back to the requested field order.
                int[] selectedFields = projection.getReorderingIndexes();
                return createArrowReadContext(
                        outputSchema,
                        tableInfo.getSchema(),
                        schemaId,
                        selectedFields,
                        true,
                        schemaGetter);
            }
            // Remote Arrow reads do not support server-side projection pushdown.
            int[] selectedFields = projection.getProjection();
            return createArrowReadContext(
                    tableInfo.getSchema(),
                    tableInfo.getSchema(),
                    schemaId,
                    selectedFields,
                    false,
                    schemaGetter);
        } else if (logFormat == LogFormat.INDEXED) {
            int[] selectedFields = projection.getProjection();
            return createIndexedReadContext(rowType, schemaId, selectedFields, schemaGetter);
        } else if (logFormat == LogFormat.COMPACTED) {
            int[] selectedFields = projection.getProjection();
            return createCompactedRowReadContext(rowType, schemaId, selectedFields, schemaGetter);
        } else {
            throw new IllegalArgumentException("Unsupported log format: " + logFormat);
        }
    }

    private static LogRecordReadContext createArrowReadContext(
            Schema outputSchema,
            Schema targetFullSchema,
            int schemaId,
            int[] selectedFields,
            boolean projectionPushDowned,
            SchemaGetter schemaGetter) {
        // TODO: use a more reasonable memory limit
        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        FieldGetter[] fieldGetters =
                buildProjectedFieldGetters(outputSchema.getRowType(), selectedFields);
        return new LogRecordReadContext(
                LogFormat.ARROW,
                outputSchema,
                targetFullSchema,
                schemaId,
                allocator,
                fieldGetters,
                projectionPushDowned,
                schemaGetter);
    }

    /**
     * Creates a LogRecordReadContext for ARROW log format, that underlying Arrow resources are not
     * reused.
     *
     * @param rowType the schema of the table
     * @param schemaId the schemaId of the table
     * @param schemaGetter the schema getter of to get schema by schemaId
     */
    @VisibleForTesting
    public static LogRecordReadContext createArrowReadContext(
            RowType rowType, int schemaId, SchemaGetter schemaGetter) {
        int[] selectedFields = IntStream.range(0, rowType.getFieldCount()).toArray();
        return createArrowReadContext(
                schemaFromRowType(rowType),
                schemaFromRowType(rowType),
                schemaId,
                selectedFields,
                false,
                schemaGetter);
    }

    @VisibleForTesting
    public static LogRecordReadContext createArrowReadContext(
            RowType rowType,
            int schemaId,
            SchemaGetter schemaGetter,
            boolean projectionPushDowned) {
        int[] selectedFields = IntStream.range(0, rowType.getFieldCount()).toArray();
        return createArrowReadContext(
                schemaFromRowType(rowType),
                projectionPushDowned
                        ? resolveTargetFullSchema(rowType, schemaId, schemaGetter)
                        : schemaFromRowType(rowType),
                schemaId,
                selectedFields,
                projectionPushDowned,
                schemaGetter);
    }

    /**
     * Creates a LogRecordReadContext for INDEXED log format.
     *
     * @param rowType the schema of the table
     * @param schemaId the schemaId of the table
     * @param schemaGetter the schema getter of to get schema by schemaId
     */
    @VisibleForTesting
    public static LogRecordReadContext createIndexedReadContext(
            RowType rowType, int schemaId, SchemaGetter schemaGetter) {
        int[] selectedFields = IntStream.range(0, rowType.getFieldCount()).toArray();
        return createIndexedReadContext(rowType, schemaId, selectedFields, schemaGetter);
    }

    /** Creates a LogRecordReadContext for COMPACTED log format. */
    public static LogRecordReadContext createCompactedRowReadContext(
            RowType rowType, int schemaId) {
        int[] selectedFields = IntStream.range(0, rowType.getFieldCount()).toArray();
        return createCompactedRowReadContext(rowType, schemaId, selectedFields);
    }

    /** Creates a LogRecordReadContext for COMPACTED log format with schema evolution support. */
    public static LogRecordReadContext createCompactedRowReadContext(
            RowType rowType, int schemaId, SchemaGetter schemaGetter) {
        int[] selectedFields = IntStream.range(0, rowType.getFieldCount()).toArray();
        return createCompactedRowReadContext(rowType, schemaId, selectedFields, schemaGetter);
    }

    /**
     * Creates a LogRecordReadContext for INDEXED log format.
     *
     * @param rowType the schema of the read data
     * @param schemaId the schemaId of the table
     * @param selectedFields the final selected fields of the read data
     * @param schemaGetter the schema getter of to get schema by schemaId
     */
    public static LogRecordReadContext createIndexedReadContext(
            RowType rowType, int schemaId, int[] selectedFields, SchemaGetter schemaGetter) {
        Schema outputSchema = schemaFromRowType(rowType);
        FieldGetter[] fieldGetters =
                buildProjectedFieldGetters(outputSchema.getRowType(), selectedFields);
        // for INDEXED log format, the projection is NEVER push downed to the server side
        return new LogRecordReadContext(
                LogFormat.INDEXED,
                outputSchema,
                outputSchema,
                schemaId,
                null,
                fieldGetters,
                false,
                schemaGetter);
    }

    /**
     * Creates a LogRecordReadContext for COMPACTED log format.
     *
     * @param rowType the schema of the read data
     * @param schemaId the schemaId of the table
     * @param selectedFields the final selected fields of the read data
     */
    public static LogRecordReadContext createCompactedRowReadContext(
            RowType rowType, int schemaId, int[] selectedFields) {
        return createCompactedRowReadContext(rowType, schemaId, selectedFields, null);
    }

    /** Creates a LogRecordReadContext for COMPACTED log format. */
    public static LogRecordReadContext createCompactedRowReadContext(
            RowType rowType,
            int schemaId,
            int[] selectedFields,
            @Nullable SchemaGetter schemaGetter) {
        Schema outputSchema = schemaFromRowType(rowType);
        FieldGetter[] fieldGetters =
                buildProjectedFieldGetters(outputSchema.getRowType(), selectedFields);
        // for COMPACTED log format, the projection is NEVER push downed to the server side
        return new LogRecordReadContext(
                LogFormat.COMPACTED,
                outputSchema,
                outputSchema,
                schemaId,
                null,
                fieldGetters,
                false,
                schemaGetter);
    }

    private LogRecordReadContext(
            LogFormat logFormat,
            Schema outputSchema,
            Schema targetFullSchema,
            int targetSchemaId,
            BufferAllocator bufferAllocator,
            FieldGetter[] selectedFieldGetters,
            boolean projectionPushDowned,
            @Nullable SchemaGetter schemaGetter) {
        this.logFormat = logFormat;
        this.outputSchema = outputSchema;
        this.outputRowType = outputSchema.getRowType();
        this.targetFullSchema = targetFullSchema;
        this.targetSchemaId = targetSchemaId;
        this.bufferAllocator = bufferAllocator;
        this.selectedFieldGetters = selectedFieldGetters;
        this.projectionPushDowned = projectionPushDowned;
        this.schemaGetter = schemaGetter;
    }

    @Override
    public LogFormat getLogFormat() {
        return logFormat;
    }

    @Override
    public RowType getFullRowType(int schemaId) {
        return getFullSchema(schemaId).getRowType();
    }

    @Override
    public RowType getStoredRowType(int schemaId, @Nullable int[] targetColumns) {
        if (targetColumns != null) {
            return getFullRowType(schemaId).project(targetColumns);
        }
        if (projectionPushDowned) {
            return outputRowType;
        }
        return getFullRowType(schemaId);
    }

    /** Get the selected field getters for the read data. */
    public FieldGetter[] getSelectedFieldGetters() {
        return selectedFieldGetters;
    }

    /** Whether the projection is push downed to the server side and the returned data is pruned. */
    public boolean isProjectionPushDowned() {
        return projectionPushDowned;
    }

    @Override
    public VectorSchemaRoot getVectorSchemaRoot(int schemaId, @Nullable int[] targetColumns) {
        if (logFormat != LogFormat.ARROW) {
            throw new IllegalArgumentException(
                    "Only Arrow log format provides vector schema root.");
        }

        RowType rowType = getStoredRowType(schemaId, targetColumns);
        VectorSchemaRootKey key =
                new VectorSchemaRootKey(schemaId, targetColumns, rowType.asSerializableString());
        return vectorSchemaRootMap.computeIfAbsent(
                key,
                (ignored) ->
                        VectorSchemaRoot.create(
                                ArrowUtils.toArrowSchema(rowType), bufferAllocator));
    }

    @Override
    public BufferAllocator getBufferAllocator() {
        if (logFormat != LogFormat.ARROW) {
            throw new IllegalArgumentException("Only Arrow log format provides buffer allocator.");
        }
        checkNotNull(bufferAllocator, "The buffer allocator is not available.");
        return bufferAllocator;
    }

    @Nullable
    @Override
    public ProjectedRow getOutputProjectedRow(int schemaId) {
        Schema originSchema = getFullSchema(schemaId);
        if (originSchema.getRowType().equalsWithFieldId(outputRowType)) {
            return null;
        }
        return ProjectedRow.from(originSchema, outputSchema);
    }

    public void close() {
        vectorSchemaRootMap.values().forEach(VectorSchemaRoot::close);
        if (bufferAllocator != null) {
            bufferAllocator.close();
        }
    }

    private Schema getFullSchema(int schemaId) {
        if (schemaId == targetSchemaId) {
            return targetFullSchema;
        }
        checkNotNull(schemaGetter, "Schema getter is required to resolve schema %s.", schemaId);
        return schemaGetter.getSchema(schemaId);
    }

    private static FieldGetter[] buildProjectedFieldGetters(RowType rowType, int[] selectedFields) {
        List<DataType> dataTypeList = rowType.getChildren();
        FieldGetter[] fieldGetters = new FieldGetter[selectedFields.length];
        for (int i = 0; i < fieldGetters.length; i++) {
            // build deep field getter to support nested types
            fieldGetters[i] =
                    InternalRow.createDeepFieldGetter(
                            dataTypeList.get(selectedFields[i]), selectedFields[i]);
        }
        return fieldGetters;
    }

    private static Schema resolveTargetFullSchema(
            RowType rowType, int schemaId, @Nullable SchemaGetter schemaGetter) {
        if (schemaGetter != null) {
            return schemaGetter.getSchema(schemaId);
        }
        return schemaFromRowType(rowType);
    }

    private static Schema projectSchema(Schema schema, int[] selectedFields) {
        List<Column> columns = schema.getColumns();
        return Schema.newBuilder()
                .fromColumns(
                        Arrays.stream(selectedFields)
                                .mapToObj(columns::get)
                                .collect(java.util.stream.Collectors.toList()))
                .build();
    }

    private static Schema schemaFromRowType(RowType rowType) {
        List<Column> columns =
                rowType.getFields().stream()
                        .map(
                                field ->
                                        new Column(
                                                field.getName(),
                                                field.getType(),
                                                field.getDescription().orElse(null),
                                                field.getFieldId()))
                        .collect(java.util.stream.Collectors.toList());
        try {
            return Schema.newBuilder().fromColumns(columns).build();
        } catch (IllegalStateException e) {
            return Schema.newBuilder().fromRowType(rowType).build();
        }
    }

    private static final class VectorSchemaRootKey {
        private final int schemaId;
        @Nullable private final int[] targetColumns;
        private final String storedRowTypeSignature;

        private VectorSchemaRootKey(
                int schemaId, @Nullable int[] targetColumns, String storedRowTypeSignature) {
            this.schemaId = schemaId;
            this.targetColumns =
                    targetColumns == null
                            ? null
                            : Arrays.copyOf(targetColumns, targetColumns.length);
            this.storedRowTypeSignature = storedRowTypeSignature;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof VectorSchemaRootKey)) {
                return false;
            }
            VectorSchemaRootKey that = (VectorSchemaRootKey) o;
            return schemaId == that.schemaId
                    && Arrays.equals(targetColumns, that.targetColumns)
                    && storedRowTypeSignature.equals(that.storedRowTypeSignature);
        }

        @Override
        public int hashCode() {
            int result = Integer.hashCode(schemaId);
            result = 31 * result + Arrays.hashCode(targetColumns);
            result = 31 * result + storedRowTypeSignature.hashCode();
            return result;
        }
    }
}
