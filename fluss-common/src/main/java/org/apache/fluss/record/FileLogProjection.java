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
import org.apache.fluss.compression.ArrowCompressionInfo;
import org.apache.fluss.exception.InvalidColumnProjectionException;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.record.bytesview.MultiBytesView;
import org.apache.fluss.shaded.arrow.com.google.flatbuffers.FlatBufferBuilder;
import org.apache.fluss.shaded.arrow.org.apache.arrow.flatbuf.Buffer;
import org.apache.fluss.shaded.arrow.org.apache.arrow.flatbuf.FieldNode;
import org.apache.fluss.shaded.arrow.org.apache.arrow.flatbuf.Message;
import org.apache.fluss.shaded.arrow.org.apache.arrow.flatbuf.RecordBatch;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.TypeLayout;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.ipc.message.ArrowBodyCompression;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.ipc.message.ArrowBuffer;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo.Field;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo.Schema;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.ArrowUtils;
import org.apache.fluss.utils.crc.Crc32C;
import org.apache.fluss.utils.types.Tuple2;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.Checksum;

import static org.apache.fluss.record.DefaultLogRecordBatch.APPEND_ONLY_FLAG_MASK;
import static org.apache.fluss.record.LogRecordBatchFormat.BASE_OFFSET_OFFSET;
import static org.apache.fluss.record.LogRecordBatchFormat.COMMIT_TIMESTAMP_OFFSET;
import static org.apache.fluss.record.LogRecordBatchFormat.LENGTH_OFFSET;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V0;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V1;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_OVERHEAD;
import static org.apache.fluss.record.LogRecordBatchFormat.MAGIC_OFFSET;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_LEADER_EPOCH;
import static org.apache.fluss.record.LogRecordBatchFormat.PARTIAL_COLUMNS_FLAG;
import static org.apache.fluss.record.LogRecordBatchFormat.V0_RECORD_BATCH_HEADER_SIZE;
import static org.apache.fluss.record.LogRecordBatchFormat.V1_RECORD_BATCH_HEADER_SIZE;
import static org.apache.fluss.record.LogRecordBatchFormat.attributeOffset;
import static org.apache.fluss.record.LogRecordBatchFormat.batchSequenceOffset;
import static org.apache.fluss.record.LogRecordBatchFormat.crcOffset;
import static org.apache.fluss.record.LogRecordBatchFormat.lastOffsetDeltaOffset;
import static org.apache.fluss.record.LogRecordBatchFormat.leaderEpochOffset;
import static org.apache.fluss.record.LogRecordBatchFormat.recordBatchHeaderSize;
import static org.apache.fluss.record.LogRecordBatchFormat.recordBatchHeaderSizeWithPartialColumns;
import static org.apache.fluss.record.LogRecordBatchFormat.recordsCountOffset;
import static org.apache.fluss.record.LogRecordBatchFormat.schemaIdOffset;
import static org.apache.fluss.record.LogRecordBatchFormat.writeClientIdOffset;
import static org.apache.fluss.utils.FileUtils.readFully;
import static org.apache.fluss.utils.FileUtils.readFullyOrFail;

/** Column projection util on Arrow format {@link FileLogRecords}. */
public class FileLogProjection {

    // see the arrow binary message format in the page:
    // https://arrow.apache.org/docs/format/Columnar.html#encapsulated-message-format
    private static final int ARROW_IPC_CONTINUATION_LENGTH = 4;
    private static final int ARROW_IPC_METADATA_SIZE_OFFSET = ARROW_IPC_CONTINUATION_LENGTH;
    private static final int ARROW_IPC_METADATA_SIZE_LENGTH = 4;
    private static final int ARROW_HEADER_SIZE =
            ARROW_IPC_CONTINUATION_LENGTH + ARROW_IPC_METADATA_SIZE_LENGTH;

    // the projection cache shared in the TabletServer
    private final ProjectionPushdownCache projectionsCache;

    // shared resources for multiple projections
    private final ByteArrayOutputStream outputStream;
    private final WriteChannel writeChannel;

    /**
     * Buffer to read log records batch header. V1 is larger than V0, so use V1 head buffer can read
     * V0 header even if there is no enough bytes in log file.
     */
    private final ByteBuffer logHeaderBuffer = ByteBuffer.allocate(V1_RECORD_BATCH_HEADER_SIZE);

    private final ByteBuffer arrowHeaderBuffer = ByteBuffer.allocate(ARROW_HEADER_SIZE);
    private final ByteBuffer targetColumnsCountBuffer = ByteBuffer.allocate(2);
    private final ByteBuffer checksumBuffer = ByteBuffer.allocate(8 * 1024);
    private ByteBuffer arrowMetadataBuffer;
    private SchemaGetter schemaGetter;
    private long tableId;
    private ArrowCompressionInfo compressionInfo;
    private int[] requestedColumnIds;
    private RowType requestedOutputRowType;
    private String requestedOutputSignature;

    public FileLogProjection(ProjectionPushdownCache projectionsCache) {
        this.projectionsCache = projectionsCache;
        this.outputStream = new ByteArrayOutputStream();
        this.writeChannel = new WriteChannel(Channels.newChannel(outputStream));
        // fluss use little endian for encoding log records batch
        this.logHeaderBuffer.order(ByteOrder.LITTLE_ENDIAN);
        // arrow force use little endian to encode int32 values
        this.arrowHeaderBuffer.order(ByteOrder.LITTLE_ENDIAN);
        this.targetColumnsCountBuffer.order(ByteOrder.LITTLE_ENDIAN);
    }

    public void setCurrentProjection(
            long tableId,
            SchemaGetter schemaGetter,
            ArrowCompressionInfo compressionInfo,
            int[] selectedFieldPositions) {
        SchemaInfo latestSchemaInfo = schemaGetter.getLatestSchemaInfo();
        org.apache.fluss.metadata.Schema latestSchema = latestSchemaInfo.getSchema();
        RowType latestRowType = latestSchema.getRowType();
        // Validate the incoming fetch projection in the latest logical schema first.
        toBitSet(latestRowType.getFieldCount(), selectedFieldPositions);

        this.tableId = tableId;
        this.schemaGetter = schemaGetter;
        this.compressionInfo = compressionInfo;
        this.requestedColumnIds = new int[selectedFieldPositions.length];
        for (int i = 0; i < selectedFieldPositions.length; i++) {
            this.requestedColumnIds[i] =
                    latestSchema.getColumns().get(selectedFieldPositions[i]).getColumnId();
        }
        this.requestedOutputRowType = latestRowType.project(selectedFieldPositions);
        this.requestedOutputSignature = requestedOutputRowType.asSerializableString();
    }

    /**
     * Project the log records to a subset of fields and the size of returned log records shouldn't
     * exceed maxBytes.
     *
     * @return the projected records.
     */
    public BytesViewLogRecords project(FileChannel channel, int start, int end, int maxBytes)
            throws IOException {

        MultiBytesView.Builder builder = MultiBytesView.builder();
        int position = start;

        // The condition is an optimization to avoid read log header when there is no enough bytes,
        // So we use V0 header size here for a conservative judgment. In the end, the condition
        // of (position >= end - recordBatchHeaderSize) will ensure the final correctness.
        while (maxBytes > V0_RECORD_BATCH_HEADER_SIZE) {
            if (position > end - V0_RECORD_BATCH_HEADER_SIZE) {
                // the remaining bytes in the file are not enough to read a batch header up to
                // magic.
                return new BytesViewLogRecords(builder.build());
            }
            // read log header
            logHeaderBuffer.rewind();
            readLogHeaderFullyOrFail(channel, logHeaderBuffer, position);

            logHeaderBuffer.rewind();
            byte sourceMagic = logHeaderBuffer.get(MAGIC_OFFSET);
            int standardHeaderSize = recordBatchHeaderSize(sourceMagic);
            int batchSizeInBytes = LOG_OVERHEAD + logHeaderBuffer.getInt(LENGTH_OFFSET);
            short schemaId = logHeaderBuffer.getShort(schemaIdOffset(sourceMagic));
            boolean isPartial =
                    (logHeaderBuffer.get(attributeOffset(sourceMagic)) & PARTIAL_COLUMNS_FLAG) != 0;
            int[] storedTargetColumns =
                    isPartial
                            ? readStoredTargetColumns(
                                    channel, position + recordBatchHeaderSize(sourceMagic))
                            : null;
            int effectiveHeaderSize =
                    storedTargetColumns == null
                            ? standardHeaderSize
                            : recordBatchHeaderSizeWithPartialColumns(
                                    sourceMagic, storedTargetColumns.length);
            ProjectionInfo currentProjection =
                    getOrCreateProjectionInfo(schemaId, storedTargetColumns);

            if (position > end - batchSizeInBytes) {
                // the remaining bytes in the file are not enough to read a full batch
                return new BytesViewLogRecords(builder.build());
            }

            // Return empty batch to push forward log offset. The empty batch was generated when
            // build cdc log batch when there
            // is no cdc log generated for this kv batch. See the comments about the field
            // 'lastOffsetDelta' in DefaultLogRecordBatch.
            if (batchSizeInBytes == effectiveHeaderSize) {
                builder.addBytes(channel, position, batchSizeInBytes);
                position += batchSizeInBytes;
                continue;
            }

            boolean isAppendOnly =
                    (logHeaderBuffer.get(attributeOffset(sourceMagic)) & APPEND_ONLY_FLAG_MASK) > 0;

            final int changeTypeBytes;
            final long arrowHeaderOffset;
            if (isAppendOnly) {
                changeTypeBytes = 0;
                arrowHeaderOffset = position + effectiveHeaderSize;
            } else {
                changeTypeBytes = logHeaderBuffer.getInt(recordsCountOffset(sourceMagic));
                arrowHeaderOffset = position + effectiveHeaderSize + changeTypeBytes;
            }

            // read arrow header
            arrowHeaderBuffer.rewind();
            readFullyOrFail(channel, arrowHeaderBuffer, arrowHeaderOffset, "arrow header");
            arrowHeaderBuffer.position(ARROW_IPC_METADATA_SIZE_OFFSET);
            int arrowMetadataSize = arrowHeaderBuffer.getInt();

            resizeArrowMetadataBuffer(arrowMetadataSize);
            arrowMetadataBuffer.rewind();
            readFullyOrFail(
                    channel,
                    arrowMetadataBuffer,
                    arrowHeaderOffset + ARROW_HEADER_SIZE,
                    "arrow metadata");

            arrowMetadataBuffer.rewind();
            Message metadata = Message.getRootAsMessage(arrowMetadataBuffer);
            ProjectedArrowBatch projectedArrowBatch =
                    projectArrowBatch(
                            metadata,
                            currentProjection.nodesProjection,
                            currentProjection.buffersProjection,
                            currentProjection.bufferCount);
            long arrowBodyLength = projectedArrowBatch.bodyLength();
            byte responseMagic =
                    resolveResponseMagic(sourceMagic, currentProjection.responseTargetColumns);
            int projectedHeaderSize =
                    currentProjection.responseTargetColumns == null
                            ? recordBatchHeaderSize(responseMagic)
                            : recordBatchHeaderSizeWithPartialColumns(
                                    responseMagic, currentProjection.responseTargetColumns.length);

            // 3. create new arrow batch metadata which already projected.
            byte[] headerMetadata =
                    serializeArrowRecordBatchMetadata(
                            projectedArrowBatch,
                            arrowBodyLength,
                            currentProjection.bodyCompression);

            int newBatchSizeInBytes =
                    projectedHeaderSize
                            + changeTypeBytes
                            + headerMetadata.length
                            + (int) arrowBodyLength; // safe to cast to int
            if (newBatchSizeInBytes > maxBytes) {
                // the remaining bytes in the file are not enough to read a full batch
                return new BytesViewLogRecords(builder.build());
            }

            // 4. update and copy log batch header
            byte[] logHeader =
                    buildProjectedLogHeader(
                            sourceMagic,
                            responseMagic,
                            projectedHeaderSize,
                            newBatchSizeInBytes,
                            currentProjection.responseTargetColumns);
            long changeTypeOffset = position + effectiveHeaderSize;

            // 5. build log records
            long bufferOffset = arrowHeaderOffset + ARROW_HEADER_SIZE + arrowMetadataSize;
            long crc =
                    computeProjectedBatchCrc(
                            logHeader,
                            responseMagic,
                            channel,
                            changeTypeOffset,
                            changeTypeBytes,
                            headerMetadata,
                            bufferOffset,
                            projectedArrowBatch.buffers);
            ByteBuffer.wrap(logHeader)
                    .order(ByteOrder.LITTLE_ENDIAN)
                    .putInt(crcOffset(responseMagic), (int) crc);
            builder.addBytes(logHeader);
            if (!isAppendOnly) {
                builder.addBytes(channel, changeTypeOffset, changeTypeBytes);
            }
            builder.addBytes(headerMetadata);
            projectedArrowBatch.buffers.forEach(
                    b ->
                            builder.addBytes(
                                    channel, bufferOffset + b.getOffset(), (int) b.getSize()));

            maxBytes -= newBatchSizeInBytes;
            position += batchSizeInBytes;
        }

        return new BytesViewLogRecords(builder.build());
    }

    private int[] readStoredTargetColumns(FileChannel channel, long position) throws IOException {
        targetColumnsCountBuffer.clear();
        readFullyOrFail(
                channel, targetColumnsCountBuffer, position, "partial target columns count");
        targetColumnsCountBuffer.flip();
        int targetColumnCount = targetColumnsCountBuffer.getShort() & 0xFFFF;
        if (targetColumnCount == 0) {
            return new int[0];
        }
        ByteBuffer targetColumnsBuffer =
                ByteBuffer.allocate(targetColumnCount * Short.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        readFullyOrFail(
                channel, targetColumnsBuffer, position + Short.BYTES, "partial target columns");
        targetColumnsBuffer.flip();
        int[] targetColumns = new int[targetColumnCount];
        for (int i = 0; i < targetColumnCount; i++) {
            targetColumns[i] = targetColumnsBuffer.getShort() & 0xFFFF;
        }
        return targetColumns;
    }

    private byte[] buildProjectedLogHeader(
            byte sourceMagic,
            byte responseMagic,
            int projectedHeaderSize,
            int newBatchSizeInBytes,
            @javax.annotation.Nullable int[] responseTargetColumns) {
        byte[] logHeader = new byte[projectedHeaderSize];
        ByteBuffer headerBuffer = ByteBuffer.wrap(logHeader).order(ByteOrder.LITTLE_ENDIAN);
        if (sourceMagic == responseMagic) {
            int standardHeaderSize = recordBatchHeaderSize(sourceMagic);
            logHeaderBuffer.rewind();
            logHeaderBuffer.get(logHeader, 0, standardHeaderSize);
        } else if (sourceMagic == LOG_MAGIC_VALUE_V0 && responseMagic == LOG_MAGIC_VALUE_V1) {
            headerBuffer.putLong(BASE_OFFSET_OFFSET, logHeaderBuffer.getLong(BASE_OFFSET_OFFSET));
            headerBuffer.putInt(LENGTH_OFFSET, newBatchSizeInBytes - LOG_OVERHEAD);
            headerBuffer.put(MAGIC_OFFSET, responseMagic);
            headerBuffer.putLong(
                    COMMIT_TIMESTAMP_OFFSET, logHeaderBuffer.getLong(COMMIT_TIMESTAMP_OFFSET));
            headerBuffer.putInt(leaderEpochOffset(responseMagic), NO_LEADER_EPOCH);
            headerBuffer.putInt(crcOffset(responseMagic), 0);
            headerBuffer.putShort(
                    schemaIdOffset(responseMagic),
                    logHeaderBuffer.getShort(schemaIdOffset(sourceMagic)));
            headerBuffer.put(
                    attributeOffset(responseMagic),
                    logHeaderBuffer.get(attributeOffset(sourceMagic)));
            headerBuffer.putInt(
                    lastOffsetDeltaOffset(responseMagic),
                    logHeaderBuffer.getInt(lastOffsetDeltaOffset(sourceMagic)));
            headerBuffer.putLong(
                    writeClientIdOffset(responseMagic),
                    logHeaderBuffer.getLong(writeClientIdOffset(sourceMagic)));
            headerBuffer.putInt(
                    batchSequenceOffset(responseMagic),
                    logHeaderBuffer.getInt(batchSequenceOffset(sourceMagic)));
            headerBuffer.putInt(
                    recordsCountOffset(responseMagic),
                    logHeaderBuffer.getInt(recordsCountOffset(sourceMagic)));
        } else {
            throw new IllegalArgumentException(
                    String.format(
                            "Unsupported projected header rewrite from magic v%s to v%s",
                            sourceMagic, responseMagic));
        }
        headerBuffer.putInt(LENGTH_OFFSET, newBatchSizeInBytes - LOG_OVERHEAD);
        headerBuffer.putInt(crcOffset(responseMagic), 0);
        byte attributes = headerBuffer.get(attributeOffset(responseMagic));
        attributes =
                responseTargetColumns == null
                        ? (byte) (attributes & ~PARTIAL_COLUMNS_FLAG)
                        : (byte) (attributes | PARTIAL_COLUMNS_FLAG);
        headerBuffer.put(attributeOffset(responseMagic), attributes);
        if (responseTargetColumns != null) {
            headerBuffer.position(recordBatchHeaderSize(responseMagic));
            headerBuffer.putShort((short) responseTargetColumns.length);
            for (int targetColumn : responseTargetColumns) {
                headerBuffer.putShort((short) targetColumn);
            }
        }
        return logHeader;
    }

    private static byte resolveResponseMagic(
            byte sourceMagic, @javax.annotation.Nullable int[] responseTargetColumns) {
        if (responseTargetColumns != null && sourceMagic == LOG_MAGIC_VALUE_V0) {
            return LOG_MAGIC_VALUE_V1;
        }
        return sourceMagic;
    }

    private long computeProjectedBatchCrc(
            byte[] logHeader,
            byte magic,
            FileChannel channel,
            long changeTypeOffset,
            int changeTypeBytes,
            byte[] headerMetadata,
            long bufferOffset,
            List<ArrowBuffer> buffers)
            throws IOException {
        Checksum checksum = Crc32C.create();
        checksum.update(logHeader, schemaIdOffset(magic), logHeader.length - schemaIdOffset(magic));
        if (changeTypeBytes > 0) {
            updateChecksumFromChannel(checksum, channel, changeTypeOffset, changeTypeBytes);
        }
        checksum.update(headerMetadata, 0, headerMetadata.length);
        for (ArrowBuffer buffer : buffers) {
            updateChecksumFromChannel(
                    checksum, channel, bufferOffset + buffer.getOffset(), (int) buffer.getSize());
        }
        return checksum.getValue();
    }

    private void updateChecksumFromChannel(
            Checksum checksum, FileChannel channel, long position, int size) throws IOException {
        long remaining = size;
        long currentPosition = position;
        while (remaining > 0) {
            checksumBuffer.clear();
            checksumBuffer.limit((int) Math.min(checksumBuffer.capacity(), remaining));
            readFullyOrFail(channel, checksumBuffer, currentPosition, "projected log batch bytes");
            int bytesRead = checksumBuffer.position();
            checksum.update(checksumBuffer.array(), 0, bytesRead);
            currentPosition += bytesRead;
            remaining -= bytesRead;
        }
    }

    private ProjectedArrowBatch projectArrowBatch(
            Message metadata, BitSet nodesProjection, BitSet buffersProjection, int bufferCount) {
        List<ArrowFieldNode> newNodes = new ArrayList<>();
        List<ArrowBuffer> newBufferLayouts = new ArrayList<>();
        List<ArrowBuffer> selectedBuffers = new ArrayList<>();
        RecordBatch recordBatch = (RecordBatch) metadata.header(new RecordBatch());
        long numRecords = recordBatch.length();
        for (int i = nodesProjection.nextSetBit(0); i >= 0; i = nodesProjection.nextSetBit(i + 1)) {
            FieldNode node = recordBatch.nodes(i);
            newNodes.add(new ArrowFieldNode(node.length(), node.nullCount()));
        }
        long bodyLength = metadata.bodyLength();
        long newOffset = 0L;
        for (int i = buffersProjection.nextSetBit(0);
                i >= 0;
                i = buffersProjection.nextSetBit(i + 1)) {
            Buffer buf = recordBatch.buffers(i);
            long nextOffset =
                    i < bufferCount - 1 ? recordBatch.buffers(i + 1).offset() : bodyLength;
            long paddedLength = nextOffset - buf.offset();
            selectedBuffers.add(new ArrowBuffer(buf.offset(), paddedLength));
            newBufferLayouts.add(new ArrowBuffer(newOffset, buf.length()));
            newOffset += paddedLength;
        }

        return new ProjectedArrowBatch(numRecords, newNodes, newBufferLayouts, selectedBuffers);
    }

    /**
     * Serialize metadata of a {@link ArrowRecordBatch}. This avoids to create an instance of {@link
     * ArrowRecordBatch}.
     *
     * @see MessageSerializer#serialize(WriteChannel, ArrowRecordBatch)
     * @see ArrowRecordBatch#writeTo(FlatBufferBuilder)
     */
    private byte[] serializeArrowRecordBatchMetadata(
            ProjectedArrowBatch batch, long arrowBodyLength, ArrowBodyCompression bodyCompression)
            throws IOException {
        outputStream.reset();
        ArrowUtils.serializeArrowRecordBatchMetadata(
                writeChannel,
                batch.numRecords,
                batch.nodes,
                batch.buffersLayout,
                bodyCompression,
                arrowBodyLength);
        return outputStream.toByteArray();
    }

    private void resizeArrowMetadataBuffer(int metadataSize) {
        if (arrowMetadataBuffer == null || arrowMetadataBuffer.capacity() < metadataSize) {
            arrowMetadataBuffer = ByteBuffer.allocate(metadataSize);
            arrowMetadataBuffer.order(ByteOrder.LITTLE_ENDIAN);
        } else {
            arrowMetadataBuffer.limit(metadataSize);
        }
    }

    /** Flatten fields by a pre-order depth-first traversal of the fields in the schema. */
    private void flattenFields(
            List<Field> arrowFields,
            BitSet selectedFields,
            List<Tuple2<Field, Boolean>> flattenedFields) {
        for (int i = 0; i < arrowFields.size(); i++) {
            Field field = arrowFields.get(i);
            boolean selected = selectedFields.get(i);
            flattenedFields.add(Tuple2.of(field, selected));
            List<Field> children = field.getChildren();
            flattenFields(children, fillBitSet(children.size(), selected), flattenedFields);
        }
    }

    private static BitSet toBitSet(int length, int[] selectedIndexes) {
        BitSet bitset = new BitSet(length);
        int prev = -1;
        for (int i : selectedIndexes) {
            if (i < prev) {
                throw new InvalidColumnProjectionException(
                        "The projection indexes should be in field order, but is "
                                + Arrays.toString(selectedIndexes));
            } else if (i == prev) {
                throw new InvalidColumnProjectionException(
                        "The projection indexes should not contain duplicated fields, but is "
                                + Arrays.toString(selectedIndexes));
            } else if (i >= length) {
                throw new InvalidColumnProjectionException(
                        "Projected fields "
                                + Arrays.toString(selectedIndexes)
                                + " is out of bound for schema with "
                                + length
                                + " fields.");
            }
            bitset.set(i);
            prev = i;
        }
        return bitset;
    }

    private static BitSet fillBitSet(int length, boolean value) {
        BitSet bitset = new BitSet(length);
        if (value) {
            bitset.set(0, length);
        } else {
            bitset.clear();
        }
        return bitset;
    }

    /**
     * Read log header fully or fail with EOFException if there is no enough bytes to read a full
     * log header. This handles different log header size for magic v0 and v1.
     */
    static void readLogHeaderFullyOrFail(FileChannel channel, ByteBuffer buffer, int position)
            throws IOException {
        if (position < 0) {
            throw new IllegalArgumentException(
                    "The file channel position cannot be negative, but it is " + position);
        }
        readFully(channel, buffer, position);
        if (buffer.hasRemaining()) {
            int size = buffer.position();
            byte magic = buffer.get(MAGIC_OFFSET);
            if (magic == LOG_MAGIC_VALUE_V0 && size < V0_RECORD_BATCH_HEADER_SIZE) {
                throw new EOFException(
                        String.format(
                                "Failed to read v0 log header from file channel `%s`. Expected to read %d bytes, "
                                        + "but reached end of file after reading %d bytes. Started read from position %d.",
                                channel, V0_RECORD_BATCH_HEADER_SIZE, size, position));
            } else if (magic == LOG_MAGIC_VALUE_V1 && size < V1_RECORD_BATCH_HEADER_SIZE) {
                throw new EOFException(
                        String.format(
                                "Failed to read v1 log header from file channel `%s`. Expected to read %d bytes, "
                                        + "but reached end of file after reading %d bytes. Started read from position %d.",
                                channel, V1_RECORD_BATCH_HEADER_SIZE, size, position));
            }
        }
    }

    @VisibleForTesting
    ByteBuffer getLogHeaderBuffer() {
        return logHeaderBuffer;
    }

    private ProjectionInfo getOrCreateProjectionInfo(
            short schemaId, @javax.annotation.Nullable int[] storedTargetColumns) {
        ProjectionInfo cachedProjection =
                projectionsCache.getProjectionInfo(
                        tableId,
                        schemaId,
                        storedTargetColumns,
                        requestedColumnIds,
                        requestedOutputSignature);
        if (cachedProjection == null) {
            cachedProjection =
                    createProjectionInfo(
                            schemaId,
                            storedTargetColumns,
                            requestedColumnIds,
                            requestedOutputRowType);
            projectionsCache.setProjectionInfo(
                    tableId,
                    schemaId,
                    storedTargetColumns,
                    requestedColumnIds,
                    requestedOutputSignature,
                    cachedProjection);
        }
        return cachedProjection;
    }

    private ProjectionInfo createProjectionInfo(
            short schemaId,
            @javax.annotation.Nullable int[] storedTargetColumns,
            int[] requestedColumnIds,
            RowType requestedOutputRowType) {
        org.apache.fluss.metadata.Schema schema = schemaGetter.getSchema(schemaId);
        RowType fullRowType = schema.getRowType();
        RowType storedRowType =
                storedTargetColumns == null
                        ? fullRowType
                        : fullRowType.project(storedTargetColumns);
        int[] sourcePayloadColumns =
                storedTargetColumns == null
                        ? allFieldPositions(fullRowType.getFieldCount())
                        : storedTargetColumns;

        Set<Integer> requestedColumnIdSet = new HashSet<>();
        for (int requestedColumnId : requestedColumnIds) {
            requestedColumnIdSet.add(requestedColumnId);
        }

        List<Integer> payloadPositions = new ArrayList<>();
        List<Integer> survivingTargetColumns = new ArrayList<>();
        List<Integer> sourceColumnIds = schema.getColumnIds();
        for (int payloadPosition = 0;
                payloadPosition < sourcePayloadColumns.length;
                payloadPosition++) {
            int fullFieldPosition = sourcePayloadColumns[payloadPosition];
            if (requestedColumnIdSet.contains(sourceColumnIds.get(fullFieldPosition))) {
                payloadPositions.add(payloadPosition);
                // Returned partial metadata must follow payload order, not full-schema order.
                survivingTargetColumns.add(fullFieldPosition);
            }
        }

        int[] storedSelectedFieldPositions =
                payloadPositions.stream().mapToInt(Integer::intValue).toArray();
        int[] survivingSourceColumns =
                survivingTargetColumns.stream().mapToInt(Integer::intValue).toArray();
        int[] responseTargetColumns =
                storedTargetColumns != null
                                || !matchesRequestedOutput(
                                        schema,
                                        survivingSourceColumns,
                                        requestedColumnIds,
                                        requestedOutputRowType)
                        ? survivingSourceColumns
                        : null;

        // initialize the projection util information
        Schema arrowSchema = ArrowUtils.toArrowSchema(storedRowType);
        BitSet selection = toBitSet(arrowSchema.getFields().size(), storedSelectedFieldPositions);
        List<Tuple2<Field, Boolean>> flattenedFields = new ArrayList<>();
        flattenFields(arrowSchema.getFields(), selection, flattenedFields);
        int totalFieldNodes = flattenedFields.size();
        int[] bufferLayoutCount = new int[totalFieldNodes];
        BitSet nodesProjection = new BitSet(totalFieldNodes);
        int totalBuffers = 0;
        for (int i = 0; i < totalFieldNodes; i++) {
            Field fieldNode = flattenedFields.get(i).f0;
            boolean selected = flattenedFields.get(i).f1;
            nodesProjection.set(i, selected);
            bufferLayoutCount[i] = TypeLayout.getTypeBufferCount(fieldNode.getType());
            totalBuffers += bufferLayoutCount[i];
        }
        BitSet buffersProjection = new BitSet(totalBuffers);
        int bufferIndex = 0;
        for (int i = 0; i < totalFieldNodes; i++) {
            if (nodesProjection.get(i)) {
                buffersProjection.set(bufferIndex, bufferIndex + bufferLayoutCount[i]);
            }
            bufferIndex += bufferLayoutCount[i];
        }

        ArrowBodyCompression bodyCompression =
                CompressionUtil.createBodyCompression(compressionInfo.createCompressionCodec());
        return new ProjectionInfo(
                nodesProjection,
                buffersProjection,
                bufferIndex,
                bodyCompression,
                responseTargetColumns);
    }

    private static int[] allFieldPositions(int fieldCount) {
        int[] fieldPositions = new int[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            fieldPositions[i] = i;
        }
        return fieldPositions;
    }

    private static boolean matchesRequestedOutput(
            org.apache.fluss.metadata.Schema sourceSchema,
            int[] survivingSourceColumns,
            int[] requestedColumnIds,
            RowType requestedOutputRowType) {
        if (survivingSourceColumns.length != requestedColumnIds.length) {
            return false;
        }
        List<org.apache.fluss.metadata.Schema.Column> sourceColumns = sourceSchema.getColumns();
        for (int i = 0; i < survivingSourceColumns.length; i++) {
            org.apache.fluss.metadata.Schema.Column sourceColumn =
                    sourceColumns.get(survivingSourceColumns[i]);
            if (sourceColumn.getColumnId() != requestedColumnIds[i]) {
                return false;
            }
            if (!sourceColumn
                    .getDataType()
                    .copy(true)
                    .equals(requestedOutputRowType.getTypeAt(i).copy(true))) {
                return false;
            }
        }
        return true;
    }

    /** Projection pushdown information for a specific schema and selected fields. */
    public static final class ProjectionInfo {
        final BitSet nodesProjection;
        final BitSet buffersProjection;
        final int bufferCount;
        final ArrowBodyCompression bodyCompression;
        @javax.annotation.Nullable final int[] responseTargetColumns;

        private ProjectionInfo(
                BitSet nodesProjection,
                BitSet buffersProjection,
                int bufferCount,
                ArrowBodyCompression bodyCompression,
                @javax.annotation.Nullable int[] responseTargetColumns) {
            this.nodesProjection = nodesProjection;
            this.buffersProjection = buffersProjection;
            this.bufferCount = bufferCount;
            this.bodyCompression = bodyCompression;
            this.responseTargetColumns =
                    responseTargetColumns == null
                            ? null
                            : Arrays.copyOf(responseTargetColumns, responseTargetColumns.length);
        }
    }

    /** Metadata of a projected arrow record batch. */
    static final class ProjectedArrowBatch {
        /** Number of records. */
        final long numRecords;

        /** The projected nodes of {@link ArrowRecordBatch#getNodes()}. */
        final List<ArrowFieldNode> nodes;

        /** The new buffer layouts of the {@link #buffers}. */
        final List<ArrowBuffer> buffersLayout;

        /** The projected buffer positions of {@link ArrowRecordBatch#getBuffers()}. */
        final List<ArrowBuffer> buffers;

        public ProjectedArrowBatch(
                long numRecords,
                List<ArrowFieldNode> nodes,
                List<ArrowBuffer> buffersLayout,
                List<ArrowBuffer> buffers) {
            this.numRecords = numRecords;
            this.nodes = nodes;
            this.buffersLayout = buffersLayout;
            this.buffers = buffers;
        }

        public long bodyLength() {
            long bodyLength = 0;
            for (ArrowBuffer buffer : buffers) {
                bodyLength += buffer.getSize();
            }
            return bodyLength;
        }
    }
}
