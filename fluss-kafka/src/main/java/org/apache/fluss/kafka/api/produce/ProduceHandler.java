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

package org.apache.fluss.kafka.api.produce;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.kafka.KafkaRequestContext;
import org.apache.fluss.kafka.backend.produce.KafkaProduceBackend;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.PartitionWrite;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.TopicWrite;
import org.apache.fluss.kafka.backend.produce.KafkaProduceResult;
import org.apache.fluss.kafka.backend.produce.KafkaProduceResult.PartitionResult;
import org.apache.fluss.kafka.backend.produce.KafkaProduceResult.TopicResult;
import org.apache.fluss.kafka.dispatcher.KafkaApiHandler;
import org.apache.fluss.kafka.dispatcher.KafkaApiSpec;
import org.apache.fluss.kafka.metrics.KafkaProduceMetrics;
import org.apache.fluss.kafka.network.KafkaFrameAdmissionLease;

import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.InvalidRequiredAcksException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.message.ProduceRequestData.PartitionProduceData;
import org.apache.kafka.common.message.ProduceRequestData.TopicProduceData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.record.BaseRecords;
import org.apache.kafka.common.record.DefaultRecordBatch;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.common.utils.CloseableIterator;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Implements non-idempotent Kafka Produce versions 3 through 11. */
@Internal
public final class ProduceHandler implements KafkaApiHandler<ProduceRequest> {

    private static final short MIN_SUPPORTED_VERSION = 3;
    private static final long MAX_DECOMPRESSION_SCRATCH_BYTES = 256L * 1024;
    private static final KafkaApiSpec API_SPEC =
            new KafkaApiSpec(
                    ApiKeys.PRODUCE, MIN_SUPPORTED_VERSION, ApiKeys.PRODUCE.latestVersion(), true);

    private final KafkaProduceBackend backend;
    private final KafkaProduceMetrics produceMetrics;
    private final long maxCopiedBytesPerRequest;
    private final long maxCopiedBytesPerRecord;

    /** Creates a non-idempotent Produce handler. */
    public ProduceHandler(KafkaProduceBackend backend) {
        this(backend, KafkaProduceMetrics.noOp());
    }

    /** Creates a non-idempotent Produce handler with runtime metrics. */
    public ProduceHandler(KafkaProduceBackend backend, KafkaProduceMetrics produceMetrics) {
        this(backend, produceMetrics, Long.MAX_VALUE, Long.MAX_VALUE);
    }

    /** Creates a handler with hard decompressed/copied-memory limits. */
    public ProduceHandler(
            KafkaProduceBackend backend,
            KafkaProduceMetrics produceMetrics,
            long maxCopiedBytesPerRequest,
            long maxCopiedBytesPerRecord) {
        checkArgument(maxCopiedBytesPerRequest > 0, "maxCopiedBytesPerRequest must be positive");
        checkArgument(maxCopiedBytesPerRecord > 0, "maxCopiedBytesPerRecord must be positive");
        checkArgument(
                maxCopiedBytesPerRecord <= maxCopiedBytesPerRequest,
                "maxCopiedBytesPerRecord must not exceed maxCopiedBytesPerRequest");
        this.backend = checkNotNull(backend);
        this.produceMetrics = checkNotNull(produceMetrics);
        this.maxCopiedBytesPerRequest = maxCopiedBytesPerRequest;
        this.maxCopiedBytesPerRecord = maxCopiedBytesPerRecord;
    }

    @Override
    public KafkaApiSpec apiSpec() {
        return API_SPEC;
    }

    @Override
    public CompletableFuture<? extends AbstractResponse> handle(
            KafkaRequestContext context, ProduceRequest request) {
        long copyStartedNanos = metricsNowNanos();
        CopyStats copyStats = new CopyStats();
        CopyBudget copyBudget = new CopyBudget(maxCopiedBytesPerRequest, maxCopiedBytesPerRecord);
        RawAdmissionBudget rawAdmission = new RawAdmissionBudget(context);
        try {
            validateRequest(request);
            List<TopicWrite> topics = new ArrayList<>();
            for (TopicProduceData topic : request.data().topicData()) {
                if (!Topic.isValid(topic.name())) {
                    throw new InvalidTopicException("Invalid Kafka topic name " + topic.name());
                }
                rawAdmission.grow(copyBudget.reserveTopic(topic.name()));
                List<PartitionWrite> partitions = new ArrayList<>();
                for (PartitionProduceData partition : topic.partitionData()) {
                    rawAdmission.grow(copyBudget.reservePartition());
                    copyStats.partitionCount++;
                    partitions.add(
                            new PartitionWrite(
                                    partition.index(),
                                    copyRecords(
                                            request.version(),
                                            partition.records(),
                                            copyStats,
                                            copyBudget,
                                            rawAdmission)));
                }
                topics.add(new TopicWrite(topic.name(), partitions));
            }
            KafkaProduceCommand command =
                    new KafkaProduceCommand(
                            request.acks(),
                            request.timeout(),
                            topics,
                            context.listenerName(),
                            clientAddress(context.remoteAddress()),
                            context.principal(),
                            context.nativeAdmissionConnection(),
                            context.admissionScheduler(),
                            context.requestBytes());
            context.registerNativeAdmissionTransfer(command.nativeAdmissionTransferFuture());
            try {
                CompletableFuture<KafkaProduceResult> backendFuture = backend.write(command);
                backendFuture.whenComplete(
                        (ignored, failure) -> command.completeNativeAdmissionTransfer());
                return backendFuture.thenApply(ProduceHandler::toResponse);
            } catch (Throwable failure) {
                command.completeNativeAdmissionTransfer();
                throw failure;
            }
        } catch (Throwable failure) {
            rawAdmission.rollback();
            throw failure;
        } finally {
            recordMetric(
                    () ->
                            produceMetrics.recordRecordCopy(
                                    copyStartedNanos,
                                    copyStats.recordCount,
                                    copyStats.partitionCount));
        }
    }

    private long metricsNowNanos() {
        try {
            return produceMetrics.nowNanos();
        } catch (Throwable ignored) {
            // Metrics are observational and must not affect copied-memory ownership.
            return System.nanoTime();
        }
    }

    private void recordMetric(Runnable recorder) {
        try {
            recorder.run();
        } catch (Throwable ignored) {
            // Metrics are observational and must not affect copied-memory ownership.
        }
    }

    private static void validateRequest(ProduceRequest request) {
        if (request.transactionalId() != null) {
            throw new InvalidRequestException(
                    "Transactional Produce is not supported by the Fluss Kafka compatibility layer.");
        }
        if (request.acks() != -1 && request.acks() != 0 && request.acks() != 1) {
            throw new InvalidRequiredAcksException("Invalid required acks " + request.acks());
        }
    }

    private static List<KafkaProduceCommand.Record> copyRecords(
            short version,
            BaseRecords baseRecords,
            CopyStats copyStats,
            CopyBudget copyBudget,
            RawAdmissionBudget rawAdmission) {
        if (!(baseRecords instanceof Records)) {
            throw new InvalidRequestException("Unsupported Kafka records representation.");
        }
        ProduceRequest.validateRecords(version, baseRecords);
        Records records = (Records) baseRecords;
        List<KafkaProduceCommand.Record> copied = new ArrayList<>();
        for (RecordBatch batch : records.batches()) {
            batch.ensureValid();
            if (batch.hasProducerId() || batch.isTransactional() || batch.isControlBatch()) {
                throw new InvalidRequestException(
                        "Idempotent, transactional, and control record batches are not supported.");
            }
            if (!(batch instanceof DefaultRecordBatch)) {
                throw new InvalidRequestException(
                        "Unsupported Kafka magic-v2 record batch implementation "
                                + batch.getClass().getName());
            }
            DefaultRecordBatch defaultBatch = (DefaultRecordBatch) batch;
            BatchCopyEstimate batchEstimate =
                    preflightBatch(defaultBatch, copyBudget, rawAdmission);
            rawAdmission.grow(batchEstimate.reservedBytes());
            try (BoundedBufferSupplier bufferSupplier =
                            new BoundedBufferSupplier(copyBudget.maxScratchBytes(), rawAdmission);
                    CloseableIterator<org.apache.kafka.common.record.Record> iterator =
                            defaultBatch.streamingIterator(bufferSupplier)) {
                while (iterator.hasNext()) {
                    org.apache.kafka.common.record.Record record = iterator.next();
                    record.ensureValid();
                    copyStats.recordCount++;
                    copied.add(
                            KafkaProduceCommand.Record.copyOfKafkaRecord(
                                    record, defaultBatch.isCompressed()));
                }
            } catch (KafkaException failure) {
                throw unwrapCopyLimit(failure);
            } finally {
                rawAdmission.release(batchEstimate.transientBytes);
            }
        }
        return copied;
    }

    static List<KafkaProduceCommand.Record> copyRecordsForTesting(
            short version,
            BaseRecords baseRecords,
            long maxCopiedBytesPerRequest,
            long maxCopiedBytesPerRecord) {
        return copyRecords(
                version,
                baseRecords,
                new CopyStats(),
                new CopyBudget(maxCopiedBytesPerRequest, maxCopiedBytesPerRecord),
                RawAdmissionBudget.noOp());
    }

    static List<KafkaProduceCommand.Record> copyRecordsForTesting(
            short version,
            BaseRecords baseRecords,
            long maxCopiedBytesPerRequest,
            long maxCopiedBytesPerRecord,
            KafkaFrameAdmissionLease admissionLease) {
        RawAdmissionBudget rawAdmission = new RawAdmissionBudget(admissionLease);
        try {
            return copyRecords(
                    version,
                    baseRecords,
                    new CopyStats(),
                    new CopyBudget(maxCopiedBytesPerRequest, maxCopiedBytesPerRecord),
                    rawAdmission);
        } catch (Throwable failure) {
            rawAdmission.rollback();
            throw failure;
        }
    }

    private static BatchCopyEstimate preflightBatch(
            DefaultRecordBatch batch, CopyBudget copyBudget, RawAdmissionBudget rawAdmission) {
        Integer recordCount = batch.countOrNull();
        if (recordCount == null || recordCount < 0) {
            throw new InvalidRecordException("Kafka record batch has an invalid record count.");
        }
        long copiedBytesBefore = copyBudget.copiedBytes();
        long maxRecordBytes = 0;
        try (BoundedBufferSupplier bufferSupplier =
                        new BoundedBufferSupplier(copyBudget.maxScratchBytes(), rawAdmission);
                CountingInputStream input =
                        new CountingInputStream(batch.recordInputStream(bufferSupplier))) {
            for (int index = 0; index < recordCount; index++) {
                maxRecordBytes = Math.max(maxRecordBytes, preflightRecord(input, copyBudget));
            }
            if (input.read() != -1) {
                throw new InvalidRecordException(
                        "Kafka record batch contains data after its declared record count.");
            }
            long copiedBytes = copyBudget.copiedBytes() - copiedBytesBefore;
            return new BatchCopyEstimate(copiedBytes, maxRecordBytes);
        } catch (IOException e) {
            throw new InvalidRecordException("Failed to inspect Kafka record batch.", e);
        } catch (KafkaException failure) {
            throw unwrapCopyLimit(failure);
        }
    }

    private static RuntimeException unwrapCopyLimit(KafkaException failure) {
        Throwable cause = failure;
        while (cause != null && cause != cause.getCause()) {
            if (cause instanceof RecordTooLargeException) {
                return (RecordTooLargeException) cause;
            }
            cause = cause.getCause();
        }
        return failure;
    }

    private static long preflightRecord(CountingInputStream input, CopyBudget copyBudget)
            throws IOException {
        long recordStart = input.count();
        int recordBodyBytes = ByteUtils.readVarint(input);
        if (recordBodyBytes < 0) {
            throw new InvalidRecordException(
                    "Kafka record declares a negative body size " + recordBodyBytes + ".");
        }
        long decompressedBytes =
                saturatedAdd(ByteUtils.sizeOfVarint(recordBodyBytes), recordBodyBytes);
        copyBudget.validateDecompressedRecord(decompressedBytes);

        long bodyStart = input.count();
        readRequiredByte(input, "record attributes");
        ByteUtils.readVarlong(input);
        ByteUtils.readVarint(input);

        int keySize = readNullableSize(input, "record key");
        long copiedBytes = KafkaProduceCommand.Record.estimateBaseCopiedBytes(keySize, 0);
        copyBudget.validateCopiedRecord(copiedBytes);
        skipFully(input, keySize);

        int valueSize = readNullableSize(input, "record value");
        copiedBytes = saturatedAdd(copiedBytes, Math.max(valueSize, 0));
        copyBudget.validateCopiedRecord(copiedBytes);
        skipFully(input, valueSize);

        int headerCount = ByteUtils.readVarint(input);
        if (headerCount < 0) {
            throw new InvalidRecordException(
                    "Kafka record declares a negative header count " + headerCount + ".");
        }
        copiedBytes =
                saturatedAdd(
                        copiedBytes,
                        saturatedMultiply(
                                headerCount,
                                KafkaProduceCommand.RecordHeader.estimateCopiedBytes(0, 0)));
        copyBudget.validateCopiedRecord(copiedBytes);
        for (int index = 0; index < headerCount; index++) {
            int nameSize = ByteUtils.readVarint(input);
            if (nameSize < 0) {
                throw new InvalidRecordException(
                        "Kafka record declares a negative header name size " + nameSize + ".");
            }
            long headerVariableBytes =
                    KafkaProduceCommand.RecordHeader.estimateCopiedBytes(nameSize, 0)
                            - KafkaProduceCommand.RecordHeader.estimateCopiedBytes(0, 0);
            copiedBytes = saturatedAdd(copiedBytes, headerVariableBytes);
            copyBudget.validateCopiedRecord(copiedBytes);
            skipFully(input, nameSize);

            int headerValueSize = readNullableSize(input, "header value");
            copiedBytes = saturatedAdd(copiedBytes, Math.max(headerValueSize, 0));
            copyBudget.validateCopiedRecord(copiedBytes);
            skipFully(input, headerValueSize);
        }

        long consumedBodyBytes = input.count() - bodyStart;
        if (consumedBodyBytes != recordBodyBytes) {
            throw new InvalidRecordException(
                    "Kafka record declares "
                            + recordBodyBytes
                            + " body bytes but contains "
                            + consumedBodyBytes
                            + ".");
        }
        long consumedRecordBytes = input.count() - recordStart;
        if (consumedRecordBytes != decompressedBytes) {
            throw new InvalidRecordException("Kafka record size changed while it was inspected.");
        }
        copyBudget.reserveRecord(decompressedBytes, copiedBytes);
        return decompressedBytes;
    }

    private static int readNullableSize(InputStream input, String field) throws IOException {
        int size = ByteUtils.readVarint(input);
        if (size < -1) {
            throw new InvalidRecordException(
                    "Kafka " + field + " declares an invalid size " + size + ".");
        }
        return size;
    }

    private static void readRequiredByte(InputStream input, String field) throws IOException {
        if (input.read() < 0) {
            throw new InvalidRecordException("Kafka " + field + " is truncated.");
        }
    }

    private static void skipFully(InputStream input, int bytes) throws IOException {
        int remaining = Math.max(bytes, 0);
        while (remaining > 0) {
            long skipped = input.skip(remaining);
            if (skipped > 0 && skipped <= remaining) {
                remaining -= (int) skipped;
            } else if (skipped == 0 && input.read() >= 0) {
                remaining--;
            } else {
                throw new InvalidRecordException("Kafka record payload is truncated.");
            }
        }
    }

    private static ProduceResponse toResponse(KafkaProduceResult result) {
        ProduceResponseData data = new ProduceResponseData().setThrottleTimeMs(0);
        for (TopicResult topic : result.topics()) {
            ProduceResponseData.TopicProduceResponse topicResponse =
                    new ProduceResponseData.TopicProduceResponse().setName(topic.topicName());
            for (PartitionResult partition : topic.partitions()) {
                topicResponse
                        .partitionResponses()
                        .add(
                                new ProduceResponseData.PartitionProduceResponse()
                                        .setIndex(partition.partitionId())
                                        .setErrorCode(partition.error().code())
                                        .setBaseOffset(partition.baseOffset())
                                        .setLogAppendTimeMs(-1L)
                                        .setLogStartOffset(-1L)
                                        .setErrorMessage(partition.errorMessage()));
            }
            data.responses().add(topicResponse);
        }
        return new ProduceResponse(data);
    }

    private static InetAddress clientAddress(SocketAddress remoteAddress) {
        if (remoteAddress instanceof InetSocketAddress) {
            return ((InetSocketAddress) remoteAddress).getAddress();
        }
        return null;
    }

    private static final class CopyStats {
        private int recordCount;
        private int partitionCount;
    }

    private static final class BatchCopyEstimate {
        private final long copiedBytes;
        private final long transientBytes;

        private BatchCopyEstimate(long copiedBytes, long transientBytes) {
            this.copiedBytes = copiedBytes;
            this.transientBytes = transientBytes;
        }

        private long reservedBytes() {
            return saturatedAdd(copiedBytes, transientBytes);
        }
    }

    private static final class CopyBudget {
        private final long maxBytesPerRequest;
        private final long maxBytesPerRecord;
        private long decompressedBytes;
        private long copiedBytes;

        private CopyBudget(long maxBytesPerRequest, long maxBytesPerRecord) {
            checkArgument(maxBytesPerRequest > 0, "maxBytesPerRequest must be positive");
            checkArgument(maxBytesPerRecord > 0, "maxBytesPerRecord must be positive");
            checkArgument(
                    maxBytesPerRecord <= maxBytesPerRequest,
                    "maxBytesPerRecord must not exceed maxBytesPerRequest");
            this.maxBytesPerRequest = maxBytesPerRequest;
            this.maxBytesPerRecord = maxBytesPerRecord;
        }

        private long maxScratchBytes() {
            return Math.min(maxBytesPerRequest, MAX_DECOMPRESSION_SCRATCH_BYTES);
        }

        private long copiedBytes() {
            return copiedBytes;
        }

        private long reserveTopic(String topicName) {
            long topicBytes = TopicWrite.estimateMetadataBytes(topicName);
            reserveCopiedContainer(topicBytes, "topic metadata");
            return topicBytes;
        }

        private long reservePartition() {
            long partitionBytes = PartitionWrite.estimateMetadataBytes();
            reserveCopiedContainer(partitionBytes, "partition metadata");
            return partitionBytes;
        }

        private void reserveCopiedContainer(long bytes, String kind) {
            validateRequest(copiedBytes, bytes, kind);
            copiedBytes = saturatedAdd(copiedBytes, bytes);
        }

        private void validateDecompressedRecord(long bytes) {
            validateRecord(bytes, "decompressed");
            validateRequest(decompressedBytes, bytes, "decompressed");
        }

        private void validateCopiedRecord(long bytes) {
            validateRecord(bytes, "copied");
            validateRequest(copiedBytes, bytes, "copied");
        }

        private void reserveRecord(long decompressedRecordBytes, long copiedRecordBytes) {
            validateDecompressedRecord(decompressedRecordBytes);
            validateCopiedRecord(copiedRecordBytes);
            decompressedBytes = saturatedAdd(decompressedBytes, decompressedRecordBytes);
            copiedBytes = saturatedAdd(copiedBytes, copiedRecordBytes);
        }

        private void validateRecord(long bytes, String kind) {
            if (bytes > maxBytesPerRecord) {
                throw tooLarge(kind + " record", bytes, maxBytesPerRecord);
            }
        }

        private void validateRequest(long currentBytes, long additionalBytes, String kind) {
            if (additionalBytes > maxBytesPerRequest - currentBytes) {
                throw tooLarge(
                        kind + " request",
                        saturatedAdd(currentBytes, additionalBytes),
                        maxBytesPerRequest);
            }
        }

        private static RecordTooLargeException tooLarge(String kind, long bytes, long limit) {
            return new RecordTooLargeException(
                    "Kafka Produce "
                            + kind
                            + " requires at least "
                            + bytes
                            + " bytes but the copy limit is "
                            + limit
                            + " bytes.");
        }
    }

    private static final class RawAdmissionBudget {
        private final KafkaRequestContext context;
        private final KafkaFrameAdmissionLease admissionLease;
        private long grownBytes;

        private RawAdmissionBudget(KafkaRequestContext context) {
            this.context = context;
            this.admissionLease = null;
        }

        private RawAdmissionBudget(KafkaFrameAdmissionLease admissionLease) {
            this.context = null;
            this.admissionLease = checkNotNull(admissionLease);
        }

        private static RawAdmissionBudget noOp() {
            return new RawAdmissionBudget((KafkaRequestContext) null);
        }

        private void grow(long additionalBytes) {
            if (additionalBytes == 0) {
                return;
            }
            try {
                if (context != null) {
                    context.growRawAdmissionBytes(additionalBytes);
                } else if (admissionLease != null) {
                    admissionLease.growFrameBytes(additionalBytes);
                }
                grownBytes = saturatedAdd(grownBytes, additionalBytes);
            } catch (IllegalArgumentException failure) {
                throw new RecordTooLargeException(
                        "Kafka Produce copied/decompressed payload exceeds a PF raw-byte limit.",
                        failure);
            } catch (RejectedExecutionException | IllegalStateException failure) {
                throw new TimeoutException(
                        "Kafka Produce copied/decompressed payload admission is unavailable.",
                        failure);
            }
        }

        private void release(long bytes) {
            if (bytes == 0) {
                return;
            }
            checkArgument(bytes <= grownBytes, "Cannot release more raw bytes than were grown");
            if (context != null) {
                context.releaseGrownRawAdmissionBytes(bytes);
            } else if (admissionLease != null) {
                admissionLease.releaseGrownFrameBytes(bytes);
            }
            grownBytes -= bytes;
        }

        private void rollback() {
            if (context != null && grownBytes > 0) {
                context.releaseGrownRawAdmissionBytes(grownBytes);
                grownBytes = 0;
            } else if (admissionLease != null && grownBytes > 0) {
                admissionLease.releaseGrownFrameBytes(grownBytes);
                grownBytes = 0;
            }
        }
    }

    private static final class CountingInputStream extends FilterInputStream {
        private long count;

        private CountingInputStream(InputStream input) {
            super(input);
        }

        private long count() {
            return count;
        }

        @Override
        public int read() throws IOException {
            int value = super.read();
            if (value >= 0) {
                count++;
            }
            return value;
        }

        @Override
        public int read(byte[] target, int offset, int length) throws IOException {
            int read = super.read(target, offset, length);
            if (read > 0) {
                count += read;
            }
            return read;
        }

        @Override
        public long skip(long bytes) throws IOException {
            long skipped = super.skip(bytes);
            if (skipped > 0) {
                count += skipped;
            }
            return skipped;
        }
    }

    private static final class BoundedBufferSupplier extends BufferSupplier {
        private final long maxBytes;
        private final RawAdmissionBudget rawAdmission;
        private final Map<ByteBuffer, Integer> buffers = new IdentityHashMap<>();
        private long allocatedBytes;
        private long peakAllocatedBytes;

        private BoundedBufferSupplier(long maxBytes, RawAdmissionBudget rawAdmission) {
            this.maxBytes = maxBytes;
            this.rawAdmission = checkNotNull(rawAdmission);
        }

        @Override
        public ByteBuffer get(int capacity) {
            if (capacity < 0 || capacity > maxBytes - allocatedBytes) {
                throw CopyBudget.tooLarge(
                        "decompression scratch",
                        saturatedAdd(allocatedBytes, Math.max(capacity, 0)),
                        maxBytes);
            }
            rawAdmission.grow(capacity);
            ByteBuffer buffer;
            try {
                buffer = ByteBuffer.allocate(capacity);
            } catch (Throwable allocationFailure) {
                rawAdmission.release(capacity);
                throw allocationFailure;
            }
            buffers.put(buffer, capacity);
            allocatedBytes += capacity;
            peakAllocatedBytes = Math.max(peakAllocatedBytes, allocatedBytes);
            return buffer;
        }

        private long peakAllocatedBytes() {
            return peakAllocatedBytes;
        }

        @Override
        public void release(ByteBuffer buffer) {
            Integer capacity = buffers.remove(buffer);
            if (capacity != null) {
                allocatedBytes -= capacity;
                rawAdmission.release(capacity);
            }
        }

        @Override
        public void close() {
            long releasedBytes = allocatedBytes;
            buffers.clear();
            allocatedBytes = 0;
            rawAdmission.release(releasedBytes);
        }
    }

    private static long saturatedMultiply(long left, long right) {
        if (left == 0 || right == 0) {
            return 0;
        }
        return left > Long.MAX_VALUE / right ? Long.MAX_VALUE : left * right;
    }

    private static long saturatedAdd(long left, long right) {
        return left > Long.MAX_VALUE - right ? Long.MAX_VALUE : left + right;
    }
}
