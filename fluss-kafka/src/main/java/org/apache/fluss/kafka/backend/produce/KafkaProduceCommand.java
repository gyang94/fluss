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

package org.apache.fluss.kafka.backend.produce;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.kafka.admission.KafkaNativeProduceAdmissionController.ConnectionHandle;
import org.apache.fluss.security.acl.FlussPrincipal;

import org.apache.kafka.common.header.Header;

import javax.annotation.Nullable;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Protocol-independent write command used by the Kafka Produce backend. */
@Internal
public final class KafkaProduceCommand {

    private final short acks;
    private final int timeoutMs;
    private final AtomicReference<List<TopicWrite>> topics;
    private final String listenerName;
    private final @Nullable InetAddress clientAddress;
    private final FlussPrincipal principal;
    private final @Nullable ConnectionHandle nativeAdmissionConnection;
    private final @Nullable ScheduledExecutorService admissionScheduler;
    private final NativeAdmissionTransfer nativeAdmissionTransfer;
    private final ErrorMessageBudget errorMessageBudget;

    /** Creates a Kafka write command. */
    public KafkaProduceCommand(
            short acks,
            int timeoutMs,
            List<TopicWrite> topics,
            String listenerName,
            @Nullable InetAddress clientAddress) {
        this(
                acks,
                timeoutMs,
                topics,
                listenerName,
                clientAddress,
                FlussPrincipal.ANONYMOUS,
                null,
                null,
                0);
    }

    /** Creates a Kafka write command for the authenticated Kafka principal. */
    public KafkaProduceCommand(
            short acks,
            int timeoutMs,
            List<TopicWrite> topics,
            String listenerName,
            @Nullable InetAddress clientAddress,
            FlussPrincipal principal) {
        this(acks, timeoutMs, topics, listenerName, clientAddress, principal, null, null, 0);
    }

    /** Creates a Kafka write command carrying its connection-scoped native admission context. */
    public KafkaProduceCommand(
            short acks,
            int timeoutMs,
            List<TopicWrite> topics,
            String listenerName,
            @Nullable InetAddress clientAddress,
            FlussPrincipal principal,
            @Nullable ConnectionHandle nativeAdmissionConnection,
            @Nullable ScheduledExecutorService admissionScheduler) {
        this(
                acks,
                timeoutMs,
                topics,
                listenerName,
                clientAddress,
                principal,
                nativeAdmissionConnection,
                admissionScheduler,
                0);
    }

    /**
     * Creates a Kafka write command carrying its native admission context and request byte size.
     */
    public KafkaProduceCommand(
            short acks,
            int timeoutMs,
            List<TopicWrite> topics,
            String listenerName,
            @Nullable InetAddress clientAddress,
            FlussPrincipal principal,
            @Nullable ConnectionHandle nativeAdmissionConnection,
            @Nullable ScheduledExecutorService admissionScheduler,
            int requestBytes) {
        this.acks = acks;
        this.timeoutMs = timeoutMs;
        List<TopicWrite> topicWrites = immutableCopy(topics);
        this.topics = new AtomicReference<>(topicWrites);
        this.listenerName = checkNotNull(listenerName);
        this.clientAddress = clientAddress;
        this.principal = checkNotNull(principal);
        this.nativeAdmissionConnection = nativeAdmissionConnection;
        this.admissionScheduler = admissionScheduler;
        this.errorMessageBudget = new ErrorMessageBudget(requestBytes);
        this.nativeAdmissionTransfer =
                new NativeAdmissionTransfer(
                        topicWrites, () -> this.topics.set(Collections.emptyList()));
    }

    /** Returns Kafka required acknowledgements. */
    public short acks() {
        return acks;
    }

    /** Returns the Produce timeout in milliseconds. */
    public int timeoutMs() {
        return timeoutMs;
    }

    /** Returns the topic writes in request order. */
    public List<TopicWrite> topics() {
        return topics.get();
    }

    /** Returns the listener that received the request. */
    public String listenerName() {
        return listenerName;
    }

    /** Returns the client network address when available. */
    public @Nullable InetAddress clientAddress() {
        return clientAddress;
    }

    /** Returns the authenticated Kafka principal. */
    public FlussPrincipal principal() {
        return principal;
    }

    /** Returns the native Produce admission handle associated with the Kafka connection. */
    public @Nullable ConnectionHandle nativeAdmissionConnection() {
        return nativeAdmissionConnection;
    }

    /** Returns the event-loop scheduler used for admission and completion deadlines. */
    public @Nullable ScheduledExecutorService admissionScheduler() {
        return admissionScheduler;
    }

    /** Bounds and reserves one partition error message from this request's shared budget. */
    public @Nullable String limitErrorMessage(@Nullable String errorMessage) {
        return errorMessageBudget.limit(errorMessage);
    }

    int remainingErrorMessageBytes() {
        return errorMessageBudget.remainingBytes.get();
    }

    /**
     * Returns the gate that keeps PF raw bytes charged until copied payload ownership transfers.
     */
    public CompletableFuture<Void> nativeAdmissionTransferFuture() {
        return nativeAdmissionTransfer.future;
    }

    /** Marks one topic's copied payload as protected by native admission. */
    public void completeNativeAdmissionTransfer(TopicWrite topic) {
        nativeAdmissionTransfer.complete(checkNotNull(topic));
    }

    /**
     * Keeps PF raw ownership until the aggregate result when a topic obtained no native byte token.
     */
    public void requireNativeAdmissionTransferFallback() {
        nativeAdmissionTransfer.requireFallback();
    }

    /** Releases any remaining copied payload and completes the ownership-transfer fallback. */
    public void completeNativeAdmissionTransfer() {
        for (TopicWrite topic : topics.get()) {
            topic.releaseCopiedRecords();
        }
        nativeAdmissionTransfer.completeAll();
    }

    private static <T> List<T> immutableCopy(List<T> values) {
        return Collections.unmodifiableList(new ArrayList<>(checkNotNull(values)));
    }

    private static final class ErrorMessageBudget {
        private final AtomicInteger remainingBytes;

        private ErrorMessageBudget(int requestBytes) {
            int totalBytes =
                    requestBytes > 0
                            ? Math.min(
                                    requestBytes, KafkaProduceResult.MAX_TOTAL_ERROR_MESSAGE_BYTES)
                            : KafkaProduceResult.MAX_TOTAL_ERROR_MESSAGE_BYTES;
            this.remainingBytes = new AtomicInteger(totalBytes);
        }

        private @Nullable String limit(@Nullable String errorMessage) {
            String partitionLimited =
                    KafkaProduceResult.limitErrorMessage(
                            errorMessage, KafkaProduceResult.MAX_PARTITION_ERROR_MESSAGE_BYTES);
            int partitionBytes = KafkaProduceResult.errorMessageBytes(partitionLimited);
            if (partitionBytes == 0) {
                return partitionLimited;
            }
            while (true) {
                int availableBytes = remainingBytes.get();
                if (availableBytes == 0) {
                    return null;
                }
                String budgeted =
                        partitionBytes <= availableBytes
                                ? partitionLimited
                                : KafkaProduceResult.limitErrorMessage(
                                        partitionLimited, availableBytes);
                int reservedBytes = KafkaProduceResult.errorMessageBytes(budgeted);
                if (remainingBytes.compareAndSet(availableBytes, availableBytes - reservedBytes)) {
                    return budgeted;
                }
            }
        }
    }

    /** Records addressed to one Kafka topic. */
    @Internal
    public static final class TopicWrite {
        // Intentionally includes the TopicWrite object, immutable/container wrappers, identity
        // map entries, atomic payload holder, future-chain envelope, and object/array alignment.
        private static final long TOPIC_FIXED_ESTIMATE_BYTES = 1024;

        private final String topicName;
        private final List<PartitionWrite> partitions;
        private final Map<PartitionWrite, Integer> partitionIndexes;
        private final AtomicReference<TopicPayload> payload;
        private final long estimatedConvertedBytes;

        /** Creates the writes for one topic. */
        public TopicWrite(String topicName, List<PartitionWrite> partitions) {
            this.topicName = checkNotNull(topicName);
            this.partitions = immutableCopy(partitions);
            this.partitionIndexes = new IdentityHashMap<>();
            List<List<Record>> partitionRecords = new ArrayList<>(this.partitions.size());
            long estimatedBytes = estimateMetadataBytes(topicName);
            for (int index = 0; index < this.partitions.size(); index++) {
                PartitionWrite partition = this.partitions.get(index);
                partitionIndexes.put(partition, index);
                partitionRecords.add(partition.takeCopiedRecords());
                estimatedBytes = saturatedAdd(estimatedBytes, partition.estimatedConvertedBytes());
            }
            this.payload = new AtomicReference<>(new TopicPayload(partitionRecords));
            this.estimatedConvertedBytes = estimatedBytes;
        }

        /** Returns the Kafka topic name. */
        public String topicName() {
            return topicName;
        }

        /** Returns partition writes in request order. */
        public List<PartitionWrite> partitions() {
            return partitions;
        }

        /**
         * Returns the estimated native bytes, calculated while the copied command is constructed.
         */
        public long estimatedConvertedBytes() {
            return estimatedConvertedBytes;
        }

        /** Returns a conservative copied/native estimate for topic-level metadata. */
        public static long estimateMetadataBytes(String topicName) {
            return saturatedAdd(
                    TOPIC_FIXED_ESTIMATE_BYTES,
                    saturatedMultiply(checkNotNull(topicName).length(), 3L));
        }

        /** Returns one partition's copied records while this topic still owns its payload. */
        public List<Record> copiedRecords(PartitionWrite partition) {
            TopicPayload currentPayload = payload.get();
            Integer index = partitionIndexes.get(checkNotNull(partition));
            if (currentPayload == null || index == null) {
                return Collections.emptyList();
            }
            return currentPayload.records(index);
        }

        /** Releases one partition's copied records after conversion reaches a terminal state. */
        public void releaseCopiedRecords(PartitionWrite partition) {
            TopicPayload currentPayload = payload.get();
            Integer index = partitionIndexes.get(checkNotNull(partition));
            if (currentPayload != null && index != null) {
                currentPayload.release(index);
            }
        }

        /** Releases every copied record owned by this topic. */
        public void releaseCopiedRecords() {
            payload.set(null);
        }
    }

    /** Records addressed to one Kafka partition. */
    @Internal
    public static final class PartitionWrite {
        // Intentionally includes the PartitionWrite object, list/array wrappers, atomic holders,
        // request/response bucket descriptors, and object/array alignment.
        private static final long PARTITION_FIXED_ESTIMATE_BYTES = 512;

        private final int partitionId;
        private final AtomicReference<List<Record>> records;
        private final long estimatedCopiedRecordBytes;
        private final long estimatedConvertedBytes;

        /** Creates the writes for one partition. */
        public PartitionWrite(int partitionId, List<Record> records) {
            this.partitionId = partitionId;
            List<Record> copiedRecords = immutableCopy(records);
            this.records = new AtomicReference<>(copiedRecords);
            long copiedRecordBytes = 0;
            for (Record record : copiedRecords) {
                copiedRecordBytes =
                        saturatedAdd(copiedRecordBytes, record.estimatedConvertedBytes());
            }
            this.estimatedCopiedRecordBytes = copiedRecordBytes;
            this.estimatedConvertedBytes = saturatedAdd(estimateMetadataBytes(), copiedRecordBytes);
        }

        /** Returns the Kafka partition ID. */
        public int partitionId() {
            return partitionId;
        }

        /** Returns the estimated bytes occupied by this partition's copied payload. */
        public long estimatedConvertedBytes() {
            return estimatedConvertedBytes;
        }

        /** Returns the copied record bytes that become unreachable after conversion. */
        public long estimatedCopiedRecordBytes() {
            return estimatedCopiedRecordBytes;
        }

        /** Returns a conservative copied/native estimate for partition-level metadata. */
        public static long estimateMetadataBytes() {
            return PARTITION_FIXED_ESTIMATE_BYTES;
        }

        private List<Record> takeCopiedRecords() {
            return records.getAndSet(Collections.emptyList());
        }
    }

    private static final class TopicPayload {
        private final AtomicReferenceArray<List<Record>> partitionRecords;

        private TopicPayload(List<List<Record>> partitionRecords) {
            this.partitionRecords = new AtomicReferenceArray<>(partitionRecords.size());
            for (int index = 0; index < partitionRecords.size(); index++) {
                this.partitionRecords.set(index, partitionRecords.get(index));
            }
        }

        private List<Record> records(int index) {
            List<Record> records = partitionRecords.get(index);
            return records == null ? Collections.emptyList() : records;
        }

        private void release(int index) {
            partitionRecords.set(index, null);
        }
    }

    /** A copied Kafka record whose lifetime is independent of the network request buffer. */
    @Internal
    public static final class Record {
        // These estimates deliberately exceed the shallow Java objects. They also cover backing
        // array headers, immutable-list wrappers, references, and ordinary object alignment so a
        // request containing many empty records or headers still consumes meaningful byte budget.
        private static final long RECORD_FIXED_ESTIMATE_BYTES = 256;
        private static final long HEADER_FIXED_ESTIMATE_BYTES = 192;

        private final long timestamp;
        private final @Nullable byte[] key;
        private final @Nullable byte[] value;
        private final List<RecordHeader> headers;
        private final long estimatedConvertedBytes;

        /** Creates a copied Kafka record. */
        public Record(
                long timestamp,
                @Nullable byte[] key,
                @Nullable byte[] value,
                List<RecordHeader> headers) {
            this(timestamp, copyNullable(key), copyNullable(value), headers, false);
        }

        private Record(
                long timestamp,
                @Nullable byte[] key,
                @Nullable byte[] value,
                List<RecordHeader> headers,
                boolean owned) {
            this.timestamp = timestamp;
            this.key = key;
            this.value = value;
            this.headers = immutableCopy(headers);
            long estimatedBytes = RECORD_FIXED_ESTIMATE_BYTES;
            estimatedBytes = saturatedAdd(estimatedBytes, length(this.key));
            estimatedBytes = saturatedAdd(estimatedBytes, length(this.value));
            for (RecordHeader header : this.headers) {
                estimatedBytes = saturatedAdd(estimatedBytes, HEADER_FIXED_ESTIMATE_BYTES);
                // Three UTF-8 bytes per UTF-16 code unit is a cheap upper estimate that avoids
                // allocating another encoded copy of every header name.
                estimatedBytes =
                        saturatedAdd(estimatedBytes, saturatedMultiply(header.name.length(), 3L));
                estimatedBytes = saturatedAdd(estimatedBytes, length(header.value));
            }
            this.estimatedConvertedBytes = estimatedBytes;
        }

        /**
         * Copies one Kafka record, taking ownership of decompressed arrays when they are detached
         * from the request buffer.
         *
         * <p>The ownership flag must only be used for records returned by a compressed batch's
         * streaming iterator. Uncompressed records may still reference the network request buffer
         * and are always copied.
         */
        public static Record copyOfKafkaRecord(
                org.apache.kafka.common.record.Record record,
                boolean takeDecompressedArrayOwnership) {
            checkNotNull(record);
            List<RecordHeader> copiedHeaders = new ArrayList<>(record.headers().length);
            for (Header header : record.headers()) {
                // DefaultRecordBatch materializes every header value into a detached byte array.
                // It is safe to take that array even when key/value buffers still reference an
                // uncompressed network request.
                copiedHeaders.add(RecordHeader.takeKafkaHeader(header));
            }
            return new Record(
                    record.timestamp(),
                    copyBuffer(
                            record.hasKey() ? record.key() : null, takeDecompressedArrayOwnership),
                    copyBuffer(
                            record.hasValue() ? record.value() : null,
                            takeDecompressedArrayOwnership),
                    copiedHeaders,
                    true);
        }

        /** Returns the conservative copied-memory estimate before headers are inspected. */
        public static long estimateBaseCopiedBytes(int keySize, int valueSize) {
            long estimatedBytes = RECORD_FIXED_ESTIMATE_BYTES;
            estimatedBytes = saturatedAdd(estimatedBytes, Math.max(keySize, 0));
            return saturatedAdd(estimatedBytes, Math.max(valueSize, 0));
        }

        /** Returns the Kafka record timestamp. */
        public long timestamp() {
            return timestamp;
        }

        /**
         * Borrows the nullable Kafka record key.
         *
         * <p>The returned array is owned by this command and must not be modified or retained after
         * conversion. It exists to avoid another full-payload clone on the conversion hot path.
         */
        public @Nullable byte[] borrowedKey() {
            return key;
        }

        /**
         * Borrows the nullable Kafka record value.
         *
         * <p>The returned array is owned by this command and must not be modified or retained after
         * conversion. It exists to avoid another full-payload clone on the conversion hot path.
         */
        public @Nullable byte[] borrowedValue() {
            return value;
        }

        /** Returns the copied Kafka headers in record order. */
        public List<RecordHeader> headers() {
            return headers;
        }

        private long estimatedConvertedBytes() {
            return estimatedConvertedBytes;
        }

        private static @Nullable byte[] copyNullable(@Nullable byte[] value) {
            return value == null ? null : value.clone();
        }

        private static @Nullable byte[] copyBuffer(
                @Nullable ByteBuffer source, boolean takeOwnership) {
            if (source == null) {
                return null;
            }
            ByteBuffer duplicate = source.duplicate();
            if (takeOwnership
                    && duplicate.hasArray()
                    && !duplicate.isReadOnly()
                    && duplicate.arrayOffset() + duplicate.position() == 0
                    && duplicate.remaining() == duplicate.array().length) {
                return duplicate.array();
            }
            byte[] copied = new byte[duplicate.remaining()];
            duplicate.get(copied);
            return copied;
        }
    }

    /** A copied Kafka record header. */
    @Internal
    public static final class RecordHeader {
        private final String name;
        private final @Nullable byte[] value;

        /** Creates a copied Kafka record header. */
        public RecordHeader(String name, @Nullable byte[] value) {
            this(name, value == null ? null : value.clone(), true);
        }

        private RecordHeader(String name, @Nullable byte[] value, boolean owned) {
            this.name = checkNotNull(name);
            this.value = value;
        }

        private static RecordHeader takeKafkaHeader(Header header) {
            checkNotNull(header);
            return new RecordHeader(header.key(), header.value(), true);
        }

        /** Returns a conservative copied-memory estimate for one encoded Kafka header. */
        public static long estimateCopiedBytes(int encodedNameBytes, int valueSize) {
            long estimatedBytes = Record.HEADER_FIXED_ESTIMATE_BYTES;
            estimatedBytes =
                    saturatedAdd(
                            estimatedBytes, saturatedMultiply(Math.max(encodedNameBytes, 0), 3L));
            return saturatedAdd(estimatedBytes, Math.max(valueSize, 0));
        }

        /** Returns the header name. */
        public String name() {
            return name;
        }

        /**
         * Borrows the nullable header value owned by this command.
         *
         * <p>The returned array must not be modified or retained after conversion.
         */
        public @Nullable byte[] borrowedValue() {
            return value;
        }
    }

    private static long length(@Nullable byte[] value) {
        return value == null ? 0 : value.length;
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

    private static final class NativeAdmissionTransfer {
        private final CompletableFuture<Void> future = new CompletableFuture<>();
        private final Map<TopicWrite, Boolean> pendingTopics = new IdentityHashMap<>();
        private final Runnable completionAction;
        private boolean fallbackRequired;
        private boolean completed;

        private NativeAdmissionTransfer(List<TopicWrite> topics, Runnable completionAction) {
            this.completionAction = checkNotNull(completionAction);
            for (TopicWrite topic : topics) {
                pendingTopics.put(topic, Boolean.TRUE);
            }
            if (pendingTopics.isEmpty()) {
                completed = true;
                completeFuture();
            }
        }

        private void complete(TopicWrite topic) {
            boolean completeFuture;
            synchronized (pendingTopics) {
                pendingTopics.remove(topic);
                completeFuture = pendingTopics.isEmpty() && !fallbackRequired && !completed;
                if (completeFuture) {
                    completed = true;
                }
            }
            if (completeFuture) {
                completeFuture();
            }
        }

        private void requireFallback() {
            synchronized (pendingTopics) {
                if (!completed) {
                    fallbackRequired = true;
                }
            }
        }

        private void completeAll() {
            boolean completeFuture;
            synchronized (pendingTopics) {
                pendingTopics.clear();
                completeFuture = !completed;
                completed = true;
            }
            if (completeFuture) {
                completeFuture();
            }
        }

        private void completeFuture() {
            completionAction.run();
            future.complete(null);
        }
    }
}
