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

import org.apache.fluss.kafka.admission.KafkaProduceAdmissionController;
import org.apache.fluss.kafka.admission.KafkaProduceAdmissionController.ConnectionHandle;
import org.apache.fluss.kafka.admission.KafkaProduceAdmissionController.RequestLease;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.Record;
import org.apache.fluss.kafka.network.KafkaFrameAdmissionLease;
import org.apache.fluss.rpc.netty.server.RequestChannel;
import org.apache.fluss.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/** Tests bounded streaming copies of Kafka Produce record batches. */
class ProduceHandlerCopyMemoryTest {

    private static final short PRODUCE_VERSION = ApiKeys.PRODUCE.latestVersion();

    @Test
    void testCopiesGzipBatchWithStreamingIterator() {
        byte[] value = repeatedBytes(32 * 1024);
        MemoryRecords records = records(Compression.gzip().build(), record(value));

        List<Record> copied =
                ProduceHandler.copyRecordsForTesting(
                        PRODUCE_VERSION, records, 256 * 1024, 64 * 1024);

        assertThat(copied).hasSize(1);
        assertThat(copied.get(0).borrowedKey()).containsExactly(1, 2, 3);
        assertThat(copied.get(0).borrowedValue()).containsExactly(value);
    }

    @Test
    void testCompressedTransientReservationIsReleasedAfterCopy() {
        byte[] value = repeatedBytes(32 * 1024);
        MemoryRecords records = records(Compression.gzip().build(), record(value));
        TrackingLease lease = new TrackingLease();

        List<Record> copied =
                ProduceHandler.copyRecordsForTesting(
                        PRODUCE_VERSION, records, 256 * 1024, 64 * 1024, lease);

        assertThat(copied).hasSize(1);
        assertThat(lease.grownBytes).isEqualTo(Record.estimateBaseCopiedBytes(3, value.length));
        assertThat(lease.peakGrownBytes).isGreaterThan(lease.grownBytes);
        lease.releaseFrameBytes();
        assertThat(lease.grownBytes).isZero();
    }

    @Test
    void testCopiesZstdBatchWithStreamingIterator() {
        byte[] value = repeatedBytes(24 * 1024);
        MemoryRecords records = records(Compression.zstd().build(), record(value));

        List<Record> copied =
                ProduceHandler.copyRecordsForTesting(
                        PRODUCE_VERSION, records, 256 * 1024, 64 * 1024);

        assertThat(copied)
                .singleElement()
                .satisfies(record -> assertThat(record.borrowedValue()).containsExactly(value));
    }

    @Test
    void testRejectsHighlyCompressedSingleRecordBeforePayloadCopy() {
        MemoryRecords records =
                records(Compression.gzip().build(), record(repeatedBytes(128 * 1024)));

        assertThatThrownBy(
                        () ->
                                ProduceHandler.copyRecordsForTesting(
                                        PRODUCE_VERSION, records, 256 * 1024, 16 * 1024))
                .isInstanceOf(RecordTooLargeException.class)
                .hasMessageContaining("decompressed record")
                .hasMessageContaining("16384 bytes");
    }

    @Test
    void testRejectsGzipBombWhenCopiedPayloadCannotGrowPfRawLease() {
        MemoryRecords records =
                records(Compression.gzip().build(), record(repeatedBytes(128 * 1024)));
        long frameBytes = records.sizeInBytes() + 4L;
        KafkaProduceAdmissionController controller =
                new KafkaProduceAdmissionController(10, 32 * 1024, 10, 32 * 1024);
        RequestChannel requestChannel = new RequestChannel(1_000);
        EmbeddedChannel channel = new EmbeddedChannel();
        requestChannel.registerChannel(channel);
        ConnectionHandle connection = controller.registerConnection(channel, requestChannel);
        RequestLease lease = connection.reserve(frameBytes).getFuture().join();

        try {
            assertThat(frameBytes).isLessThan(1024);
            assertThatThrownBy(
                            () ->
                                    ProduceHandler.copyRecordsForTesting(
                                            PRODUCE_VERSION,
                                            records,
                                            256 * 1024,
                                            256 * 1024,
                                            lease))
                    .isInstanceOf(RecordTooLargeException.class)
                    .hasMessageContaining("PF raw-byte limit");
            assertThat(controller.rawBytes()).isEqualTo(frameBytes);
            assertThat(controller.liveBytes()).isEqualTo(frameBytes);

            lease.close();
            assertThat(controller.rawBytes()).isZero();
            assertThat(controller.liveBytes()).isZero();
        } finally {
            lease.close();
            connection.close();
            requestChannel.unregisterChannel(channel);
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void testMapsAggregateCopiedPayloadPressureToRetryableTimeout() {
        MemoryRecords records =
                records(Compression.gzip().build(), record(repeatedBytes(64 * 1024)));
        long frameBytes = records.sizeInBytes() + 4L;
        KafkaProduceAdmissionController controller =
                new KafkaProduceAdmissionController(10, 200 * 1024, 10, 200 * 1024);
        RequestChannel firstRequestChannel = new RequestChannel(1_000);
        RequestChannel secondRequestChannel = new RequestChannel(1_000);
        EmbeddedChannel firstChannel = new EmbeddedChannel();
        EmbeddedChannel secondChannel = new EmbeddedChannel();
        firstRequestChannel.registerChannel(firstChannel);
        secondRequestChannel.registerChannel(secondChannel);
        ConnectionHandle firstConnection =
                controller.registerConnection(firstChannel, firstRequestChannel);
        ConnectionHandle secondConnection =
                controller.registerConnection(secondChannel, secondRequestChannel);
        RequestLease holder = firstConnection.reserve(150 * 1024L).getFuture().join();
        RequestLease target = secondConnection.reserve(frameBytes).getFuture().join();

        try {
            Throwable failure =
                    catchThrowable(
                            () ->
                                    ProduceHandler.copyRecordsForTesting(
                                            PRODUCE_VERSION,
                                            records,
                                            256 * 1024,
                                            256 * 1024,
                                            target));
            assertThat(failure)
                    .isInstanceOf(TimeoutException.class)
                    .hasMessageContaining("admission is unavailable");
            assertThat(Errors.forException(failure)).isEqualTo(Errors.REQUEST_TIMED_OUT);
            assertThat(controller.rawBytes()).isEqualTo(150 * 1024L + frameBytes);
            assertThat(controller.liveBytes()).isEqualTo(150 * 1024L + frameBytes);
        } finally {
            holder.close();
            target.close();
            firstConnection.close();
            secondConnection.close();
            firstRequestChannel.unregisterChannel(firstChannel);
            secondRequestChannel.unregisterChannel(secondChannel);
            firstChannel.finishAndReleaseAll();
            secondChannel.finishAndReleaseAll();
        }
        assertThat(controller.rawBytes()).isZero();
        assertThat(controller.liveBytes()).isZero();
    }

    @Test
    void testRejectsAggregateDecompressedRequestAcrossRecords() {
        MemoryRecords records =
                records(
                        Compression.gzip().build(),
                        record(repeatedBytes(12 * 1024)),
                        record(repeatedBytes(12 * 1024)));

        assertThatThrownBy(
                        () ->
                                ProduceHandler.copyRecordsForTesting(
                                        PRODUCE_VERSION, records, 20 * 1024, 16 * 1024))
                .isInstanceOf(RecordTooLargeException.class)
                .hasMessageContaining("decompressed request")
                .hasMessageContaining("20480 bytes");
    }

    @Test
    void testRejectsCompressedHeaderObjectBombBeforeStreamingCopy() {
        Header[] headers = new Header[256];
        Arrays.fill(headers, new RecordHeader("", null));
        MemoryRecords records =
                records(
                        Compression.gzip().build(),
                        new SimpleRecord(1L, new byte[] {1}, new byte[] {2}, headers));

        assertThat(records.sizeInBytes()).isLessThan(1024);
        assertThatThrownBy(
                        () ->
                                ProduceHandler.copyRecordsForTesting(
                                        PRODUCE_VERSION, records, 256 * 1024, 8 * 1024))
                .isInstanceOf(RecordTooLargeException.class)
                .hasMessageContaining("copied record")
                .hasMessageContaining("8192 bytes");
    }

    @Test
    void testAcceptsHeadersWithinCopiedMemoryBoundary() {
        Header[] headers = new Header[16];
        Arrays.fill(headers, new RecordHeader("header", new byte[] {4, 5, 6}));
        MemoryRecords records =
                records(
                        Compression.zstd().build(),
                        new SimpleRecord(1L, new byte[] {1}, new byte[] {2}, headers));

        List<Record> copied =
                ProduceHandler.copyRecordsForTesting(
                        PRODUCE_VERSION, records, 256 * 1024, 4 * 1024);

        assertThat(copied)
                .singleElement()
                .satisfies(record -> assertThat(record.headers()).hasSize(16));
    }

    private static MemoryRecords records(Compression compression, SimpleRecord... records) {
        return MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V2, 1L, compression, records);
    }

    private static SimpleRecord record(byte[] value) {
        return new SimpleRecord(1L, new byte[] {1, 2, 3}, value);
    }

    private static byte[] repeatedBytes(int size) {
        byte[] bytes = new byte[size];
        Arrays.fill(bytes, (byte) 'a');
        return bytes;
    }

    private static final class TrackingLease implements KafkaFrameAdmissionLease {
        private long grownBytes;
        private long peakGrownBytes;

        @Override
        public void growFrameBytes(long additionalBytes) {
            grownBytes += additionalBytes;
            peakGrownBytes = Math.max(peakGrownBytes, grownBytes);
        }

        @Override
        public void releaseGrownFrameBytes(long additionalBytes) {
            grownBytes -= additionalBytes;
        }

        @Override
        public void releaseFrameBytes() {
            grownBytes = 0;
        }

        @Override
        public void close() {
            releaseFrameBytes();
        }
    }
}
