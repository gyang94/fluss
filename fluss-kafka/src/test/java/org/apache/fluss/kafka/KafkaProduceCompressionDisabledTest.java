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

package org.apache.fluss.kafka;

import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.DefaultRecordBatch;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.utils.Crc32C;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

/** Disabled Produce must reject even damaged compressed payloads before record traversal. */
class KafkaProduceCompressionDisabledTest {
    static Stream<Arguments> cases() {
        List<Arguments> cases = new ArrayList<>();
        for (CompressionType codec : CompressionType.values()) {
            if (codec == CompressionType.NONE) {
                continue;
            }
            for (short acks : new short[] {-1, 0, 1}) {
                for (boolean sasl : new boolean[] {false, true}) {
                    for (boolean dynamic : new boolean[] {false, true}) {
                        cases.add(Arguments.of(codec, acks, sasl, dynamic));
                    }
                }
            }
        }
        return cases.stream();
    }

    @ParameterizedTest
    @MethodSource("cases")
    void testDisabledBeforeDecompression(
            CompressionType codec, short acks, boolean sasl, boolean dynamic) {
        for (short version : new short[] {3, 8, 11}) {
            try (KafkaServiceDisabledTest.Fixture fixture =
                    new KafkaServiceDisabledTest.Fixture(dynamic, sasl)) {
                ProduceRequest body = compressedRequest(codec, version, acks);
                RequestHeader header =
                        new RequestHeader(ApiKeys.PRODUCE, version, "compressed", 21);
                ByteBuf frame = KafkaServiceDisabledTest.frame(header, body);
                fixture.channel.writeInbound(frame);
                KafkaRequest queued = (KafkaRequest) fixture.requests.pollRequest(0);
                assertThat(queued).isNotNull();
                ProduceRequest decoded = queued.request();
                List<MemoryRecords> records = new ArrayList<>();
                for (ProduceRequestData.TopicProduceData topic : decoded.data().topicData()) {
                    for (ProduceRequestData.PartitionProduceData partition :
                            topic.partitionData()) {
                        MemoryRecords observed = spy((MemoryRecords) partition.records());
                        partition.setRecords(observed);
                        records.add(observed);
                    }
                }
                if (dynamic) {
                    // Disable after decoding, while the request is still waiting for dispatch.
                    fixture.controller.setEnabled(false);
                }
                try {
                    fixture.handler.processRequest(queued);
                } finally {
                    queued.releaseBuffer();
                }
                fixture.channel.runPendingTasks();
                if (acks == 0) {
                    assertThat(fixture.channel.isActive()).isFalse();
                    assertThat(fixture.readOutboundBytes()).isEmpty();
                } else {
                    ProduceResponse response = (ProduceResponse) fixture.response(header);
                    assertThat(response.errorCounts())
                            .containsOnlyKeys(Errors.BROKER_NOT_AVAILABLE);
                    assertThat(response.data().responses().find("db.one").partitionResponses())
                            .hasSize(3)
                            .allSatisfy(
                                    partition -> {
                                        assertThat(partition.baseOffset()).isEqualTo(-1L);
                                        assertThat(partition.errorMessage())
                                                .isEqualTo(
                                                        version >= 8
                                                                ? KafkaServiceUnavailableException
                                                                        .MESSAGE
                                                                : null);
                                    });
                    assertThat(fixture.channel.isActive()).isTrue();
                }
                for (MemoryRecords record : records) {
                    if (acks != 0) {
                        // Kafka's error response indexes partitions using their encoded sizes.
                        verify(record).sizeInBytes();
                    }
                    verifyNoMoreInteractions(record);
                }
                fixture.assertNoBusinessOrAuthentication();
                assertThat(frame.refCnt()).isZero();
                assertThat(KafkaServiceController.connection(fixture.channel).pendingRequests())
                        .isZero();
            }
        }
    }

    static ProduceRequest compressedRequest(CompressionType codec, short version, short acks) {
        byte[] value = new byte[1024];
        Arrays.fill(value, (byte) 7);
        MemoryRecords valid =
                MemoryRecords.withRecords(
                        RecordBatch.MAGIC_VALUE_V2,
                        0L,
                        Compression.of(codec).build(),
                        new SimpleRecord(123L, value));
        byte[] corruptedBytes = new byte[valid.sizeInBytes()];
        valid.buffer().duplicate().get(corruptedBytes);
        Arrays.fill(
                corruptedBytes,
                DefaultRecordBatch.RECORD_BATCH_OVERHEAD,
                corruptedBytes.length,
                (byte) 0);
        MemoryRecords corrupted = MemoryRecords.readableRecords(ByteBuffer.wrap(corruptedBytes));
        updateChecksum(corrupted);
        // A valid prefix without the LZ4 header checksum exercises a non-IOException failure.
        int truncatedSize = DefaultRecordBatch.RECORD_BATCH_OVERHEAD + 6;
        ByteBuffer truncatedBytes = ByteBuffer.allocate(truncatedSize);
        ByteBuffer source = valid.buffer().duplicate();
        source.limit(truncatedSize);
        truncatedBytes.put(source).flip();
        truncatedBytes.putInt(Long.BYTES, truncatedSize - Long.BYTES - Integer.BYTES);
        MemoryRecords truncated = MemoryRecords.readableRecords(truncatedBytes);
        updateChecksum(truncated);
        List<ProduceRequestData.PartitionProduceData> partitions = new ArrayList<>();
        for (MemoryRecords records : Arrays.asList(valid, corrupted, truncated)) {
            records.batches().iterator().next().ensureValid();
            partitions.add(
                    new ProduceRequestData.PartitionProduceData()
                            .setIndex(partitions.size())
                            .setRecords(records));
        }
        ProduceRequestData.TopicProduceData topic =
                new ProduceRequestData.TopicProduceData()
                        .setName("db.one")
                        .setPartitionData(partitions);
        return new ProduceRequest(
                new ProduceRequestData()
                        .setAcks(acks)
                        .setTimeoutMs(1000)
                        .setTopicData(
                                new ProduceRequestData.TopicProduceDataCollection(
                                        Collections.singletonList(topic).iterator())),
                version);
    }

    private static void updateChecksum(MemoryRecords records) {
        ByteBuffer bytes = records.buffer();
        int start = DefaultRecordBatch.CRC_OFFSET + Integer.BYTES;
        bytes.putInt(
                DefaultRecordBatch.CRC_OFFSET,
                (int) Crc32C.compute(bytes, start, bytes.limit() - start));
    }
}
