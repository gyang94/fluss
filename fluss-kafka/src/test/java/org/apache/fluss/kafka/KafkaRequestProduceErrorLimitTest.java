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

import org.apache.fluss.kafka.backend.produce.KafkaProduceResult;
import org.apache.fluss.kafka.security.KafkaSaslConnection;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBufAllocator;

import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceRequestData.PartitionProduceData;
import org.apache.kafka.common.message.ProduceRequestData.TopicProduceData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaRequestProduceErrorLimitTest {

    @Test
    void testExceptionalProduceResponseIsBoundedBeforeSerialization() {
        int partitionCount = 32;
        int requestBytes = 4096;
        short version = ApiKeys.PRODUCE.latestVersion();
        ProduceRequest requestBody = produceRequest(version, partitionCount);
        CompletableFuture<AbstractResponse> responseFuture = new CompletableFuture<>();
        ByteBuf requestBuffer = ByteBufAllocator.DEFAULT.buffer();
        KafkaRequest request =
                new KafkaRequest(
                        ApiKeys.PRODUCE,
                        version,
                        new RequestHeader(ApiKeys.PRODUCE, version, "client-id", 1),
                        requestBody,
                        "KAFKA",
                        KafkaSaslConnection.plaintext(),
                        requestBuffer,
                        new TestingChannelHandlerContext(),
                        responseFuture,
                        System.nanoTime(),
                        requestBytes);
        responseFuture.completeExceptionally(new RuntimeException(repeat("错误", 16 * 1024)));

        ByteBuf serialized = request.responseBuffer();
        try {
            ProduceResponse response =
                    (ProduceResponse)
                            AbstractResponse.parseResponse(
                                    serialized.nioBuffer(), request.header());
            long totalErrorMessageBytes = 0;
            for (ProduceResponseData.PartitionProduceResponse partition :
                    response.data().responses().find("topic").partitionResponses()) {
                int errorMessageBytes = utf8Length(partition.errorMessage());
                assertThat(errorMessageBytes)
                        .isLessThanOrEqualTo(KafkaProduceResult.MAX_PARTITION_ERROR_MESSAGE_BYTES);
                totalErrorMessageBytes += errorMessageBytes;
            }
            assertThat(totalErrorMessageBytes).isLessThanOrEqualTo(requestBytes);
        } finally {
            serialized.release();
            requestBuffer.release();
        }
    }

    private static ProduceRequest produceRequest(short version, int partitionCount) {
        List<PartitionProduceData> partitions = new ArrayList<>();
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            partitions.add(
                    new PartitionProduceData()
                            .setIndex(partitionId)
                            .setRecords(MemoryRecords.EMPTY));
        }
        TopicProduceData topic =
                new TopicProduceData().setName("topic").setPartitionData(partitions);
        ProduceRequestData data =
                new ProduceRequestData()
                        .setAcks((short) 1)
                        .setTimeoutMs(1000)
                        .setTopicData(
                                new ProduceRequestData.TopicProduceDataCollection(
                                        Collections.singletonList(topic).iterator()));
        return new ProduceRequest(data, version);
    }

    private static int utf8Length(String value) {
        return value == null ? 0 : value.getBytes(StandardCharsets.UTF_8).length;
    }

    private static String repeat(String value, int repetitions) {
        StringBuilder builder = new StringBuilder(value.length() * repetitions);
        for (int index = 0; index < repetitions; index++) {
            builder.append(value);
        }
        return builder.toString();
    }
}
