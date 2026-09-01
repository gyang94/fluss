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

import org.apache.fluss.kafka.backend.produce.KafkaProduceResult.PartitionResult;
import org.apache.fluss.kafka.backend.produce.KafkaProduceResult.TopicResult;

import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link KafkaProduceResult}. */
class KafkaProduceResultTest {

    @Test
    void testPartitionErrorMessageIsLimitedByUtf8Bytes() {
        String asciiMessage =
                repeat("a", KafkaProduceResult.MAX_PARTITION_ERROR_MESSAGE_BYTES + 10);
        PartitionResult asciiResult = failedPartition(asciiMessage);

        assertThat(utf8Length(asciiResult.errorMessage()))
                .isEqualTo(KafkaProduceResult.MAX_PARTITION_ERROR_MESSAGE_BYTES);
        assertThat(asciiResult.errorMessage())
                .isEqualTo(repeat("a", KafkaProduceResult.MAX_PARTITION_ERROR_MESSAGE_BYTES));

        String completeMultibytePrefix = repeat("a", 1021) + "界";
        PartitionResult completeMultibyteResult = failedPartition(completeMultibytePrefix + "x");
        assertThat(completeMultibyteResult.errorMessage()).isEqualTo(completeMultibytePrefix);
        assertThat(utf8Length(completeMultibyteResult.errorMessage()))
                .isEqualTo(KafkaProduceResult.MAX_PARTITION_ERROR_MESSAGE_BYTES);

        String incompleteMultibytePrefix = repeat("a", 1022) + "界";
        PartitionResult incompleteMultibyteResult = failedPartition(incompleteMultibytePrefix);
        assertThat(incompleteMultibyteResult.errorMessage()).isEqualTo(repeat("a", 1022));
        assertThat(incompleteMultibyteResult.errorMessage()).doesNotContain("\ufffd");
        assertThat(utf8Length(incompleteMultibyteResult.errorMessage()))
                .isLessThanOrEqualTo(KafkaProduceResult.MAX_PARTITION_ERROR_MESSAGE_BYTES);
    }

    @Test
    void testTopicErrorMessagesRemainBoundedAcrossManyPartitions() {
        int partitionCount = 256;
        String hugeErrorMessage = repeat("错误", 16 * 1024);
        List<PartitionResult> partitions = new ArrayList<>(partitionCount);
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            partitions.add(
                    new PartitionResult(
                            partitionId,
                            Errors.UNKNOWN_SERVER_ERROR,
                            -1L,
                            partitionId + hugeErrorMessage));
        }

        KafkaProduceResult result =
                new KafkaProduceResult(
                        java.util.Collections.singletonList(new TopicResult("topic", partitions)));

        long totalErrorMessageBytes = 0;
        for (PartitionResult partition : result.topics().get(0).partitions()) {
            int errorMessageBytes = utf8Length(partition.errorMessage());
            assertThat(errorMessageBytes)
                    .isLessThanOrEqualTo(KafkaProduceResult.MAX_PARTITION_ERROR_MESSAGE_BYTES);
            totalErrorMessageBytes += errorMessageBytes;
        }
        assertThat(totalErrorMessageBytes)
                .isLessThanOrEqualTo(KafkaProduceResult.MAX_TOTAL_ERROR_MESSAGE_BYTES);
    }

    private static PartitionResult failedPartition(String errorMessage) {
        return new PartitionResult(0, Errors.UNKNOWN_SERVER_ERROR, -1L, errorMessage);
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
