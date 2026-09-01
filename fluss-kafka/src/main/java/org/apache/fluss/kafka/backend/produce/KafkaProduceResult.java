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

import org.apache.kafka.common.protocol.Errors;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Result of a Kafka Produce backend invocation. */
@Internal
public final class KafkaProduceResult {
    /** Maximum UTF-8 bytes retained for one partition error message. */
    public static final int MAX_PARTITION_ERROR_MESSAGE_BYTES = 1024;

    /** Maximum UTF-8 bytes retained for all partition error messages in one result. */
    public static final int MAX_TOTAL_ERROR_MESSAGE_BYTES = 64 * 1024;

    private final List<TopicResult> topics;

    /** Creates a Produce result. */
    public KafkaProduceResult(List<TopicResult> topics) {
        this.topics = boundedTopics(topics);
    }

    /** Returns topic results in request order. */
    public List<TopicResult> topics() {
        return topics;
    }

    /** Returns a prefix whose UTF-8 representation does not exceed {@code maxBytes}. */
    public static @Nullable String limitErrorMessage(@Nullable String errorMessage, int maxBytes) {
        if (errorMessage == null) {
            return null;
        }
        if (maxBytes < 0) {
            throw new IllegalArgumentException("maxBytes must not be negative");
        }
        int utf8Bytes = 0;
        int index = 0;
        while (index < errorMessage.length()) {
            char current = errorMessage.charAt(index);
            int characterBytes;
            int characterWidth = 1;
            if (current <= 0x7f) {
                characterBytes = 1;
            } else if (current <= 0x7ff) {
                characterBytes = 2;
            } else if (Character.isHighSurrogate(current)
                    && index + 1 < errorMessage.length()
                    && Character.isLowSurrogate(errorMessage.charAt(index + 1))) {
                characterBytes = 4;
                characterWidth = 2;
            } else if (Character.isSurrogate(current)) {
                // Java's UTF-8 encoder replaces an unpaired surrogate with one byte.
                characterBytes = 1;
            } else {
                characterBytes = 3;
            }
            if (utf8Bytes + characterBytes > maxBytes) {
                break;
            }
            utf8Bytes += characterBytes;
            index += characterWidth;
        }
        if (index == 0 && !errorMessage.isEmpty()) {
            return null;
        }
        return index == errorMessage.length() ? errorMessage : errorMessage.substring(0, index);
    }

    /** Returns the number of bytes used by the message's UTF-8 representation. */
    public static int errorMessageBytes(@Nullable String errorMessage) {
        if (errorMessage == null) {
            return 0;
        }
        int bytes = 0;
        for (int index = 0; index < errorMessage.length(); index++) {
            char current = errorMessage.charAt(index);
            if (current <= 0x7f) {
                bytes++;
            } else if (current <= 0x7ff) {
                bytes += 2;
            } else if (Character.isHighSurrogate(current)
                    && index + 1 < errorMessage.length()
                    && Character.isLowSurrogate(errorMessage.charAt(index + 1))) {
                bytes += 4;
                index++;
            } else if (Character.isSurrogate(current)) {
                bytes++;
            } else {
                bytes += 3;
            }
        }
        return bytes;
    }

    private static List<TopicResult> boundedTopics(List<TopicResult> topics) {
        int remainingBytes = MAX_TOTAL_ERROR_MESSAGE_BYTES;
        List<TopicResult> boundedTopics = new ArrayList<>();
        for (TopicResult topic : checkNotNull(topics)) {
            List<PartitionResult> boundedPartitions = new ArrayList<>();
            for (PartitionResult partition : checkNotNull(topic).partitions()) {
                String errorMessage =
                        limitErrorMessage(
                                partition.errorMessage(),
                                Math.min(MAX_PARTITION_ERROR_MESSAGE_BYTES, remainingBytes));
                remainingBytes -= errorMessageBytes(errorMessage);
                boundedPartitions.add(
                        new PartitionResult(
                                partition.partitionId(),
                                partition.error(),
                                partition.baseOffset(),
                                errorMessage));
            }
            boundedTopics.add(new TopicResult(topic.topicName(), boundedPartitions));
        }
        return immutableCopy(boundedTopics);
    }

    private static <T> List<T> immutableCopy(List<T> values) {
        return Collections.unmodifiableList(new ArrayList<>(checkNotNull(values)));
    }

    /** Results for one topic. */
    @Internal
    public static final class TopicResult {
        private final String topicName;
        private final List<PartitionResult> partitions;

        /** Creates the result for one topic. */
        public TopicResult(String topicName, List<PartitionResult> partitions) {
            this.topicName = checkNotNull(topicName);
            this.partitions = immutableCopy(partitions);
        }

        /** Returns the Kafka topic name. */
        public String topicName() {
            return topicName;
        }

        /** Returns the partition results in request order. */
        public List<PartitionResult> partitions() {
            return partitions;
        }
    }

    /** Result for one partition. */
    @Internal
    public static final class PartitionResult {
        private final int partitionId;
        private final Errors error;
        private final long baseOffset;
        private final @Nullable String errorMessage;

        /** Creates the result for one partition. */
        public PartitionResult(
                int partitionId, Errors error, long baseOffset, @Nullable String errorMessage) {
            this.partitionId = partitionId;
            this.error = checkNotNull(error);
            this.baseOffset = baseOffset;
            this.errorMessage = limitErrorMessage(errorMessage, MAX_PARTITION_ERROR_MESSAGE_BYTES);
        }

        /** Returns the Kafka partition ID. */
        public int partitionId() {
            return partitionId;
        }

        /** Returns the Kafka protocol error. */
        public Errors error() {
            return error;
        }

        /** Returns the first appended offset, or {@code -1} on failure. */
        public long baseOffset() {
            return baseOffset;
        }

        /** Returns an optional diagnostic error message. */
        public @Nullable String errorMessage() {
            return errorMessage;
        }
    }
}
