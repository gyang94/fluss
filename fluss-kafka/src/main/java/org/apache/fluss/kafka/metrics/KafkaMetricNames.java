/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.kafka.metrics;

/** Metric names used by the Kafka protocol compatibility layer. */
final class KafkaMetricNames {

    static final String REQUESTS_RATE = "requestsPerSecond";
    static final String ERRORS_RATE = "errorsPerSecond";
    static final String FAILED_PARTITIONS_RATE = "failedPartitionsPerSecond";
    static final String RECORDS_RATE = "recordsPerSecond";
    static final String BYTES_IN_RATE = "bytesInPerSecond";
    static final String ARROW_BYTES_OUT_RATE = "arrowBytesOutPerSecond";

    static final String IN_FLIGHT_REQUESTS = "inFlightRequests";
    static final String ARROW_ALLOCATED_BYTES = "arrowAllocatedBytes";
    static final String ACTIVE_ARROW_WRITERS = "activeArrowWriters";
    static final String ARROW_WRITER_WAITERS = "arrowWriterWaiters";
    static final String CACHED_ARROW_SCHEMA_KEYS = "cachedArrowSchemaKeys";
    static final String ARROW_POOL_ROTATIONS = "arrowPoolRotations";
    static final String ARROW_ACQUIRE_TIMEOUTS = "arrowAcquireTimeouts";
    static final String ARROW_ENCODE_ABORTS = "arrowEncodeAborts";
    static final String ARROW_RESOURCE_ERRORS = "arrowResourceErrors";
    static final String LIVE_REQUESTS = "liveRequests";
    static final String LIVE_BYTES = "liveBytes";
    static final String RAW_BYTES = "rawBytes";
    static final String PRODUCE_CONNECTIONS = "produceConnections";
    static final String PAUSED_CONNECTIONS = "pausedConnections";
    static final String MAX_CONNECTION_LIVE_REQUESTS = "maxConnectionLiveRequests";
    static final String MAX_CONNECTION_RAW_BYTES = "maxConnectionRawBytes";
    static final String LIVE_REQUEST_LIMIT = "liveRequestLimit";
    static final String RAW_BYTES_LIMIT = "rawBytesLimit";
    static final String CONNECTION_LIVE_REQUEST_LIMIT = "connectionLiveRequestLimit";
    static final String CONNECTION_RAW_BYTES_LIMIT = "connectionRawBytesLimit";
    static final String LIVE_REQUEST_PAUSE_EVENTS = "liveRequestPauseEvents";
    static final String RAW_BYTES_PAUSE_EVENTS = "rawBytesPauseEvents";
    static final String LIVE_REQUEST_OVERSHOOT_EVENTS = "liveRequestOvershootEvents";
    static final String RAW_BYTES_OVERSHOOT_EVENTS = "rawBytesOvershootEvents";
    static final String MAX_LIVE_REQUEST_OVERSHOOT = "maxLiveRequestOvershoot";
    static final String MAX_RAW_BYTES_OVERSHOOT = "maxRawBytesOvershoot";
    static final String CUMULATIVE_LIVE_REQUEST_PAUSE_TIME_MICROS =
            "cumulativeLiveRequestPauseTimeMicros";
    static final String CUMULATIVE_RAW_BYTES_PAUSE_TIME_MICROS =
            "cumulativeRawBytesPauseTimeMicros";
    static final String LONGEST_ACTIVE_LIVE_REQUEST_PAUSE_TIME_MILLIS =
            "longestActiveLiveRequestPauseTimeMillis";
    static final String LONGEST_ACTIVE_RAW_BYTES_PAUSE_TIME_MILLIS =
            "longestActiveRawBytesPauseTimeMillis";
    static final String PRODUCE_RESERVATION_WAITERS = "produceReservationWaiters";
    static final String CONTROL_RESERVATION_WAITERS = "controlReservationWaiters";
    static final String CONTROL_LIVE_REQUESTS = "controlLiveRequests";
    static final String CONTROL_RAW_BYTES = "controlRawBytes";
    static final String CONTROL_LIVE_REQUEST_LIMIT = "controlLiveRequestLimit";
    static final String CONTROL_RAW_BYTES_LIMIT = "controlRawBytesLimit";
    static final String CONTROL_CONNECTION_LIVE_REQUEST_LIMIT = "controlConnectionLiveRequestLimit";
    static final String CONTROL_CONNECTION_RAW_BYTES_LIMIT = "controlConnectionRawBytesLimit";
    static final String KAFKA_CONNECTIONS = "kafkaConnections";
    static final String KAFKA_CONNECTION_LIMIT = "kafkaConnectionLimit";
    static final String KAFKA_CONNECTION_REJECTIONS = "kafkaConnectionRejections";
    static final String RESERVATION_REJECTIONS = "reservationRejections";
    static final String RESERVATION_CANCELLATIONS = "reservationCancellations";
    static final String PRE_FRAME_WAIT_TIMEOUTS = "preFrameWaitTimeouts";
    static final String BODY_READ_TIMEOUTS = "bodyReadTimeouts";
    static final String NATIVE_IN_FLIGHT_REQUESTS = "nativeInFlightRequests";
    static final String NATIVE_CONVERTED_BYTES = "nativeConvertedBytes";
    static final String NATIVE_PENDING_RESERVED_BYTES = "nativePendingReservedBytes";
    static final String NATIVE_TOTAL_RESERVED_BYTES = "nativeTotalReservedBytes";
    static final String NATIVE_ADMISSION_WAITERS = "nativeAdmissionWaiters";
    static final String NATIVE_ADMISSION_CONNECTIONS = "nativeAdmissionConnections";
    static final String MAX_CONNECTION_NATIVE_IN_FLIGHT_REQUESTS =
            "maxConnectionNativeInFlightRequests";
    static final String MAX_CONNECTION_NATIVE_CONVERTED_BYTES = "maxConnectionNativeConvertedBytes";
    static final String MAX_CONNECTION_NATIVE_PENDING_RESERVED_BYTES =
            "maxConnectionNativePendingReservedBytes";
    static final String MAX_CONNECTION_NATIVE_TOTAL_RESERVED_BYTES =
            "maxConnectionNativeTotalReservedBytes";
    static final String NATIVE_IN_FLIGHT_REQUEST_LIMIT = "nativeInFlightRequestLimit";
    static final String NATIVE_CONVERTED_BYTES_LIMIT = "nativeConvertedBytesLimit";
    static final String NATIVE_CONNECTION_IN_FLIGHT_REQUEST_LIMIT =
            "nativeConnectionInFlightRequestLimit";
    static final String NATIVE_CONNECTION_CONVERTED_BYTES_LIMIT =
            "nativeConnectionConvertedBytesLimit";
    static final String NATIVE_PENDING_RESERVATION_LIMIT = "nativePendingReservationLimit";
    static final String NATIVE_ADMISSION_REJECTIONS = "nativeAdmissionRejections";
    static final String NATIVE_ADMISSION_TIMEOUTS = "nativeAdmissionTimeouts";
    static final String NATIVE_ADMISSION_CANCELLATIONS = "nativeAdmissionCancellations";
    static final String NATIVE_REQUEST_TOO_LARGE_REJECTIONS = "nativeRequestTooLargeRejections";
    static final String NATIVE_RESIZE_FAILURES = "nativeResizeFailures";
    static final String NATIVE_DISCONNECTS_WITH_INFLIGHT = "nativeDisconnectsWithInflight";
    static final String NATIVE_COMPLETION_GRACE_TIMEOUTS = "nativeCompletionGraceTimeouts";
    static final String NATIVE_INVARIANT_VIOLATIONS = "nativeInvariantViolations";
    static final String PENDING_RESPONSE_WRITES = "pendingResponseWrites";
    static final String PENDING_RESPONSE_WRITE_BYTES = "pendingResponseWriteBytes";
    static final String OLDEST_PENDING_RESPONSE_WRITE_AGE_MILLIS =
            "oldestPendingResponseWriteAgeMillis";
    static final String REQUEST_BYTES = "requestBytes";
    static final String RECORDS_PER_REQUEST = "recordsPerRequest";
    static final String PARTITIONS_PER_REQUEST = "partitionsPerRequest";
    static final String ARROW_BATCH_BYTES = "arrowBatchBytes";

    static final String REQUEST_DECODE_TIME_MICROS = "requestDecodeTimeMicros";
    static final String PRE_FRAME_WAIT_TIME_MICROS = "preFrameWaitTimeMicros";
    static final String BODY_READ_TIME_MICROS = "bodyReadTimeMicros";
    static final String NATIVE_ADMISSION_WAIT_TIME_MICROS = "nativeAdmissionWaitTimeMicros";
    static final String NATIVE_ESTIMATED_BYTES = "nativeEstimatedBytes";
    static final String NATIVE_ACTUAL_BYTES = "nativeActualBytes";
    static final String NATIVE_ESTIMATE_ERROR_BYTES = "nativeEstimateErrorBytes";
    static final String NATIVE_COMPLETION_TIME_MICROS = "nativeCompletionTimeMicros";
    static final String REQUEST_QUEUE_TIME_MICROS = "requestQueueTimeMicros";
    static final String RECORD_COPY_TIME_MICROS = "recordDecompressCopyTimeMicros";
    static final String TABLE_INFO_LOOKUP_TIME_MICROS = "tableInfoLookupTimeMicros";
    static final String TABLE_INFO_PARSE_TIME_MICROS = "tableInfoParseTimeMicros";
    static final String CONTRACT_COMPILE_TIME_MICROS = "contractCompileTimeMicros";
    static final String RECORD_CONVERT_TIME_MICROS = "recordDecodeAssembleTimeMicros";
    static final String ARROW_ENCODE_TIME_MICROS = "arrowEncodeTimeMicros";
    static final String ARROW_WRITER_ACQUIRE_WAIT_TIME_MICROS = "arrowWriterAcquireWaitTimeMicros";
    static final String NATIVE_PRODUCE_SUBMIT_TIME_MICROS = "nativeProduceSubmitTimeMicros";
    static final String ACKS_WAIT_TIME_MICROS = "acksWaitTimeMicros";
    static final String RESPONSE_HEAD_OF_LINE_TIME_MICROS = "responseHeadOfLineTimeMicros";
    static final String RESPONSE_WRITE_COMPLETION_TIME_MICROS = "responseWriteCompletionTimeMicros";
    static final String TOTAL_TIME_MICROS = "totalTimeMicros";

    static final String RECORD_ERRORS = "recordErrors";
    static final String INVALID_RECORDS = "invalidRecords";
    static final String RESCUED_RECORDS = "rescuedRecords";
    static final String DROPPED_RECORDS = "droppedRecords";
    static final String FAILED_RECORDS = "failedRecords";
    static final String SUCCESSFUL_RECORDS = "successfulRecords";
    static final String LAST_SUCCESSFUL_WRITE_TIME_MILLIS = "lastSuccessfulWriteTimeMillis";
    static final String PRODUCER_CONNECTIONS = "producerConnections";

    private KafkaMetricNames() {}
}
