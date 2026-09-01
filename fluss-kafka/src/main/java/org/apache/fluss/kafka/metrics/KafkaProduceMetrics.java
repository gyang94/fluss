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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.kafka.admission.KafkaNativeProduceAdmissionController;
import org.apache.fluss.kafka.admission.KafkaProduceAdmissionController;
import org.apache.fluss.kafka.network.KafkaFrameAdmissionMetrics;
import org.apache.fluss.metrics.Counter;
import org.apache.fluss.metrics.DescriptiveStatisticsHistogram;
import org.apache.fluss.metrics.Histogram;
import org.apache.fluss.metrics.MeterView;
import org.apache.fluss.metrics.ThreadSafeSimpleCounter;
import org.apache.fluss.metrics.groups.MetricGroup;
import org.apache.fluss.utils.clock.Clock;
import org.apache.fluss.utils.clock.SystemClock;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Low-cardinality runtime metrics for Kafka Produce processing on a TabletServer. */
@Internal
public final class KafkaProduceMetrics implements KafkaFrameAdmissionMetrics {

    private static final int HISTOGRAM_WINDOW_SIZE = 1024;
    private static final KafkaProduceMetrics NO_OP =
            new KafkaProduceMetrics(SystemClock.getInstance());

    private final Clock clock;
    private final boolean enabled;
    private final MetricGroup metricGroup;
    private final AtomicLong responseWriteSequence = new AtomicLong();
    private final AtomicLong pendingResponseWrites = new AtomicLong();
    private final AtomicLong pendingResponseWriteBytes = new AtomicLong();
    private final ConcurrentMap<Long, Long> pendingResponseWriteStarts = new ConcurrentHashMap<>();

    private final Counter requests;
    private final Counter errors;
    private final Counter failedPartitions;
    private final Counter records;
    private final Counter bytesIn;
    private final Counter arrowBytesOut;
    private final Counter inFlightRequests;
    private final Counter arrowPoolRotations;
    private final Counter arrowAcquireTimeouts;
    private final Counter arrowEncodeAborts;
    private final Counter arrowResourceErrors;
    private final Counter kafkaConnectionRejections;
    private final Counter reservationRejections;
    private final Counter reservationCancellations;
    private final Counter preFrameWaitTimeouts;
    private final Counter bodyReadTimeouts;
    private final Counter nativeAdmissionRejections;
    private final Counter nativeAdmissionTimeouts;
    private final Counter nativeAdmissionCancellations;
    private final Counter nativeRequestTooLargeRejections;
    private final Counter nativeResizeFailures;
    private final Counter nativeDisconnectsWithInflight;
    private final Counter nativeCompletionGraceTimeouts;
    private final Counter nativeInvariantViolations;

    private final Histogram requestBytes;
    private final Histogram recordsPerRequest;
    private final Histogram partitionsPerRequest;
    private final Histogram arrowBatchBytes;
    private final Histogram requestDecodeTimeMicros;
    private final Histogram preFrameWaitTimeMicros;
    private final Histogram bodyReadTimeMicros;
    private final Histogram nativeAdmissionWaitTimeMicros;
    private final Histogram nativeEstimatedBytes;
    private final Histogram nativeActualBytes;
    private final Histogram nativeEstimateErrorBytes;
    private final Histogram nativeCompletionTimeMicros;
    private final Histogram requestQueueTimeMicros;
    private final Histogram recordCopyTimeMicros;
    private final Histogram tableInfoLookupTimeMicros;
    private final Histogram tableInfoParseTimeMicros;
    private final Histogram contractCompileTimeMicros;
    private final Histogram recordConvertTimeMicros;
    private final Histogram arrowEncodeTimeMicros;
    private final Histogram arrowWriterAcquireWaitTimeMicros;
    private final Histogram nativeProduceSubmitTimeMicros;
    private final Histogram acksWaitTimeMicros;
    private final Histogram responseHeadOfLineTimeMicros;
    private final Histogram responseWriteCompletionTimeMicros;
    private final Histogram totalTimeMicros;

    /** Creates and registers Produce metrics below the supplied server metric group. */
    public KafkaProduceMetrics(MetricGroup serverMetricGroup) {
        this(serverMetricGroup, SystemClock.getInstance());
    }

    /** Creates registered Produce metrics using the supplied monotonic clock. */
    public KafkaProduceMetrics(MetricGroup serverMetricGroup, Clock clock) {
        this.clock = checkNotNull(clock);
        this.enabled = true;
        MetricGroup metricGroup =
                checkNotNull(serverMetricGroup).addGroup("kafka").addGroup("request", "produce");
        this.metricGroup = metricGroup;

        requests = registerMeter(metricGroup, KafkaMetricNames.REQUESTS_RATE);
        errors = registerMeter(metricGroup, KafkaMetricNames.ERRORS_RATE);
        failedPartitions = registerMeter(metricGroup, KafkaMetricNames.FAILED_PARTITIONS_RATE);
        records = registerMeter(metricGroup, KafkaMetricNames.RECORDS_RATE);
        bytesIn = registerMeter(metricGroup, KafkaMetricNames.BYTES_IN_RATE);
        arrowBytesOut = registerMeter(metricGroup, KafkaMetricNames.ARROW_BYTES_OUT_RATE);
        inFlightRequests = new ThreadSafeSimpleCounter();
        metricGroup.gauge(KafkaMetricNames.IN_FLIGHT_REQUESTS, inFlightRequests::getCount);
        arrowPoolRotations = new ThreadSafeSimpleCounter();
        arrowAcquireTimeouts = new ThreadSafeSimpleCounter();
        arrowEncodeAborts = new ThreadSafeSimpleCounter();
        arrowResourceErrors = new ThreadSafeSimpleCounter();
        kafkaConnectionRejections = new ThreadSafeSimpleCounter();
        reservationRejections = new ThreadSafeSimpleCounter();
        reservationCancellations = new ThreadSafeSimpleCounter();
        preFrameWaitTimeouts = new ThreadSafeSimpleCounter();
        bodyReadTimeouts = new ThreadSafeSimpleCounter();
        nativeAdmissionRejections = new ThreadSafeSimpleCounter();
        nativeAdmissionTimeouts = new ThreadSafeSimpleCounter();
        nativeAdmissionCancellations = new ThreadSafeSimpleCounter();
        nativeRequestTooLargeRejections = new ThreadSafeSimpleCounter();
        nativeResizeFailures = new ThreadSafeSimpleCounter();
        nativeDisconnectsWithInflight = new ThreadSafeSimpleCounter();
        nativeCompletionGraceTimeouts = new ThreadSafeSimpleCounter();
        nativeInvariantViolations = new ThreadSafeSimpleCounter();
        metricGroup.counter(KafkaMetricNames.ARROW_POOL_ROTATIONS, arrowPoolRotations);
        metricGroup.counter(KafkaMetricNames.ARROW_ACQUIRE_TIMEOUTS, arrowAcquireTimeouts);
        metricGroup.counter(KafkaMetricNames.ARROW_ENCODE_ABORTS, arrowEncodeAborts);
        metricGroup.counter(KafkaMetricNames.ARROW_RESOURCE_ERRORS, arrowResourceErrors);
        metricGroup.counter(
                KafkaMetricNames.KAFKA_CONNECTION_REJECTIONS, kafkaConnectionRejections);
        metricGroup.counter(KafkaMetricNames.RESERVATION_REJECTIONS, reservationRejections);
        metricGroup.counter(KafkaMetricNames.RESERVATION_CANCELLATIONS, reservationCancellations);
        metricGroup.counter(KafkaMetricNames.PRE_FRAME_WAIT_TIMEOUTS, preFrameWaitTimeouts);
        metricGroup.counter(KafkaMetricNames.BODY_READ_TIMEOUTS, bodyReadTimeouts);
        metricGroup.counter(
                KafkaMetricNames.NATIVE_ADMISSION_REJECTIONS, nativeAdmissionRejections);
        metricGroup.counter(KafkaMetricNames.NATIVE_ADMISSION_TIMEOUTS, nativeAdmissionTimeouts);
        metricGroup.counter(
                KafkaMetricNames.NATIVE_ADMISSION_CANCELLATIONS, nativeAdmissionCancellations);
        metricGroup.counter(
                KafkaMetricNames.NATIVE_REQUEST_TOO_LARGE_REJECTIONS,
                nativeRequestTooLargeRejections);
        metricGroup.counter(KafkaMetricNames.NATIVE_RESIZE_FAILURES, nativeResizeFailures);
        metricGroup.counter(
                KafkaMetricNames.NATIVE_DISCONNECTS_WITH_INFLIGHT, nativeDisconnectsWithInflight);
        metricGroup.counter(
                KafkaMetricNames.NATIVE_COMPLETION_GRACE_TIMEOUTS, nativeCompletionGraceTimeouts);
        metricGroup.counter(
                KafkaMetricNames.NATIVE_INVARIANT_VIOLATIONS, nativeInvariantViolations);
        metricGroup.gauge(KafkaMetricNames.PENDING_RESPONSE_WRITES, pendingResponseWrites::get);
        metricGroup.gauge(
                KafkaMetricNames.PENDING_RESPONSE_WRITE_BYTES, pendingResponseWriteBytes::get);
        metricGroup.gauge(
                KafkaMetricNames.OLDEST_PENDING_RESPONSE_WRITE_AGE_MILLIS,
                this::oldestPendingResponseWriteAgeMillis);

        requestBytes = registerHistogram(metricGroup, KafkaMetricNames.REQUEST_BYTES);
        recordsPerRequest = registerHistogram(metricGroup, KafkaMetricNames.RECORDS_PER_REQUEST);
        partitionsPerRequest =
                registerHistogram(metricGroup, KafkaMetricNames.PARTITIONS_PER_REQUEST);
        arrowBatchBytes = registerHistogram(metricGroup, KafkaMetricNames.ARROW_BATCH_BYTES);
        requestDecodeTimeMicros =
                registerHistogram(metricGroup, KafkaMetricNames.REQUEST_DECODE_TIME_MICROS);
        preFrameWaitTimeMicros =
                registerHistogram(metricGroup, KafkaMetricNames.PRE_FRAME_WAIT_TIME_MICROS);
        bodyReadTimeMicros = registerHistogram(metricGroup, KafkaMetricNames.BODY_READ_TIME_MICROS);
        nativeAdmissionWaitTimeMicros =
                registerHistogram(metricGroup, KafkaMetricNames.NATIVE_ADMISSION_WAIT_TIME_MICROS);
        nativeEstimatedBytes =
                registerHistogram(metricGroup, KafkaMetricNames.NATIVE_ESTIMATED_BYTES);
        nativeActualBytes = registerHistogram(metricGroup, KafkaMetricNames.NATIVE_ACTUAL_BYTES);
        nativeEstimateErrorBytes =
                registerHistogram(metricGroup, KafkaMetricNames.NATIVE_ESTIMATE_ERROR_BYTES);
        nativeCompletionTimeMicros =
                registerHistogram(metricGroup, KafkaMetricNames.NATIVE_COMPLETION_TIME_MICROS);
        requestQueueTimeMicros =
                registerHistogram(metricGroup, KafkaMetricNames.REQUEST_QUEUE_TIME_MICROS);
        recordCopyTimeMicros =
                registerHistogram(metricGroup, KafkaMetricNames.RECORD_COPY_TIME_MICROS);
        tableInfoLookupTimeMicros =
                registerHistogram(metricGroup, KafkaMetricNames.TABLE_INFO_LOOKUP_TIME_MICROS);
        tableInfoParseTimeMicros =
                registerHistogram(metricGroup, KafkaMetricNames.TABLE_INFO_PARSE_TIME_MICROS);
        contractCompileTimeMicros =
                registerHistogram(metricGroup, KafkaMetricNames.CONTRACT_COMPILE_TIME_MICROS);
        recordConvertTimeMicros =
                registerHistogram(metricGroup, KafkaMetricNames.RECORD_CONVERT_TIME_MICROS);
        arrowEncodeTimeMicros =
                registerHistogram(metricGroup, KafkaMetricNames.ARROW_ENCODE_TIME_MICROS);
        arrowWriterAcquireWaitTimeMicros =
                registerHistogram(
                        metricGroup, KafkaMetricNames.ARROW_WRITER_ACQUIRE_WAIT_TIME_MICROS);
        nativeProduceSubmitTimeMicros =
                registerHistogram(metricGroup, KafkaMetricNames.NATIVE_PRODUCE_SUBMIT_TIME_MICROS);
        acksWaitTimeMicros = registerHistogram(metricGroup, KafkaMetricNames.ACKS_WAIT_TIME_MICROS);
        responseHeadOfLineTimeMicros =
                registerHistogram(metricGroup, KafkaMetricNames.RESPONSE_HEAD_OF_LINE_TIME_MICROS);
        responseWriteCompletionTimeMicros =
                registerHistogram(
                        metricGroup, KafkaMetricNames.RESPONSE_WRITE_COMPLETION_TIME_MICROS);
        totalTimeMicros = registerHistogram(metricGroup, KafkaMetricNames.TOTAL_TIME_MICROS);
    }

    private KafkaProduceMetrics(Clock clock) {
        this.clock = clock;
        this.enabled = false;
        this.metricGroup = null;
        this.requests = null;
        this.errors = null;
        this.failedPartitions = null;
        this.records = null;
        this.bytesIn = null;
        this.arrowBytesOut = null;
        this.inFlightRequests = null;
        this.arrowPoolRotations = null;
        this.arrowAcquireTimeouts = null;
        this.arrowEncodeAborts = null;
        this.arrowResourceErrors = null;
        this.kafkaConnectionRejections = null;
        this.reservationRejections = null;
        this.reservationCancellations = null;
        this.preFrameWaitTimeouts = null;
        this.bodyReadTimeouts = null;
        this.nativeAdmissionRejections = null;
        this.nativeAdmissionTimeouts = null;
        this.nativeAdmissionCancellations = null;
        this.nativeRequestTooLargeRejections = null;
        this.nativeResizeFailures = null;
        this.nativeDisconnectsWithInflight = null;
        this.nativeCompletionGraceTimeouts = null;
        this.nativeInvariantViolations = null;
        this.requestBytes = null;
        this.recordsPerRequest = null;
        this.partitionsPerRequest = null;
        this.arrowBatchBytes = null;
        this.requestDecodeTimeMicros = null;
        this.preFrameWaitTimeMicros = null;
        this.bodyReadTimeMicros = null;
        this.nativeAdmissionWaitTimeMicros = null;
        this.nativeEstimatedBytes = null;
        this.nativeActualBytes = null;
        this.nativeEstimateErrorBytes = null;
        this.nativeCompletionTimeMicros = null;
        this.requestQueueTimeMicros = null;
        this.recordCopyTimeMicros = null;
        this.tableInfoLookupTimeMicros = null;
        this.tableInfoParseTimeMicros = null;
        this.contractCompileTimeMicros = null;
        this.recordConvertTimeMicros = null;
        this.arrowEncodeTimeMicros = null;
        this.arrowWriterAcquireWaitTimeMicros = null;
        this.nativeProduceSubmitTimeMicros = null;
        this.acksWaitTimeMicros = null;
        this.responseHeadOfLineTimeMicros = null;
        this.responseWriteCompletionTimeMicros = null;
        this.totalTimeMicros = null;
    }

    /** Returns the shared disabled metrics instance used by compatibility constructors. */
    public static KafkaProduceMetrics noOp() {
        return NO_OP;
    }

    /** Returns a monotonic timestamp for starting an asynchronous or synchronous stage. */
    public long nowNanos() {
        try {
            return clock.nanoseconds();
        } catch (Throwable ignored) {
            // A metrics clock must not break request ownership or output-buffer cleanup.
            return System.nanoTime();
        }
    }

    /** Records Kafka wire parsing for a Produce frame. */
    public void recordRequestDecode(long startedNanos) {
        update(requestDecodeTimeMicros, startedNanos);
    }

    /** Starts request-level accounting when a Produce request enters a worker. */
    public void requestStarted(long receivedNanos, int frameBytes) {
        if (!enabled) {
            return;
        }
        requests.inc();
        bytesIn.inc(frameBytes);
        requestBytes.update(frameBytes);
        inFlightRequests.inc();
        update(requestQueueTimeMicros, receivedNanos);
    }

    /** Completes request-level accounting when the Produce response becomes ready. */
    public void requestCompleted(long receivedNanos, boolean failed, int failedPartitionCount) {
        if (!enabled) {
            return;
        }
        if (failed) {
            errors.inc();
        }
        failedPartitions.inc(failedPartitionCount);
        update(totalTimeMicros, receivedNanos);
        inFlightRequests.dec();
    }

    /** Records record decompression/copying and request cardinalities. */
    public void recordRecordCopy(long startedNanos, int recordCount, int partitionCount) {
        if (!enabled) {
            return;
        }
        records.inc(recordCount);
        recordsPerRequest.update(recordCount);
        partitionsPerRequest.update(partitionCount);
        update(recordCopyTimeMicros, startedNanos);
    }

    /** Records one topic-level TableInfo lookup. */
    public void recordTableInfoLookup(long startedNanos) {
        update(tableInfoLookupTimeMicros, startedNanos);
    }

    /** Records TableDescriptor parsing and TableInfo construction for one topic. */
    public void recordTableInfoParse(long startedNanos) {
        update(tableInfoParseTimeMicros, startedNanos);
    }

    /** Records contract resolution and decoder construction for one partition. */
    public void recordContractCompile(long startedNanos) {
        update(contractCompileTimeMicros, startedNanos);
    }

    /** Records decoding and assembling all records for one partition. */
    public void recordDecodeAssemble(long startedNanos) {
        update(recordConvertTimeMicros, startedNanos);
    }

    /** Records Arrow encoding and the produced native batch size for one partition. */
    public void recordArrowEncode(long startedNanos, int outputBytes) {
        if (!enabled) {
            return;
        }
        update(arrowEncodeTimeMicros, startedNanos);
        if (outputBytes >= 0) {
            arrowBytesOut.inc(outputBytes);
            arrowBatchBytes.update(outputBytes);
        }
    }

    /** Registers gauges supplied by the instance-local Arrow writer manager. */
    public void registerArrowResourceGauges(
            LongSupplier allocatedBytes,
            IntSupplier activeWriters,
            IntSupplier waiters,
            IntSupplier cachedSchemaKeys) {
        if (!enabled) {
            return;
        }
        metricGroup.gauge(KafkaMetricNames.ARROW_ALLOCATED_BYTES, allocatedBytes::getAsLong);
        metricGroup.gauge(KafkaMetricNames.ACTIVE_ARROW_WRITERS, activeWriters::getAsInt);
        metricGroup.gauge(KafkaMetricNames.ARROW_WRITER_WAITERS, waiters::getAsInt);
        metricGroup.gauge(KafkaMetricNames.CACHED_ARROW_SCHEMA_KEYS, cachedSchemaKeys::getAsInt);
    }

    /** Registers gauges supplied by the TabletServer-local Produce admission controller. */
    public void registerAdmissionGauges(KafkaProduceAdmissionController admissionController) {
        if (!enabled) {
            return;
        }
        checkNotNull(admissionController, "admissionController");
        metricGroup.gauge(KafkaMetricNames.LIVE_REQUESTS, admissionController::liveRequests);
        metricGroup.gauge(KafkaMetricNames.LIVE_BYTES, admissionController::liveBytes);
        metricGroup.gauge(KafkaMetricNames.RAW_BYTES, admissionController::rawBytes);
        metricGroup.gauge(
                KafkaMetricNames.PRODUCE_CONNECTIONS, admissionController::registeredConnections);
        metricGroup.gauge(
                KafkaMetricNames.PAUSED_CONNECTIONS, admissionController::pausedConnections);
        metricGroup.gauge(
                KafkaMetricNames.PRODUCE_RESERVATION_WAITERS,
                admissionController::pendingReservations);
        metricGroup.gauge(
                KafkaMetricNames.MAX_CONNECTION_LIVE_REQUESTS,
                admissionController::maxConnectionLiveRequests);
        metricGroup.gauge(
                KafkaMetricNames.MAX_CONNECTION_RAW_BYTES,
                admissionController::maxConnectionRawBytes);
        metricGroup.gauge(
                KafkaMetricNames.LIVE_REQUEST_LIMIT, admissionController::maxLiveRequestsLimit);
        metricGroup.gauge(KafkaMetricNames.RAW_BYTES_LIMIT, admissionController::maxRawBytesLimit);
        metricGroup.gauge(
                KafkaMetricNames.CONNECTION_LIVE_REQUEST_LIMIT,
                admissionController::maxLiveRequestsPerConnectionLimit);
        metricGroup.gauge(
                KafkaMetricNames.CONNECTION_RAW_BYTES_LIMIT,
                admissionController::maxRawBytesPerConnectionLimit);
        metricGroup.gauge(
                KafkaMetricNames.LIVE_REQUEST_PAUSE_EVENTS,
                admissionController::liveRequestPauseEvents);
        metricGroup.gauge(
                KafkaMetricNames.RAW_BYTES_PAUSE_EVENTS, admissionController::rawBytesPauseEvents);
        metricGroup.gauge(
                KafkaMetricNames.LIVE_REQUEST_OVERSHOOT_EVENTS,
                admissionController::liveRequestOvershootEvents);
        metricGroup.gauge(
                KafkaMetricNames.RAW_BYTES_OVERSHOOT_EVENTS,
                admissionController::rawBytesOvershootEvents);
        metricGroup.gauge(
                KafkaMetricNames.MAX_LIVE_REQUEST_OVERSHOOT,
                admissionController::maxLiveRequestOvershoot);
        metricGroup.gauge(
                KafkaMetricNames.MAX_RAW_BYTES_OVERSHOOT,
                admissionController::maxRawBytesOvershoot);
        metricGroup.gauge(
                KafkaMetricNames.CUMULATIVE_LIVE_REQUEST_PAUSE_TIME_MICROS,
                admissionController::cumulativeLiveRequestPauseTimeMicros);
        metricGroup.gauge(
                KafkaMetricNames.CUMULATIVE_RAW_BYTES_PAUSE_TIME_MICROS,
                admissionController::cumulativeRawBytesPauseTimeMicros);
        metricGroup.gauge(
                KafkaMetricNames.LONGEST_ACTIVE_LIVE_REQUEST_PAUSE_TIME_MILLIS,
                admissionController::longestActiveLiveRequestPauseTimeMillis);
        metricGroup.gauge(
                KafkaMetricNames.LONGEST_ACTIVE_RAW_BYTES_PAUSE_TIME_MILLIS,
                admissionController::longestActiveRawBytesPauseTimeMillis);
    }

    /** Registers gauges supplied by the converted/native Produce admission controller. */
    public void registerNativeAdmissionGauges(
            KafkaNativeProduceAdmissionController admissionController) {
        if (!enabled) {
            return;
        }
        checkNotNull(admissionController, "admissionController");
        metricGroup.gauge(
                KafkaMetricNames.NATIVE_IN_FLIGHT_REQUESTS, admissionController::inFlightRequests);
        metricGroup.gauge(
                KafkaMetricNames.NATIVE_CONVERTED_BYTES, admissionController::convertedBytes);
        metricGroup.gauge(
                KafkaMetricNames.NATIVE_PENDING_RESERVED_BYTES,
                admissionController::pendingReservedBytes);
        metricGroup.gauge(
                KafkaMetricNames.NATIVE_TOTAL_RESERVED_BYTES,
                admissionController::totalReservedBytes);
        metricGroup.gauge(
                KafkaMetricNames.NATIVE_ADMISSION_WAITERS,
                admissionController::pendingReservations);
        metricGroup.gauge(
                KafkaMetricNames.NATIVE_ADMISSION_CONNECTIONS,
                admissionController::registeredConnections);
        metricGroup.gauge(
                KafkaMetricNames.MAX_CONNECTION_NATIVE_IN_FLIGHT_REQUESTS,
                admissionController::maxConnectionInFlightRequests);
        metricGroup.gauge(
                KafkaMetricNames.MAX_CONNECTION_NATIVE_CONVERTED_BYTES,
                admissionController::maxConnectionConvertedBytes);
        metricGroup.gauge(
                KafkaMetricNames.MAX_CONNECTION_NATIVE_PENDING_RESERVED_BYTES,
                admissionController::maxConnectionPendingReservedBytes);
        metricGroup.gauge(
                KafkaMetricNames.MAX_CONNECTION_NATIVE_TOTAL_RESERVED_BYTES,
                admissionController::maxConnectionTotalReservedBytes);
        metricGroup.gauge(
                KafkaMetricNames.NATIVE_IN_FLIGHT_REQUEST_LIMIT,
                admissionController::maxInFlightRequestsLimit);
        metricGroup.gauge(
                KafkaMetricNames.NATIVE_CONVERTED_BYTES_LIMIT,
                admissionController::maxConvertedBytesLimit);
        metricGroup.gauge(
                KafkaMetricNames.NATIVE_CONNECTION_IN_FLIGHT_REQUEST_LIMIT,
                admissionController::maxInFlightRequestsPerConnectionLimit);
        metricGroup.gauge(
                KafkaMetricNames.NATIVE_CONNECTION_CONVERTED_BYTES_LIMIT,
                admissionController::maxConvertedBytesPerConnectionLimit);
        metricGroup.gauge(
                KafkaMetricNames.NATIVE_PENDING_RESERVATION_LIMIT,
                admissionController::maxPendingReservationsLimit);
    }

    /** Registers gauges supplied by the TabletServer-local control-plane admission lane. */
    public void registerControlAdmissionGauges(
            IntSupplier waitingReservations,
            LongSupplier liveRequests,
            LongSupplier rawBytes,
            LongSupplier liveRequestLimit,
            LongSupplier rawBytesLimit,
            LongSupplier connectionLiveRequestLimit,
            LongSupplier connectionRawBytesLimit) {
        if (!enabled) {
            return;
        }
        metricGroup.gauge(
                KafkaMetricNames.CONTROL_RESERVATION_WAITERS,
                checkNotNull(waitingReservations, "waitingReservations")::getAsInt);
        metricGroup.gauge(
                KafkaMetricNames.CONTROL_LIVE_REQUESTS,
                checkNotNull(liveRequests, "liveRequests")::getAsLong);
        metricGroup.gauge(
                KafkaMetricNames.CONTROL_RAW_BYTES, checkNotNull(rawBytes, "rawBytes")::getAsLong);
        metricGroup.gauge(
                KafkaMetricNames.CONTROL_LIVE_REQUEST_LIMIT,
                checkNotNull(liveRequestLimit, "liveRequestLimit")::getAsLong);
        metricGroup.gauge(
                KafkaMetricNames.CONTROL_RAW_BYTES_LIMIT,
                checkNotNull(rawBytesLimit, "rawBytesLimit")::getAsLong);
        metricGroup.gauge(
                KafkaMetricNames.CONTROL_CONNECTION_LIVE_REQUEST_LIMIT,
                checkNotNull(connectionLiveRequestLimit, "connectionLiveRequestLimit")::getAsLong);
        metricGroup.gauge(
                KafkaMetricNames.CONTROL_CONNECTION_RAW_BYTES_LIMIT,
                checkNotNull(connectionRawBytesLimit, "connectionRawBytesLimit")::getAsLong);
    }

    /** Registers the current and configured maximum Kafka connection counts. */
    public void registerKafkaConnectionGauges(
            IntSupplier connections, IntSupplier connectionLimit) {
        if (!enabled) {
            return;
        }
        metricGroup.gauge(
                KafkaMetricNames.KAFKA_CONNECTIONS,
                checkNotNull(connections, "connections")::getAsInt);
        metricGroup.gauge(
                KafkaMetricNames.KAFKA_CONNECTION_LIMIT,
                checkNotNull(connectionLimit, "connectionLimit")::getAsInt);
    }

    /** Records a Kafka connection rejected by the TabletServer connection limit. */
    public void recordKafkaConnectionRejected() {
        if (enabled) {
            kafkaConnectionRejections.inc();
        }
    }

    /** Records a frame reservation rejected before its body is read. */
    public void recordReservationRejected() {
        if (enabled) {
            reservationRejections.inc();
        }
    }

    /** Records a pending frame reservation cancelled before it was granted. */
    public void recordReservationCancelled() {
        if (enabled) {
            reservationCancellations.inc();
        }
    }

    /** Records a pre-frame admission timeout and the time spent waiting. */
    public void recordPreFrameWaitTimeout(long startedNanos) {
        if (!enabled) {
            return;
        }
        preFrameWaitTimeouts.inc();
        update(preFrameWaitTimeMicros, startedNanos);
    }

    /** Records successful completion of pre-frame admission waiting. */
    public void recordPreFrameWait(long startedNanos) {
        update(preFrameWaitTimeMicros, startedNanos);
    }

    /** Records a frame body read timeout and the time spent reading. */
    public void recordBodyReadTimeout(long startedNanos) {
        if (!enabled) {
            return;
        }
        bodyReadTimeouts.inc();
        update(bodyReadTimeMicros, startedNanos);
    }

    /** Records successful completion of an admitted frame body read. */
    public void recordBodyRead(long startedNanos) {
        update(bodyReadTimeMicros, startedNanos);
    }

    /** Records a granted native admission reservation and its initial estimate. */
    public void recordNativeAdmissionGranted(long startedNanos, long estimatedBytes) {
        if (!enabled) {
            return;
        }
        update(nativeAdmissionWaitTimeMicros, startedNanos);
        nativeEstimatedBytes.update(estimatedBytes);
    }

    /** Records an immediate native admission rejection caused by bounded local capacity. */
    public void recordNativeAdmissionRejected() {
        if (enabled) {
            nativeAdmissionRejections.inc();
        }
    }

    /** Records cancellation of a queued native admission reservation. */
    public void recordNativeAdmissionCancelled() {
        if (enabled) {
            nativeAdmissionCancellations.inc();
        }
    }

    /** Records a native admission timeout and the time spent waiting. */
    public void recordNativeAdmissionTimeout(long startedNanos) {
        if (!enabled) {
            return;
        }
        nativeAdmissionTimeouts.inc();
        update(nativeAdmissionWaitTimeMicros, startedNanos);
    }

    /** Records a converted request that can never fit within a native admission byte limit. */
    public void recordNativeRequestTooLarge() {
        if (enabled) {
            nativeRequestTooLargeRejections.inc();
        }
    }

    /** Records the actual converted size and estimate error after conversion. */
    public void recordNativeResize(long estimatedBytes, long actualBytes, boolean succeeded) {
        if (!enabled) {
            return;
        }
        nativeActualBytes.update(actualBytes);
        nativeEstimateErrorBytes.update(actualBytes - estimatedBytes);
        if (!succeeded) {
            nativeResizeFailures.inc();
        }
    }

    /** Records a connection that closed while it still owned granted native admission. */
    public void recordNativeDisconnectWithInflight() {
        if (enabled) {
            nativeDisconnectsWithInflight.inc();
        }
    }

    /** Records a native Produce future that exceeded the configured completion grace period. */
    public void recordNativeCompletionGraceTimeout() {
        if (enabled) {
            nativeCompletionGraceTimeouts.inc();
        }
    }

    /** Records a detected native admission accounting invariant violation. */
    public void recordNativeInvariantViolation() {
        if (enabled) {
            nativeInvariantViolations.inc();
        }
    }

    /** Records the lifetime from native admission grant through original future completion. */
    public void recordNativeCompletion(long startedNanos) {
        update(nativeCompletionTimeMicros, startedNanos);
    }

    /** Records the time spent waiting for an Arrow writer permit. */
    public void recordArrowWriterAcquireWait(long startedNanos) {
        update(arrowWriterAcquireWaitTimeMicros, startedNanos);
    }

    /** Records a writer-pool generation rotation. */
    public void recordArrowPoolRotation() {
        if (enabled) {
            arrowPoolRotations.inc();
        }
    }

    /** Records an Arrow writer acquire timeout. */
    public void recordArrowAcquireTimeout() {
        if (enabled) {
            arrowAcquireTimeouts.inc();
        }
    }

    /** Records an aborted Arrow encoding attempt. */
    public void recordArrowEncodeAbort() {
        if (enabled) {
            arrowEncodeAborts.inc();
        }
    }

    /** Records an Arrow allocation, pooling, or lifecycle error. */
    public void recordArrowResourceError() {
        if (enabled) {
            arrowResourceErrors.inc();
        }
    }

    /** Records the synchronous part of submitting one native Produce request. */
    public void recordNativeProduceSubmit(long startedNanos) {
        update(nativeProduceSubmitTimeMicros, startedNanos);
    }

    /** Records asynchronous acknowledgement waiting for one acks=all topic write. */
    public void recordAcksWait(long startedNanos) {
        update(acksWaitTimeMicros, startedNanos);
    }

    /** Records ordered-response head-of-line waiting. */
    public void recordResponseHeadOfLine(long responseReadyNanos) {
        update(responseHeadOfLineTimeMicros, responseReadyNanos);
    }

    /** Starts accounting for one serialized Produce response submitted to the network. */
    public ResponseWrite responseWriteStarted(long responseBytes) {
        if (!enabled) {
            return ResponseWrite.NO_OP;
        }
        if (responseBytes < 0) {
            throw new IllegalArgumentException("responseBytes must be non-negative");
        }
        long id = responseWriteSequence.incrementAndGet();
        long startedNanos = clock.nanoseconds();
        pendingResponseWriteStarts.put(id, startedNanos);
        pendingResponseWriteBytes.addAndGet(responseBytes);
        pendingResponseWrites.incrementAndGet();
        return new ResponseWrite(this, id, startedNanos, responseBytes);
    }

    private void responseWriteCompleted(long id, long startedNanos, long responseBytes) {
        if (pendingResponseWriteStarts.remove(id, startedNanos)) {
            pendingResponseWrites.decrementAndGet();
            pendingResponseWriteBytes.addAndGet(-responseBytes);
            responseWriteCompletionTimeMicros.update(elapsedMicros(startedNanos));
        }
    }

    /** Returns the number of Produce response writes that have not reached a terminal state. */
    public long pendingResponseWrites() {
        return pendingResponseWrites.get();
    }

    /** Returns serialized Produce response bytes whose network writes remain pending. */
    public long pendingResponseWriteBytes() {
        return pendingResponseWriteBytes.get();
    }

    /** Returns the age of the oldest pending Produce response write, or zero when idle. */
    public long oldestPendingResponseWriteAgeMillis() {
        long oldestStartedNanos = Long.MAX_VALUE;
        for (Long startedNanos : pendingResponseWriteStarts.values()) {
            oldestStartedNanos = Math.min(oldestStartedNanos, startedNanos);
        }
        if (oldestStartedNanos == Long.MAX_VALUE) {
            return 0L;
        }
        return Math.max(
                0L, TimeUnit.NANOSECONDS.toMillis(clock.nanoseconds() - oldestStartedNanos));
    }

    private void update(Histogram histogram, long startedNanos) {
        if (enabled) {
            histogram.update(elapsedMicros(startedNanos));
        }
    }

    private long elapsedMicros(long startedNanos) {
        return Math.max(0L, TimeUnit.NANOSECONDS.toMicros(clock.nanoseconds() - startedNanos));
    }

    private static Counter registerMeter(MetricGroup metricGroup, String name) {
        Counter counter = new ThreadSafeSimpleCounter();
        metricGroup.meter(name, new MeterView(counter));
        return counter;
    }

    private static Histogram registerHistogram(MetricGroup metricGroup, String name) {
        return metricGroup.histogram(
                name, new DescriptiveStatisticsHistogram(HISTOGRAM_WINDOW_SIZE));
    }

    Counter requests() {
        return requests;
    }

    Counter errors() {
        return errors;
    }

    Counter failedPartitions() {
        return failedPartitions;
    }

    Counter records() {
        return records;
    }

    Counter bytesIn() {
        return bytesIn;
    }

    Counter arrowBytesOut() {
        return arrowBytesOut;
    }

    Counter inFlightRequests() {
        return inFlightRequests;
    }

    Counter arrowPoolRotations() {
        return arrowPoolRotations;
    }

    Counter arrowAcquireTimeouts() {
        return arrowAcquireTimeouts;
    }

    Counter arrowEncodeAborts() {
        return arrowEncodeAborts;
    }

    Counter arrowResourceErrors() {
        return arrowResourceErrors;
    }

    Counter kafkaConnectionRejections() {
        return kafkaConnectionRejections;
    }

    Counter reservationRejections() {
        return reservationRejections;
    }

    Counter reservationCancellations() {
        return reservationCancellations;
    }

    Counter preFrameWaitTimeouts() {
        return preFrameWaitTimeouts;
    }

    Counter bodyReadTimeouts() {
        return bodyReadTimeouts;
    }

    Counter nativeAdmissionRejections() {
        return nativeAdmissionRejections;
    }

    Counter nativeAdmissionTimeouts() {
        return nativeAdmissionTimeouts;
    }

    Counter nativeAdmissionCancellations() {
        return nativeAdmissionCancellations;
    }

    Counter nativeRequestTooLargeRejections() {
        return nativeRequestTooLargeRejections;
    }

    Counter nativeResizeFailures() {
        return nativeResizeFailures;
    }

    Counter nativeDisconnectsWithInflight() {
        return nativeDisconnectsWithInflight;
    }

    Counter nativeCompletionGraceTimeouts() {
        return nativeCompletionGraceTimeouts;
    }

    Counter nativeInvariantViolations() {
        return nativeInvariantViolations;
    }

    Histogram requestDecodeTimeMicros() {
        return requestDecodeTimeMicros;
    }

    Histogram preFrameWaitTimeMicros() {
        return preFrameWaitTimeMicros;
    }

    Histogram bodyReadTimeMicros() {
        return bodyReadTimeMicros;
    }

    Histogram nativeAdmissionWaitTimeMicros() {
        return nativeAdmissionWaitTimeMicros;
    }

    Histogram nativeEstimatedBytes() {
        return nativeEstimatedBytes;
    }

    Histogram nativeActualBytes() {
        return nativeActualBytes;
    }

    Histogram nativeEstimateErrorBytes() {
        return nativeEstimateErrorBytes;
    }

    Histogram nativeCompletionTimeMicros() {
        return nativeCompletionTimeMicros;
    }

    Histogram requestQueueTimeMicros() {
        return requestQueueTimeMicros;
    }

    Histogram recordCopyTimeMicros() {
        return recordCopyTimeMicros;
    }

    Histogram tableInfoLookupTimeMicros() {
        return tableInfoLookupTimeMicros;
    }

    Histogram tableInfoParseTimeMicros() {
        return tableInfoParseTimeMicros;
    }

    Histogram contractCompileTimeMicros() {
        return contractCompileTimeMicros;
    }

    Histogram recordConvertTimeMicros() {
        return recordConvertTimeMicros;
    }

    Histogram arrowEncodeTimeMicros() {
        return arrowEncodeTimeMicros;
    }

    Histogram arrowWriterAcquireWaitTimeMicros() {
        return arrowWriterAcquireWaitTimeMicros;
    }

    Histogram nativeProduceSubmitTimeMicros() {
        return nativeProduceSubmitTimeMicros;
    }

    Histogram acksWaitTimeMicros() {
        return acksWaitTimeMicros;
    }

    Histogram responseHeadOfLineTimeMicros() {
        return responseHeadOfLineTimeMicros;
    }

    Histogram responseWriteCompletionTimeMicros() {
        return responseWriteCompletionTimeMicros;
    }

    Histogram totalTimeMicros() {
        return totalTimeMicros;
    }

    /** Exactly-once lifecycle handle for one pending Produce response write. */
    @Internal
    public static final class ResponseWrite implements AutoCloseable {
        private static final ResponseWrite NO_OP = new ResponseWrite();

        private final @javax.annotation.Nullable KafkaProduceMetrics owner;
        private final long id;
        private final long startedNanos;
        private final long responseBytes;
        private final AtomicBoolean closed = new AtomicBoolean();

        private ResponseWrite() {
            this.owner = null;
            this.id = 0L;
            this.startedNanos = 0L;
            this.responseBytes = 0L;
        }

        private ResponseWrite(
                KafkaProduceMetrics owner, long id, long startedNanos, long responseBytes) {
            this.owner = owner;
            this.id = id;
            this.startedNanos = startedNanos;
            this.responseBytes = responseBytes;
        }

        /** Finishes response-write accounting. Repeated calls are harmless. */
        @Override
        public void close() {
            if (owner != null && closed.compareAndSet(false, true)) {
                owner.responseWriteCompleted(id, startedNanos, responseBytes);
            }
        }
    }
}
