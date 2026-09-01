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

import org.apache.fluss.kafka.admission.KafkaNativeProduceAdmissionController;
import org.apache.fluss.kafka.admission.KafkaNativeProduceAdmissionController.ConnectionHandle;
import org.apache.fluss.kafka.admission.KafkaNativeProduceAdmissionController.RequestLease;
import org.apache.fluss.kafka.admission.KafkaNativeProduceAdmissionController.Reservation;
import org.apache.fluss.kafka.admission.KafkaProduceAdmissionController;
import org.apache.fluss.metrics.Gauge;
import org.apache.fluss.metrics.Histogram;
import org.apache.fluss.metrics.util.TestMetricGroup;
import org.apache.fluss.utils.clock.Clock;
import org.apache.fluss.utils.clock.ManualClock;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaProduceMetricsTest {

    @Test
    void testNowNanosFallsBackWhenClockFails() {
        KafkaProduceMetrics metrics =
                new KafkaProduceMetrics(
                        TestMetricGroup.newBuilder().build(),
                        new Clock() {
                            @Override
                            public long milliseconds() {
                                return 0L;
                            }

                            @Override
                            public long nanoseconds() {
                                throw new IllegalStateException("clock unavailable");
                            }
                        });

        long before = System.nanoTime();
        long now = metrics.nowNanos();
        long after = System.nanoTime();

        assertThat(now).isBetween(before, after);
    }

    @Test
    void testRequestLifecycleMetrics() {
        ManualClock clock = new ManualClock();
        KafkaProduceMetrics metrics = createMetrics(clock);
        long receivedNanos = metrics.nowNanos();

        clock.advanceTime(2, TimeUnit.MILLISECONDS);
        metrics.recordRequestDecode(receivedNanos);
        metrics.requestStarted(receivedNanos, 512);

        assertThat(metrics.requests().getCount()).isOne();
        assertThat(metrics.bytesIn().getCount()).isEqualTo(512);
        assertThat(metrics.inFlightRequests().getCount()).isOne();
        assertThat(onlyValue(metrics.requestDecodeTimeMicros())).isEqualTo(2_000);
        assertThat(onlyValue(metrics.requestQueueTimeMicros())).isEqualTo(2_000);

        clock.advanceTime(3, TimeUnit.MILLISECONDS);
        metrics.requestCompleted(receivedNanos, true, 2);

        assertThat(metrics.errors().getCount()).isOne();
        assertThat(metrics.failedPartitions().getCount()).isEqualTo(2);
        assertThat(metrics.inFlightRequests().getCount()).isZero();
        assertThat(onlyValue(metrics.totalTimeMicros())).isEqualTo(5_000);
    }

    @Test
    void testComponentStageMetrics() {
        ManualClock clock = new ManualClock();
        KafkaProduceMetrics metrics = createMetrics(clock);

        assertStage(clock, metrics, metrics::recordTableInfoLookup, 11);
        assertThat(onlyValue(metrics.tableInfoLookupTimeMicros())).isEqualTo(11);

        assertStage(clock, metrics, metrics::recordTableInfoParse, 12);
        assertThat(onlyValue(metrics.tableInfoParseTimeMicros())).isEqualTo(12);

        assertStage(clock, metrics, metrics::recordContractCompile, 13);
        assertThat(onlyValue(metrics.contractCompileTimeMicros())).isEqualTo(13);

        assertStage(clock, metrics, metrics::recordDecodeAssemble, 14);
        assertThat(onlyValue(metrics.recordConvertTimeMicros())).isEqualTo(14);

        long arrowStartedNanos = metrics.nowNanos();
        clock.advanceTime(15, TimeUnit.MICROSECONDS);
        metrics.recordArrowEncode(arrowStartedNanos, 1_024);
        assertThat(onlyValue(metrics.arrowEncodeTimeMicros())).isEqualTo(15);
        assertThat(metrics.arrowBytesOut().getCount()).isEqualTo(1_024);

        assertStage(clock, metrics, metrics::recordNativeProduceSubmit, 16);
        assertThat(onlyValue(metrics.nativeProduceSubmitTimeMicros())).isEqualTo(16);

        assertStage(clock, metrics, metrics::recordAcksWait, 17);
        assertThat(onlyValue(metrics.acksWaitTimeMicros())).isEqualTo(17);

        assertStage(clock, metrics, metrics::recordResponseHeadOfLine, 18);
        assertThat(onlyValue(metrics.responseHeadOfLineTimeMicros())).isEqualTo(18);

        assertStage(clock, metrics, metrics::recordArrowWriterAcquireWait, 19);
        assertThat(onlyValue(metrics.arrowWriterAcquireWaitTimeMicros())).isEqualTo(19);

        metrics.recordArrowPoolRotation();
        metrics.recordArrowAcquireTimeout();
        metrics.recordArrowEncodeAbort();
        metrics.recordArrowResourceError();
        assertThat(metrics.arrowPoolRotations().getCount()).isOne();
        assertThat(metrics.arrowAcquireTimeouts().getCount()).isOne();
        assertThat(metrics.arrowEncodeAborts().getCount()).isOne();
        assertThat(metrics.arrowResourceErrors().getCount()).isOne();

        long copyStartedNanos = metrics.nowNanos();
        clock.advanceTime(20, TimeUnit.MICROSECONDS);
        metrics.recordRecordCopy(copyStartedNanos, 100, 3);
        assertThat(onlyValue(metrics.recordCopyTimeMicros())).isEqualTo(20);
        assertThat(metrics.records().getCount()).isEqualTo(100);
    }

    @Test
    void testNoOpMetricsAcceptAllUpdates() {
        KafkaProduceMetrics metrics = KafkaProduceMetrics.noOp();
        long startedNanos = metrics.nowNanos();

        metrics.recordRequestDecode(startedNanos);
        metrics.requestStarted(startedNanos, 1);
        metrics.recordRecordCopy(startedNanos, 1, 1);
        metrics.recordTableInfoLookup(startedNanos);
        metrics.recordTableInfoParse(startedNanos);
        metrics.recordContractCompile(startedNanos);
        metrics.recordDecodeAssemble(startedNanos);
        metrics.recordArrowEncode(startedNanos, 1);
        metrics.registerArrowResourceGauges(() -> 0L, () -> 0, () -> 0, () -> 0);
        metrics.registerAdmissionGauges(null);
        metrics.registerNativeAdmissionGauges(null);
        metrics.registerControlAdmissionGauges(null, null, null, null, null, null, null);
        metrics.registerKafkaConnectionGauges(null, null);
        metrics.recordKafkaConnectionRejected();
        metrics.recordReservationRejected();
        metrics.recordReservationCancelled();
        metrics.recordPreFrameWait(startedNanos);
        metrics.recordPreFrameWaitTimeout(startedNanos);
        metrics.recordBodyRead(startedNanos);
        metrics.recordBodyReadTimeout(startedNanos);
        metrics.recordNativeAdmissionGranted(startedNanos, 1);
        metrics.recordNativeAdmissionRejected();
        metrics.recordNativeAdmissionCancelled();
        metrics.recordNativeAdmissionTimeout(startedNanos);
        metrics.recordNativeRequestTooLarge();
        metrics.recordNativeResize(1, 1, false);
        metrics.recordNativeDisconnectWithInflight();
        metrics.recordNativeCompletionGraceTimeout();
        metrics.recordNativeInvariantViolation();
        metrics.recordNativeCompletion(startedNanos);
        metrics.recordArrowWriterAcquireWait(startedNanos);
        metrics.recordArrowPoolRotation();
        metrics.recordArrowAcquireTimeout();
        metrics.recordArrowEncodeAbort();
        metrics.recordArrowResourceError();
        metrics.recordNativeProduceSubmit(startedNanos);
        metrics.recordAcksWait(startedNanos);
        metrics.recordResponseHeadOfLine(startedNanos);
        metrics.responseWriteStarted(1).close();
        metrics.requestCompleted(startedNanos, false, 0);
    }

    @Test
    void testResponseWriteLifecycleMetrics() {
        ManualClock clock = new ManualClock();
        CapturingMetricGroup metricGroup = new CapturingMetricGroup();
        KafkaProduceMetrics metrics = new KafkaProduceMetrics(metricGroup, clock);

        KafkaProduceMetrics.ResponseWrite first = metrics.responseWriteStarted(100);
        clock.advanceTime(10, TimeUnit.MILLISECONDS);
        KafkaProduceMetrics.ResponseWrite second = metrics.responseWriteStarted(20);

        assertThat(metricGroup.gauges)
                .containsKeys(
                        KafkaMetricNames.PENDING_RESPONSE_WRITES,
                        KafkaMetricNames.PENDING_RESPONSE_WRITE_BYTES,
                        KafkaMetricNames.OLDEST_PENDING_RESPONSE_WRITE_AGE_MILLIS);
        assertThat(metricGroup.value(KafkaMetricNames.PENDING_RESPONSE_WRITES)).isEqualTo(2L);
        assertThat(metricGroup.value(KafkaMetricNames.PENDING_RESPONSE_WRITE_BYTES))
                .isEqualTo(120L);
        assertThat(metricGroup.value(KafkaMetricNames.OLDEST_PENDING_RESPONSE_WRITE_AGE_MILLIS))
                .isEqualTo(10L);

        first.close();
        first.close();
        assertThat(metrics.pendingResponseWrites()).isOne();
        assertThat(metrics.pendingResponseWriteBytes()).isEqualTo(20L);
        assertThat(metrics.responseWriteCompletionTimeMicros().getCount()).isOne();

        clock.advanceTime(5, TimeUnit.MILLISECONDS);
        assertThat(metrics.oldestPendingResponseWriteAgeMillis()).isEqualTo(5L);
        second.close();

        assertThat(metrics.pendingResponseWrites()).isZero();
        assertThat(metrics.pendingResponseWriteBytes()).isZero();
        assertThat(metrics.oldestPendingResponseWriteAgeMillis()).isZero();
        assertThat(metrics.responseWriteCompletionTimeMicros().getCount()).isEqualTo(2L);
    }

    @Test
    void testAdmissionGaugesExposeCurrentControllerValues() {
        CapturingMetricGroup metricGroup = new CapturingMetricGroup();
        KafkaProduceMetrics metrics = new KafkaProduceMetrics(metricGroup);
        KafkaProduceAdmissionController admissionController =
                new KafkaProduceAdmissionController(10, 1_000, 5, 500);

        metrics.registerAdmissionGauges(admissionController);

        assertThat(metricGroup.gauges)
                .containsKeys(
                        KafkaMetricNames.LIVE_REQUESTS,
                        KafkaMetricNames.LIVE_BYTES,
                        KafkaMetricNames.RAW_BYTES,
                        KafkaMetricNames.PRODUCE_CONNECTIONS,
                        KafkaMetricNames.PAUSED_CONNECTIONS,
                        KafkaMetricNames.PRODUCE_RESERVATION_WAITERS,
                        KafkaMetricNames.MAX_CONNECTION_LIVE_REQUESTS,
                        KafkaMetricNames.MAX_CONNECTION_RAW_BYTES,
                        KafkaMetricNames.LIVE_REQUEST_LIMIT,
                        KafkaMetricNames.RAW_BYTES_LIMIT,
                        KafkaMetricNames.CONNECTION_LIVE_REQUEST_LIMIT,
                        KafkaMetricNames.CONNECTION_RAW_BYTES_LIMIT,
                        KafkaMetricNames.LIVE_REQUEST_PAUSE_EVENTS,
                        KafkaMetricNames.RAW_BYTES_PAUSE_EVENTS,
                        KafkaMetricNames.LIVE_REQUEST_OVERSHOOT_EVENTS,
                        KafkaMetricNames.RAW_BYTES_OVERSHOOT_EVENTS,
                        KafkaMetricNames.MAX_LIVE_REQUEST_OVERSHOOT,
                        KafkaMetricNames.MAX_RAW_BYTES_OVERSHOOT,
                        KafkaMetricNames.CUMULATIVE_LIVE_REQUEST_PAUSE_TIME_MICROS,
                        KafkaMetricNames.CUMULATIVE_RAW_BYTES_PAUSE_TIME_MICROS,
                        KafkaMetricNames.LONGEST_ACTIVE_LIVE_REQUEST_PAUSE_TIME_MILLIS,
                        KafkaMetricNames.LONGEST_ACTIVE_RAW_BYTES_PAUSE_TIME_MILLIS);
        assertThat(metricGroup.value(KafkaMetricNames.LIVE_REQUESTS)).isEqualTo(0L);
        assertThat(metricGroup.value(KafkaMetricNames.LIVE_REQUEST_LIMIT)).isEqualTo(10L);
        assertThat(metricGroup.value(KafkaMetricNames.RAW_BYTES_LIMIT)).isEqualTo(1_000L);
        assertThat(metricGroup.value(KafkaMetricNames.CONNECTION_LIVE_REQUEST_LIMIT)).isEqualTo(5L);
        assertThat(metricGroup.value(KafkaMetricNames.CONNECTION_RAW_BYTES_LIMIT)).isEqualTo(500L);
        assertThat(metricGroup.value(KafkaMetricNames.PRODUCE_RESERVATION_WAITERS)).isEqualTo(0);

        admissionController.updateLimits(20, 2_000, 7, 700);

        assertThat(metricGroup.value(KafkaMetricNames.LIVE_REQUEST_LIMIT)).isEqualTo(20L);
        assertThat(metricGroup.value(KafkaMetricNames.RAW_BYTES_LIMIT)).isEqualTo(2_000L);
        assertThat(metricGroup.value(KafkaMetricNames.CONNECTION_LIVE_REQUEST_LIMIT)).isEqualTo(7L);
        assertThat(metricGroup.value(KafkaMetricNames.CONNECTION_RAW_BYTES_LIMIT)).isEqualTo(700L);
    }

    @Test
    void testPreFrameEventAndLatencyMetrics() {
        ManualClock clock = new ManualClock();
        KafkaProduceMetrics metrics = createMetrics(clock);

        metrics.recordKafkaConnectionRejected();
        metrics.recordReservationRejected();
        metrics.recordReservationCancelled();
        assertThat(metrics.kafkaConnectionRejections().getCount()).isOne();
        assertThat(metrics.reservationRejections().getCount()).isOne();
        assertThat(metrics.reservationCancellations().getCount()).isOne();

        long waitStartedNanos = metrics.nowNanos();
        clock.advanceTime(11, TimeUnit.MICROSECONDS);
        metrics.recordPreFrameWait(waitStartedNanos);
        assertThat(onlyValue(metrics.preFrameWaitTimeMicros())).isEqualTo(11);

        long readStartedNanos = metrics.nowNanos();
        clock.advanceTime(12, TimeUnit.MICROSECONDS);
        metrics.recordBodyRead(readStartedNanos);
        assertThat(onlyValue(metrics.bodyReadTimeMicros())).isEqualTo(12);

        long timedOutWaitStartedNanos = metrics.nowNanos();
        clock.advanceTime(13, TimeUnit.MICROSECONDS);
        metrics.recordPreFrameWaitTimeout(timedOutWaitStartedNanos);
        assertThat(metrics.preFrameWaitTimeouts().getCount()).isOne();
        assertThat(metrics.preFrameWaitTimeMicros().getCount()).isEqualTo(2);

        long timedOutReadStartedNanos = metrics.nowNanos();
        clock.advanceTime(14, TimeUnit.MICROSECONDS);
        metrics.recordBodyReadTimeout(timedOutReadStartedNanos);
        assertThat(metrics.bodyReadTimeouts().getCount()).isOne();
        assertThat(metrics.bodyReadTimeMicros().getCount()).isEqualTo(2);
    }

    @Test
    void testControlAdmissionAndConnectionGaugesFollowSuppliers() {
        CapturingMetricGroup metricGroup = new CapturingMetricGroup();
        KafkaProduceMetrics metrics = new KafkaProduceMetrics(metricGroup);
        AtomicInteger controlWaiters = new AtomicInteger(2);
        AtomicLong controlLive = new AtomicLong(3);
        AtomicLong controlRaw = new AtomicLong(4);
        AtomicInteger connections = new AtomicInteger(5);

        metrics.registerControlAdmissionGauges(
                controlWaiters::get,
                controlLive::get,
                controlRaw::get,
                () -> 128L,
                () -> 128L << 20,
                () -> 8L,
                () -> 100L << 20);
        metrics.registerKafkaConnectionGauges(connections::get, () -> 10_000);

        assertThat(metricGroup.gauges)
                .containsKeys(
                        KafkaMetricNames.CONTROL_RESERVATION_WAITERS,
                        KafkaMetricNames.CONTROL_LIVE_REQUESTS,
                        KafkaMetricNames.CONTROL_RAW_BYTES,
                        KafkaMetricNames.CONTROL_LIVE_REQUEST_LIMIT,
                        KafkaMetricNames.CONTROL_RAW_BYTES_LIMIT,
                        KafkaMetricNames.CONTROL_CONNECTION_LIVE_REQUEST_LIMIT,
                        KafkaMetricNames.CONTROL_CONNECTION_RAW_BYTES_LIMIT,
                        KafkaMetricNames.KAFKA_CONNECTIONS,
                        KafkaMetricNames.KAFKA_CONNECTION_LIMIT);
        assertThat(metricGroup.value(KafkaMetricNames.CONTROL_RESERVATION_WAITERS)).isEqualTo(2);
        assertThat(metricGroup.value(KafkaMetricNames.CONTROL_LIVE_REQUESTS)).isEqualTo(3L);
        assertThat(metricGroup.value(KafkaMetricNames.CONTROL_RAW_BYTES)).isEqualTo(4L);
        assertThat(metricGroup.value(KafkaMetricNames.CONTROL_LIVE_REQUEST_LIMIT)).isEqualTo(128L);
        assertThat(metricGroup.value(KafkaMetricNames.CONTROL_RAW_BYTES_LIMIT))
                .isEqualTo(128L << 20);
        assertThat(metricGroup.value(KafkaMetricNames.CONTROL_CONNECTION_LIVE_REQUEST_LIMIT))
                .isEqualTo(8L);
        assertThat(metricGroup.value(KafkaMetricNames.CONTROL_CONNECTION_RAW_BYTES_LIMIT))
                .isEqualTo(100L << 20);
        assertThat(metricGroup.value(KafkaMetricNames.KAFKA_CONNECTIONS)).isEqualTo(5);
        assertThat(metricGroup.value(KafkaMetricNames.KAFKA_CONNECTION_LIMIT)).isEqualTo(10_000);

        controlWaiters.set(6);
        controlLive.set(7);
        controlRaw.set(8);
        connections.set(9);
        assertThat(metricGroup.value(KafkaMetricNames.CONTROL_RESERVATION_WAITERS)).isEqualTo(6);
        assertThat(metricGroup.value(KafkaMetricNames.CONTROL_LIVE_REQUESTS)).isEqualTo(7L);
        assertThat(metricGroup.value(KafkaMetricNames.CONTROL_RAW_BYTES)).isEqualTo(8L);
        assertThat(metricGroup.value(KafkaMetricNames.KAFKA_CONNECTIONS)).isEqualTo(9);
    }

    @Test
    void testNativeAdmissionGaugesExposeControllerLifecycle() {
        CapturingMetricGroup metricGroup = new CapturingMetricGroup();
        KafkaProduceMetrics metrics = new KafkaProduceMetrics(metricGroup);
        KafkaNativeProduceAdmissionController controller =
                new KafkaNativeProduceAdmissionController(1, 1_000, 1, 500, 20);

        metrics.registerNativeAdmissionGauges(controller);

        assertThat(metricGroup.gauges)
                .containsKeys(
                        KafkaMetricNames.NATIVE_IN_FLIGHT_REQUESTS,
                        KafkaMetricNames.NATIVE_CONVERTED_BYTES,
                        KafkaMetricNames.NATIVE_PENDING_RESERVED_BYTES,
                        KafkaMetricNames.NATIVE_TOTAL_RESERVED_BYTES,
                        KafkaMetricNames.NATIVE_ADMISSION_WAITERS,
                        KafkaMetricNames.NATIVE_ADMISSION_CONNECTIONS,
                        KafkaMetricNames.MAX_CONNECTION_NATIVE_IN_FLIGHT_REQUESTS,
                        KafkaMetricNames.MAX_CONNECTION_NATIVE_CONVERTED_BYTES,
                        KafkaMetricNames.MAX_CONNECTION_NATIVE_PENDING_RESERVED_BYTES,
                        KafkaMetricNames.MAX_CONNECTION_NATIVE_TOTAL_RESERVED_BYTES,
                        KafkaMetricNames.NATIVE_IN_FLIGHT_REQUEST_LIMIT,
                        KafkaMetricNames.NATIVE_CONVERTED_BYTES_LIMIT,
                        KafkaMetricNames.NATIVE_CONNECTION_IN_FLIGHT_REQUEST_LIMIT,
                        KafkaMetricNames.NATIVE_CONNECTION_CONVERTED_BYTES_LIMIT,
                        KafkaMetricNames.NATIVE_PENDING_RESERVATION_LIMIT);
        assertThat(metricGroup.value(KafkaMetricNames.NATIVE_IN_FLIGHT_REQUEST_LIMIT))
                .isEqualTo(1L);
        assertThat(metricGroup.value(KafkaMetricNames.NATIVE_CONVERTED_BYTES_LIMIT))
                .isEqualTo(1_000L);
        assertThat(metricGroup.value(KafkaMetricNames.NATIVE_CONNECTION_IN_FLIGHT_REQUEST_LIMIT))
                .isEqualTo(1L);
        assertThat(metricGroup.value(KafkaMetricNames.NATIVE_CONNECTION_CONVERTED_BYTES_LIMIT))
                .isEqualTo(500L);
        assertThat(metricGroup.value(KafkaMetricNames.NATIVE_PENDING_RESERVATION_LIMIT))
                .isEqualTo(20);

        try (ConnectionHandle connection = controller.registerConnection()) {
            RequestLease lease = connection.reserve(100).getFuture().join();
            Reservation waiting = connection.reserve(50);
            assertThat(metricGroup.value(KafkaMetricNames.NATIVE_IN_FLIGHT_REQUESTS)).isEqualTo(1L);
            assertThat(metricGroup.value(KafkaMetricNames.NATIVE_CONVERTED_BYTES)).isEqualTo(100L);
            assertThat(metricGroup.value(KafkaMetricNames.NATIVE_PENDING_RESERVED_BYTES))
                    .isEqualTo(50L);
            assertThat(metricGroup.value(KafkaMetricNames.NATIVE_TOTAL_RESERVED_BYTES))
                    .isEqualTo(150L);
            assertThat(metricGroup.value(KafkaMetricNames.NATIVE_ADMISSION_CONNECTIONS))
                    .isEqualTo(1);
            assertThat(metricGroup.value(KafkaMetricNames.MAX_CONNECTION_NATIVE_IN_FLIGHT_REQUESTS))
                    .isEqualTo(1L);
            assertThat(metricGroup.value(KafkaMetricNames.MAX_CONNECTION_NATIVE_CONVERTED_BYTES))
                    .isEqualTo(100L);
            assertThat(
                            metricGroup.value(
                                    KafkaMetricNames.MAX_CONNECTION_NATIVE_PENDING_RESERVED_BYTES))
                    .isEqualTo(50L);
            assertThat(
                            metricGroup.value(
                                    KafkaMetricNames.MAX_CONNECTION_NATIVE_TOTAL_RESERVED_BYTES))
                    .isEqualTo(150L);
            waiting.cancel();
            lease.close();
        }

        assertThat(metricGroup.value(KafkaMetricNames.NATIVE_IN_FLIGHT_REQUESTS)).isEqualTo(0L);
        assertThat(metricGroup.value(KafkaMetricNames.NATIVE_CONVERTED_BYTES)).isEqualTo(0L);
        assertThat(metricGroup.value(KafkaMetricNames.NATIVE_PENDING_RESERVED_BYTES)).isEqualTo(0L);
        assertThat(metricGroup.value(KafkaMetricNames.NATIVE_TOTAL_RESERVED_BYTES)).isEqualTo(0L);
        assertThat(metricGroup.value(KafkaMetricNames.NATIVE_ADMISSION_CONNECTIONS)).isEqualTo(0);
    }

    @Test
    void testNativeAdmissionOutcomeAndSizeMetrics() {
        ManualClock clock = new ManualClock();
        KafkaProduceMetrics metrics = createMetrics(clock);
        long waitStartedNanos = metrics.nowNanos();
        clock.advanceTime(11, TimeUnit.MICROSECONDS);
        metrics.recordNativeAdmissionGranted(waitStartedNanos, 100);

        assertThat(onlyValue(metrics.nativeAdmissionWaitTimeMicros())).isEqualTo(11);
        assertThat(onlyValue(metrics.nativeEstimatedBytes())).isEqualTo(100);

        metrics.recordNativeResize(100, 80, true);
        assertThat(onlyValue(metrics.nativeActualBytes())).isEqualTo(80);
        assertThat(onlyValue(metrics.nativeEstimateErrorBytes())).isEqualTo(-20);
        assertThat(metrics.nativeResizeFailures().getCount()).isZero();

        metrics.recordNativeResize(100, 120, false);
        assertThat(metrics.nativeActualBytes().getCount()).isEqualTo(2);
        assertThat(metrics.nativeEstimateErrorBytes().getCount()).isEqualTo(2);
        assertThat(metrics.nativeResizeFailures().getCount()).isOne();

        long timeoutStartedNanos = metrics.nowNanos();
        clock.advanceTime(12, TimeUnit.MICROSECONDS);
        metrics.recordNativeAdmissionTimeout(timeoutStartedNanos);
        assertThat(metrics.nativeAdmissionTimeouts().getCount()).isOne();
        assertThat(metrics.nativeAdmissionWaitTimeMicros().getCount()).isEqualTo(2);

        long completionStartedNanos = metrics.nowNanos();
        clock.advanceTime(13, TimeUnit.MICROSECONDS);
        metrics.recordNativeCompletion(completionStartedNanos);
        assertThat(onlyValue(metrics.nativeCompletionTimeMicros())).isEqualTo(13);

        metrics.recordNativeAdmissionRejected();
        metrics.recordNativeAdmissionCancelled();
        metrics.recordNativeRequestTooLarge();
        metrics.recordNativeDisconnectWithInflight();
        metrics.recordNativeCompletionGraceTimeout();
        metrics.recordNativeInvariantViolation();
        assertThat(metrics.nativeAdmissionRejections().getCount()).isOne();
        assertThat(metrics.nativeAdmissionCancellations().getCount()).isOne();
        assertThat(metrics.nativeRequestTooLargeRejections().getCount()).isOne();
        assertThat(metrics.nativeDisconnectsWithInflight().getCount()).isOne();
        assertThat(metrics.nativeCompletionGraceTimeouts().getCount()).isOne();
        assertThat(metrics.nativeInvariantViolations().getCount()).isOne();
    }

    private static KafkaProduceMetrics createMetrics(ManualClock clock) {
        return new KafkaProduceMetrics(TestMetricGroup.newBuilder().build(), clock);
    }

    private static void assertStage(
            ManualClock clock, KafkaProduceMetrics metrics, StageRecorder recorder, long micros) {
        long startedNanos = metrics.nowNanos();
        clock.advanceTime(micros, TimeUnit.MICROSECONDS);
        recorder.record(startedNanos);
    }

    private static long onlyValue(Histogram histogram) {
        assertThat(histogram.getCount()).isOne();
        return histogram.getStatistics().getValues()[0];
    }

    private interface StageRecorder {
        void record(long startedNanos);
    }

    private static final class CapturingMetricGroup extends TestMetricGroup {
        private final Map<String, Gauge<?>> gauges = new HashMap<>();

        private CapturingMetricGroup() {
            super(
                    new String[0],
                    Collections.emptyMap(),
                    (name, filter) -> name,
                    (filter, delimiter) -> "kafka.request.produce");
        }

        @Override
        public <T, G extends Gauge<T>> G gauge(String name, G gauge) {
            gauges.put(name, gauge);
            return gauge;
        }

        private Object value(String name) {
            return gauges.get(name).getValue();
        }
    }
}
