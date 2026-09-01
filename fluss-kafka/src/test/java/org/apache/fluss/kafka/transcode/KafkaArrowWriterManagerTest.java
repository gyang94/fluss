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

package org.apache.fluss.kafka.transcode;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.TimeoutException;
import org.apache.fluss.kafka.format.KafkaDataFormat;
import org.apache.fluss.kafka.metrics.KafkaProduceMetrics;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.arrow.ArrowWriter;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KafkaArrowWriterManagerTest {

    @Test
    void testReusesWriterForSameSchema() {
        KafkaArrowWriterManager manager = manager(2, 4, Duration.ofSeconds(1));
        try {
            ArrowWriter first;
            try (KafkaArrowWriterManager.WriterLease lease = manager.acquire(tableInfo(1L, 1))) {
                first = lease.writer();
            }
            try (KafkaArrowWriterManager.WriterLease lease = manager.acquire(tableInfo(1L, 1))) {
                assertThat(lease.writer()).isSameAs(first);
            }
            assertThat(manager.activeWriterCount()).isZero();
            assertThat(manager.cachedSchemaKeyCount()).isOne();
        } finally {
            manager.close();
        }
        assertThat(manager.allocatedMemoryBytes()).isZero();
    }

    @Test
    void testSchemaKeyLimitRotatesGenerationAndClosesBorrowedWriterOnReturn() {
        KafkaArrowWriterManager manager = manager(2, 1, Duration.ofSeconds(1));
        KafkaArrowWriterManager.WriterLease oldGeneration = manager.acquire(tableInfo(1L, 1));
        ArrowWriter oldWriter = oldGeneration.writer();
        try (KafkaArrowWriterManager.WriterLease ignored = manager.acquire(tableInfo(2L, 1))) {
            assertThat(manager.cachedSchemaKeyCount()).isOne();
        }
        oldGeneration.close();

        try (KafkaArrowWriterManager.WriterLease current = manager.acquire(tableInfo(2L, 1))) {
            assertThat(current.writer()).isNotSameAs(oldWriter);
        } finally {
            manager.close();
        }
        assertThat(manager.allocatedMemoryBytes()).isZero();
    }

    @Test
    void testConcurrentWriterLimitAndAcquireTimeout() {
        KafkaArrowWriterManager manager = manager(1, 4, Duration.ofMillis(50));
        try (KafkaArrowWriterManager.WriterLease ignored = manager.acquire(tableInfo(1L, 1))) {
            assertThatThrownBy(() -> manager.acquire(tableInfo(1L, 1)))
                    .isInstanceOf(TimeoutException.class)
                    .hasMessageContaining("Timed out waiting");
            assertThat(manager.activeWriterCount()).isOne();
        } finally {
            manager.close();
        }
    }

    @Test
    void testAllocatorFailureIsRetriableAndDoesNotLeak() {
        KafkaArrowWriterManager manager =
                new KafkaArrowWriterManager(
                        1L, 1, 4, Duration.ofSeconds(1), 1 << 20, KafkaProduceMetrics.noOp());
        try {
            assertThatThrownBy(() -> manager.acquire(tableInfo(1L, 1)))
                    .isInstanceOf(TimeoutException.class)
                    .hasMessageContaining("Failed to allocate or initialize")
                    .hasCauseInstanceOf(RuntimeException.class);
            assertThat(manager.activeWriterCount()).isZero();
            assertThat(manager.allocatedMemoryBytes()).isZero();
        } finally {
            manager.close();
        }
        assertThat(manager.allocatedMemoryBytes()).isZero();
    }

    @Test
    void testCloseIsNonBlockingRejectsAcquireAndCompletesAfterReturn() {
        KafkaArrowWriterManager manager = manager(1, 4, Duration.ofSeconds(1));
        KafkaArrowWriterManager.WriterLease lease = manager.acquire(tableInfo(1L, 1));

        CompletableFuture<Void> firstClose = manager.closeAsync();
        CompletableFuture<Void> secondClose = manager.closeAsync();
        assertThat(secondClose).isSameAs(firstClose).isNotDone();
        assertThat(manager.isClosed()).isTrue();
        assertThatThrownBy(() -> manager.acquire(tableInfo(1L, 1)))
                .isInstanceOf(FlussRuntimeException.class)
                .hasMessageContaining("closing or closed");

        lease.close();
        firstClose.join();
        assertThat(manager.activeWriterCount()).isZero();
        assertThat(manager.allocatedMemoryBytes()).isZero();
        manager.close();
    }

    @Test
    void testWaitingAcquireObservesCloseWithoutDeadlock() throws Exception {
        KafkaArrowWriterManager manager = manager(1, 4, Duration.ofSeconds(10));
        KafkaArrowWriterManager.WriterLease lease = manager.acquire(tableInfo(1L, 1));
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<KafkaArrowWriterManager.WriterLease> waiting =
                    executor.submit(() -> manager.acquire(tableInfo(1L, 1)));
            awaitWaiter(manager);
            CompletableFuture<Void> closeFuture = manager.closeAsync();

            assertThatThrownBy(() -> waiting.get(2, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .cause()
                    .isInstanceOf(FlussRuntimeException.class);
            lease.close();
            closeFuture.get(2, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
            if (!manager.isClosed()) {
                lease.close();
                manager.close();
            }
        }
    }

    private static void awaitWaiter(KafkaArrowWriterManager manager) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (manager.waiterCount() == 0 && System.nanoTime() < deadline) {
            Thread.yield();
        }
        assertThat(manager.waiterCount()).isOne();
    }

    private static KafkaArrowWriterManager manager(
            int maxConcurrentWriters, int maxSchemaKeys, Duration timeout) {
        return new KafkaArrowWriterManager(
                16L << 20,
                maxConcurrentWriters,
                maxSchemaKeys,
                timeout,
                1 << 20,
                KafkaProduceMetrics.noOp());
    }

    private static TableInfo tableInfo(long tableId, int schemaId) {
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(Schema.newBuilder().column("payload", DataTypes.BYTES()).build())
                        .distributedBy(1)
                        .property(ConfigOptions.TABLE_LOG_FORMAT, LogFormat.ARROW)
                        .customProperty(
                                KafkaDataFormat.VALUE_FORMAT_CONFIG, KafkaDataFormat.RAW.value())
                        .build();
        return TableInfo.of(
                TablePath.of("kafka", "topic_" + tableId),
                tableId,
                schemaId,
                descriptor,
                null,
                1L,
                1L);
    }
}
