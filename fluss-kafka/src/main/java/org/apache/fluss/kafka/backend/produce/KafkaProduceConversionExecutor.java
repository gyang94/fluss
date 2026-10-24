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
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;
import org.apache.fluss.utils.concurrent.FutureUtils;

import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Bounded workers that serialize conversion and append submission for each ordering key.
 *
 * <p>An operation must submit its native append before returning its acknowledgement future. The
 * next operation for that key can then start without waiting for the acknowledgement. Ordering keys
 * must have stable equality and hash codes for the lifetime of their tasks.
 */
@Internal
public final class KafkaProduceConversionExecutor {
    private final ThreadPoolExecutor workers;
    private final long maximumPendingTasks;
    private final Object submissionLock = new Object();

    @GuardedBy("submissionLock")
    private final Map<Object, Deque<ConversionTask<?>>> tasksByKey = new HashMap<>();

    @GuardedBy("submissionLock")
    private int pendingTasks;

    @GuardedBy("submissionLock")
    private boolean closed;

    @GuardedBy("submissionLock")
    private CompletableFuture<Void> closeFuture;

    /** Creates shared workers with a bound covering both runnable and per-key queued tasks. */
    public KafkaProduceConversionExecutor(int threads, int queueCapacity) {
        checkArgument(threads > 0, "Conversion thread count must be positive.");
        checkArgument(
                queueCapacity >= threads, "Queue capacity must be at least the thread count.");
        maximumPendingTasks = (long) threads + queueCapacity;
        workers =
                new ThreadPoolExecutor(
                        threads,
                        threads,
                        0L,
                        TimeUnit.MILLISECONDS,
                        // Admission bounds all accepted tasks, including those waiting on a key.
                        new LinkedBlockingQueue<>(),
                        new ExecutorThreadFactory("kafka-produce-conversion"),
                        new ThreadPoolExecutor.AbortPolicy());
    }

    /** Submits conversion without running rejected work on the caller's thread. */
    public <T> CompletableFuture<T> submit(
            Object orderingKey, Supplier<CompletableFuture<T>> operation) {
        checkNotNull(orderingKey);
        checkNotNull(operation);
        synchronized (submissionLock) {
            if (closed || pendingTasks >= maximumPendingTasks) {
                return FutureUtils.completedExceptionally(
                        new RejectedExecutionException(
                                closed
                                        ? "Kafka conversion executor is closed."
                                        : "Kafka conversion queue is full."));
            }
            ConversionTask<T> task = new ConversionTask<>(orderingKey, operation);
            Deque<ConversionTask<?>> tasks =
                    tasksByKey.computeIfAbsent(orderingKey, ignored -> new ArrayDeque<>());
            tasks.addLast(task);
            pendingTasks++;
            if (tasks.size() == 1) {
                workers.execute(task);
            }
            return task.result;
        }
    }

    /** Stops admission, rejects queued tasks, interrupts running tasks and awaits worker exit. */
    public CompletableFuture<Void> closeAsync() {
        List<ConversionTask<?>> queued = new ArrayList<>();
        CompletableFuture<Void> completion;
        synchronized (submissionLock) {
            if (closeFuture != null) {
                return closeFuture;
            }
            closeFuture = new CompletableFuture<>();
            completion = closeFuture;
            closed = true;
            Iterator<Deque<ConversionTask<?>>> queues = tasksByKey.values().iterator();
            while (queues.hasNext()) {
                Deque<ConversionTask<?>> tasks = queues.next();
                Iterator<ConversionTask<?>> iterator = tasks.iterator();
                while (iterator.hasNext()) {
                    ConversionTask<?> task = iterator.next();
                    if (!task.started) {
                        task.operation = null;
                        queued.add(task);
                        iterator.remove();
                        pendingTasks--;
                    }
                }
                if (tasks.isEmpty()) {
                    queues.remove();
                }
            }
            workers.shutdownNow();
        }
        RejectedExecutionException failure =
                new RejectedExecutionException("Kafka conversion executor is shutting down.");
        for (ConversionTask<?> task : queued) {
            task.result.completeExceptionally(failure);
        }
        CompletableFuture.runAsync(
                        () -> {
                            try {
                                if (!workers.awaitTermination(10, TimeUnit.SECONDS)) {
                                    throw new IllegalStateException(
                                            "Kafka conversion workers did not terminate.");
                                }
                            } catch (InterruptedException interrupted) {
                                Thread.currentThread().interrupt();
                                throw new IllegalStateException(
                                        "Interrupted closing Kafka conversion workers.",
                                        interrupted);
                            }
                        })
                .whenComplete(
                        (ignored, error) -> {
                            if (error == null) {
                                completion.complete(null);
                            } else {
                                completion.completeExceptionally(error);
                            }
                        });
        return completion;
    }

    private void finishTask(Object orderingKey) {
        synchronized (submissionLock) {
            Deque<ConversionTask<?>> tasks = tasksByKey.get(orderingKey);
            tasks.removeFirst();
            pendingTasks--;
            if (tasks.isEmpty()) {
                tasksByKey.remove(orderingKey);
            } else {
                workers.execute(tasks.peekFirst());
            }
        }
    }

    private final class ConversionTask<T> implements Runnable {
        private final Object orderingKey;
        private final CompletableFuture<T> result = new CompletableFuture<>();

        @GuardedBy("submissionLock")
        private Supplier<CompletableFuture<T>> operation;

        @GuardedBy("submissionLock")
        private boolean started;

        private ConversionTask(Object orderingKey, Supplier<CompletableFuture<T>> operation) {
            this.orderingKey = orderingKey;
            this.operation = operation;
        }

        @Override
        public void run() {
            Supplier<CompletableFuture<T>> action;
            synchronized (submissionLock) {
                if (closed) {
                    return;
                }
                started = true;
                action = operation;
                operation = null;
            }
            CompletableFuture<T> acknowledgement;
            try {
                acknowledgement =
                        result.isDone() ? null : checkNotNull(action.get(), "conversion result");
            } catch (Throwable failure) {
                acknowledgement = FutureUtils.completedExceptionally(failure);
            } finally {
                finishTask(orderingKey);
            }
            if (acknowledgement != null) {
                acknowledgement.whenComplete(
                        (value, failure) -> {
                            if (failure == null) {
                                result.complete(value);
                            } else {
                                result.completeExceptionally(failure);
                            }
                        });
            }
        }
    }
}
