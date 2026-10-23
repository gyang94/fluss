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

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Shared, bounded workers for Kafka conversion, with ordered native append submission.
 *
 * <p>Tasks with the same ordering key may convert concurrently, but must call {@link
 * #awaitAppendTurn()} before invoking native append. An operation must submit its append before
 * returning, but need not wait for acknowledgements. Ordering keys must have stable equality and
 * hash codes for the lifetime of their tasks.
 */
@Internal
public final class KafkaProduceConversionExecutor {
    private final ThreadPoolExecutor workers;
    private final Object submissionLock = new Object();
    private final Map<Object, CompletableFuture<Void>> lastSubmitted = new HashMap<>();
    private final ThreadLocal<ConversionTask<?>> currentTask = new ThreadLocal<>();
    private volatile boolean closed;
    private CompletableFuture<Void> closeFuture;

    /** Creates a fixed shared worker pool with a bounded conversion queue. */
    public KafkaProduceConversionExecutor(int threads, int queueCapacity) {
        checkArgument(threads > 0, "Conversion thread count must be positive.");
        checkArgument(
                queueCapacity >= threads, "Queue capacity must be at least the thread count.");
        workers =
                new ThreadPoolExecutor(
                        threads,
                        threads,
                        0L,
                        TimeUnit.MILLISECONDS,
                        new ArrayBlockingQueue<>(queueCapacity),
                        new ExecutorThreadFactory("kafka-produce-conversion"),
                        new ThreadPoolExecutor.AbortPolicy());
    }

    /** Submits conversion without ever running rejected work on the caller's thread. */
    public <T> CompletableFuture<T> submit(
            Object orderingKey, Supplier<CompletableFuture<T>> operation) {
        checkNotNull(orderingKey);
        checkNotNull(operation);
        // Register and enqueue atomically: a task must never occupy a worker while its
        // predecessor is still waiting to enter the FIFO work queue.
        synchronized (submissionLock) {
            ConversionTask<T> task =
                    new ConversionTask<>(operation, lastSubmitted.get(orderingKey));
            try {
                if (closed) {
                    throw new RejectedExecutionException("Kafka conversion executor is closed.");
                }
                workers.execute(task);
                // Rejected tasks must not replace the last accepted task's submission gate.
                // Only accepted work attaches a dependent stage to the predecessor, keeping
                // repeated rejections from retaining an unbounded chain behind a slow task.
                // A failed conversion must still preserve all earlier pending submissions.
                CompletableFuture<Void> submitted =
                        task.predecessor == null
                                ? task.finished
                                : CompletableFuture.allOf(task.predecessor, task.finished);
                lastSubmitted.put(orderingKey, submitted);
                submitted.whenComplete(
                        (ignored, failure) -> {
                            synchronized (submissionLock) {
                                lastSubmitted.remove(orderingKey, submitted);
                            }
                        });
            } catch (RejectedExecutionException failure) {
                task.cancel(failure);
            }
            return task.result;
        }
    }

    /** Waits for earlier tasks with this key to submit append, without waiting for their ACKs. */
    public void awaitAppendTurn() {
        ConversionTask<?> task = checkNotNull(currentTask.get(), "current conversion task");
        try {
            if (task.predecessor != null) {
                task.predecessor.get();
            }
            if (closed) {
                throw new CancellationException("Kafka conversion executor is closed.");
            }
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new CancellationException("Kafka conversion worker was interrupted.");
        } catch (ExecutionException failure) {
            throw new IllegalStateException("Kafka append submission gate failed.", failure);
        }
    }

    /**
     * Stops new work, rejects queued requests, interrupts workers, and awaits their termination.
     */
    public synchronized CompletableFuture<Void> closeAsync() {
        if (closeFuture != null) {
            return closeFuture;
        }
        closed = true;
        RejectedExecutionException failure =
                new RejectedExecutionException("Kafka conversion executor is shutting down.");
        List<Runnable> queued = workers.shutdownNow();
        for (Runnable task : queued) {
            ((ConversionTask<?>) task).cancel(failure);
        }
        closeFuture =
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
                        });
        return closeFuture;
    }

    private final class ConversionTask<T> implements Runnable {
        private final AtomicReference<Supplier<CompletableFuture<T>>> operation;
        private final CompletableFuture<T> result = new CompletableFuture<>();
        private final AtomicBoolean claimed = new AtomicBoolean();
        private final @Nullable CompletableFuture<Void> predecessor;
        private final CompletableFuture<Void> finished = new CompletableFuture<>();

        private ConversionTask(
                Supplier<CompletableFuture<T>> operation,
                @Nullable CompletableFuture<Void> predecessor) {
            this.operation = new AtomicReference<>(operation);
            this.predecessor = predecessor;
        }

        @Override
        public void run() {
            if (!claimed.compareAndSet(false, true)) {
                return;
            }
            Supplier<CompletableFuture<T>> action = operation.getAndSet(null);
            if (result.isDone()) {
                finished.complete(null);
                return;
            }
            currentTask.set(this);
            try {
                checkNotNull(action.get(), "conversion result")
                        .whenComplete(
                                (value, failure) -> {
                                    if (failure == null) {
                                        result.complete(value);
                                    } else {
                                        result.completeExceptionally(failure);
                                    }
                                });
            } catch (Throwable failure) {
                result.completeExceptionally(failure);
            } finally {
                currentTask.remove();
                finished.complete(null);
            }
        }

        private void cancel(Throwable failure) {
            if (claimed.compareAndSet(false, true)) {
                operation.set(null);
                result.completeExceptionally(failure);
                finished.complete(null);
            }
        }
    }
}
