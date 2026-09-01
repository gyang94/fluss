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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/** Bounded conversion executor that explicitly rejects queued tasks during plugin shutdown. */
@Internal
public final class KafkaProduceConversionExecutor extends ThreadPoolExecutor {

    /** Creates a fixed-size executor with a bounded conversion queue. */
    public KafkaProduceConversionExecutor(int threads, int queueCapacity) {
        super(
                threads,
                threads,
                0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(queueCapacity),
                new ExecutorThreadFactory("kafka-produce-conversion"),
                new AbortPolicy());
    }

    /** Stops admission of new tasks and fails tasks that have not started. */
    public void shutdownAndCancelQueuedTasks() {
        shutdown();
        List<Runnable> queuedTasks = new ArrayList<>();
        getQueue().drainTo(queuedTasks);
        cancelQueuedTasks(queuedTasks);
    }

    /** Stops workers and fails every task returned from the executor queue. */
    @Override
    public List<Runnable> shutdownNow() {
        List<Runnable> queuedTasks = super.shutdownNow();
        cancelQueuedTasks(queuedTasks);
        return queuedTasks;
    }

    private static void cancelQueuedTasks(List<Runnable> queuedTasks) {
        RejectedExecutionException failure =
                new RejectedExecutionException(
                        "Kafka Produce conversion stopped before the queued task started.");
        for (Runnable queuedTask : queuedTasks) {
            if (queuedTask instanceof CancellableTask) {
                ((CancellableTask) queuedTask).cancel(failure);
            }
        }
    }

    /** A queued conversion task whose result can be failed without executing its body. */
    public interface CancellableTask extends Runnable {

        /** Fails the task if execution has not started. */
        void cancel(Throwable failure);
    }
}
