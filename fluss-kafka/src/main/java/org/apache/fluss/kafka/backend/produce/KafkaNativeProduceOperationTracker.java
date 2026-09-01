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

import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Tracks granted native Produce operations that have not crossed the submission boundary. */
@Internal
@ThreadSafe
public final class KafkaNativeProduceOperationTracker implements AutoCloseable {

    /** One granted operation that can still be cancelled safely before native submission. */
    public interface PreSubmitOperation {

        /** Cancels this operation without releasing a submitted native payload. */
        void cancelBeforeSubmit();
    }

    private final Object lock = new Object();
    private final Set<PreSubmitOperation> operations =
            Collections.newSetFromMap(new IdentityHashMap<PreSubmitOperation, Boolean>());
    private boolean closed;

    /** Registers an operation unless tracker shutdown already started. */
    public boolean register(PreSubmitOperation operation) {
        checkNotNull(operation);
        synchronized (lock) {
            return !closed && operations.add(operation);
        }
    }

    /** Removes an operation after it is submitted or otherwise reaches a terminal state. */
    public void unregister(PreSubmitOperation operation) {
        checkNotNull(operation);
        synchronized (lock) {
            operations.remove(operation);
        }
    }

    /**
     * Atomically removes an operation only when shutdown has not started.
     *
     * <p>A {@code true} result is the lifecycle boundary after which tracker shutdown must retain
     * the operation until its real native future completes. A {@code false} result means shutdown
     * won and native submission must not start.
     */
    public boolean tryStartSubmit(PreSubmitOperation operation) {
        checkNotNull(operation);
        synchronized (lock) {
            return !closed && operations.remove(operation);
        }
    }

    /** Returns the number of granted operations that have not been submitted. */
    public int activeOperations() {
        synchronized (lock) {
            return operations.size();
        }
    }

    /** Returns whether tracker shutdown has started. */
    public boolean isClosed() {
        synchronized (lock) {
            return closed;
        }
    }

    /** Stops registration and cancels every operation that is still safe to cancel. */
    @Override
    public void close() {
        List<PreSubmitOperation> snapshot;
        synchronized (lock) {
            if (closed) {
                return;
            }
            closed = true;
            snapshot = new ArrayList<>(operations);
            operations.clear();
        }

        Throwable firstFailure = null;
        for (PreSubmitOperation operation : snapshot) {
            try {
                operation.cancelBeforeSubmit();
            } catch (Throwable failure) {
                if (firstFailure == null) {
                    firstFailure = failure;
                } else {
                    firstFailure.addSuppressed(failure);
                }
            }
        }
        rethrow(firstFailure);
    }

    private static void rethrow(Throwable failure) {
        if (failure == null) {
            return;
        }
        if (failure instanceof Error) {
            throw (Error) failure;
        }
        if (failure instanceof RuntimeException) {
            throw (RuntimeException) failure;
        }
        throw new RuntimeException(failure);
    }
}
