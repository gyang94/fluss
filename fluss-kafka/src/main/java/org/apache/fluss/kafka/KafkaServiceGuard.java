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

package org.apache.fluss.kafka;

import org.apache.fluss.annotation.Internal;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Serializes native submission admission with service retirement. */
@Internal
public final class KafkaServiceGuard {
    private final Lock lock;
    private final BooleanSupplier available;

    /** Creates a guard for callers without a shared service controller. */
    public KafkaServiceGuard(BooleanSupplier available) {
        this(new ReentrantLock(), available);
    }

    KafkaServiceGuard(Lock lock, BooleanSupplier available) {
        this.lock = checkNotNull(lock);
        this.available = checkNotNull(available);
    }

    /** Returns whether this request's connection still belongs to the serving generation. */
    public boolean isAvailable() {
        return available.getAsBoolean();
    }

    /** Rejects work which has not reached native submission. */
    public void ensureAvailable() {
        if (!isAvailable()) {
            throw new KafkaServiceUnavailableException();
        }
    }

    /**
     * Invokes the synchronous submission under the service transition lock. An asynchronous result
     * is returned unchanged; acknowledgement completion never holds this lock.
     */
    public <T> T execute(Supplier<T> submission) {
        lock.lock();
        try {
            ensureAvailable();
            return submission.get();
        } finally {
            lock.unlock();
        }
    }
}
