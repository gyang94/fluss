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

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KafkaNativeProduceOperationTrackerTest {

    @Test
    void testCloseCancelsOnlyRegisteredPreSubmitOperationsExactlyOnce() {
        KafkaNativeProduceOperationTracker tracker = new KafkaNativeProduceOperationTracker();
        AtomicInteger firstCancellations = new AtomicInteger();
        AtomicInteger secondCancellations = new AtomicInteger();
        KafkaNativeProduceOperationTracker.PreSubmitOperation first =
                firstCancellations::incrementAndGet;
        KafkaNativeProduceOperationTracker.PreSubmitOperation second =
                secondCancellations::incrementAndGet;

        assertThat(tracker.register(first)).isTrue();
        assertThat(tracker.register(first)).isFalse();
        assertThat(tracker.register(second)).isTrue();
        tracker.unregister(first);
        assertThat(tracker.activeOperations()).isOne();

        tracker.close();
        tracker.close();

        assertThat(tracker.isClosed()).isTrue();
        assertThat(tracker.activeOperations()).isZero();
        assertThat(firstCancellations).hasValue(0);
        assertThat(secondCancellations).hasValue(1);
        assertThat(tracker.register(first)).isFalse();
    }

    @Test
    void testCloseAttemptsEveryCancellationAndPreservesFirstFailure() {
        KafkaNativeProduceOperationTracker tracker = new KafkaNativeProduceOperationTracker();
        AtomicInteger completedCancellations = new AtomicInteger();
        RuntimeException firstFailure = new RuntimeException("first failure");
        RuntimeException secondFailure = new RuntimeException("second failure");
        tracker.register(() -> completedCancellations.incrementAndGet());
        tracker.register(
                () -> {
                    throw firstFailure;
                });
        tracker.register(
                () -> {
                    throw secondFailure;
                });

        assertThatThrownBy(tracker::close)
                .isInstanceOf(RuntimeException.class)
                .satisfies(
                        failure -> {
                            assertThat(failure == firstFailure || failure == secondFailure)
                                    .isTrue();
                            assertThat(failure.getSuppressed()).hasSize(1);
                        });
        assertThat(completedCancellations).hasValue(1);
        assertThat(tracker.activeOperations()).isZero();
    }

    @Test
    void testSubmitBoundaryIsAtomicWithTrackerClose() {
        KafkaNativeProduceOperationTracker tracker = new KafkaNativeProduceOperationTracker();
        AtomicInteger cancellations = new AtomicInteger();
        KafkaNativeProduceOperationTracker.PreSubmitOperation submitted =
                cancellations::incrementAndGet;
        KafkaNativeProduceOperationTracker.PreSubmitOperation cancelled =
                cancellations::incrementAndGet;
        assertThat(tracker.register(submitted)).isTrue();
        assertThat(tracker.register(cancelled)).isTrue();

        assertThat(tracker.tryStartSubmit(submitted)).isTrue();
        tracker.close();

        assertThat(cancellations).hasValue(1);
        assertThat(tracker.tryStartSubmit(cancelled)).isFalse();
        assertThat(tracker.tryStartSubmit(submitted)).isFalse();
    }
}
