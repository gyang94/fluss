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

package org.apache.fluss.rpc.protocol;

import org.apache.fluss.rpc.netty.server.RequestChannel;
import org.apache.fluss.rpc.netty.server.RequestChannel.QueuePressureSubscription;
import org.apache.fluss.shaded.netty4.io.netty.channel.Channel;
import org.apache.fluss.shaded.netty4.io.netty.channel.EventLoop;
import org.apache.fluss.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests protocol-owned queue pressure subscriptions and native channel isolation. */
class RequestChannelPressureTest {

    @Test
    void testInitialStateAndHighLowWatermarks() {
        RequestChannel requests = new RequestChannel(4);
        TestingSubscriber subscriber = new TestingSubscriber(requests);
        subscriber.runTasks();
        assertThat(subscriber.states).containsExactly(false);

        for (int i = 0; i < 3; i++) {
            requests.putShutdownRequest();
        }
        subscriber.runTasks();
        assertThat(subscriber.states).containsExactly(false);

        requests.putShutdownRequest();
        subscriber.runTasks();
        assertThat(subscriber.states).containsExactly(false, true);

        requests.pollRequest(0);
        subscriber.runTasks();
        assertThat(subscriber.states).containsExactly(false, true);

        requests.pollRequest(0);
        subscriber.runTasks();
        assertThat(subscriber.states).containsExactly(false, true, false);
        subscriber.subscription.close();
    }

    @Test
    void testSubscribeDuringPressurePreservesNativeRegistrationBehavior() {
        RequestChannel requests = new RequestChannel(1);
        EmbeddedChannel existingNative = new EmbeddedChannel();
        EmbeddedChannel newNative = new EmbeddedChannel();
        try {
            requests.registerChannel(existingNative);
            requests.putShutdownRequest();
            existingNative.runPendingTasks();
            assertThat(existingNative.config().isAutoRead()).isFalse();

            requests.registerChannel(newNative);
            TestingSubscriber subscriber = new TestingSubscriber(requests);
            newNative.runPendingTasks();
            subscriber.runTasks();
            assertThat(newNative.config().isAutoRead()).isTrue();
            assertThat(subscriber.states).containsExactly(true);

            requests.pollRequest(0);
            existingNative.runPendingTasks();
            newNative.runPendingTasks();
            subscriber.runTasks();
            assertThat(existingNative.config().isAutoRead()).isTrue();
            assertThat(newNative.config().isAutoRead()).isTrue();
            assertThat(subscriber.states).containsExactly(true, false);
            subscriber.subscription.close();
        } finally {
            requests.unregisterChannel(existingNative);
            requests.unregisterChannel(newNative);
            existingNative.finishAndReleaseAll();
            newNative.finishAndReleaseAll();
        }
    }

    @Test
    void testSubscriptionSeesTransitionBeforeNotificationPass() throws Exception {
        RequestChannel requests = new RequestChannel(1);
        Channel nativeConnection = mock(Channel.class);
        EventLoop nativeEventLoop = mock(EventLoop.class);
        when(nativeConnection.isActive()).thenReturn(true);
        when(nativeConnection.eventLoop()).thenReturn(nativeEventLoop);
        CountDownLatch pauseScheduled = new CountDownLatch(1);
        CountDownLatch finishPause = new CountDownLatch(1);
        doAnswer(
                        invocation -> {
                            pauseScheduled.countDown();
                            if (!finishPause.await(5, TimeUnit.SECONDS)) {
                                throw new IllegalStateException("Timed out releasing native pause");
                            }
                            return null;
                        })
                .when(nativeEventLoop)
                .execute(any(Runnable.class));
        requests.registerChannel(nativeConnection);
        CompletableFuture<Void> enqueue = CompletableFuture.runAsync(requests::putShutdownRequest);
        TestingSubscriber subscriber = null;
        try {
            assertThat(pauseScheduled.await(5, TimeUnit.SECONDS)).isTrue();
            // The producer still holds the transition lock and has not notified subscribers.
            subscriber = new TestingSubscriber(requests);
            subscriber.runTasks();
            assertThat(subscriber.states).containsExactly(true);
        } finally {
            finishPause.countDown();
            enqueue.get(5, TimeUnit.SECONDS);
            requests.unregisterChannel(nativeConnection);
            if (subscriber != null) {
                subscriber.subscription.close();
            }
        }
    }

    @Test
    void testSubscriptionDoesNotChangeProtocolOwnedAutoRead() {
        RequestChannel requests = new RequestChannel(1);
        EmbeddedChannel connection = new EmbeddedChannel();
        List<Boolean> states = new ArrayList<>();
        try (QueuePressureSubscription ignored =
                requests.subscribeQueuePressure(connection, states::add)) {
            connection.runPendingTasks();
            requests.putShutdownRequest();
            connection.runPendingTasks();
            assertThat(connection.config().isAutoRead()).isTrue();

            // The protocol has another reason to pause. Queue recovery must not override it.
            connection.config().setAutoRead(false);
            requests.pollRequest(0);
            connection.runPendingTasks();
            assertThat(connection.config().isAutoRead()).isFalse();
            assertThat(states).containsExactly(false, true, false);
        } finally {
            connection.finishAndReleaseAll();
        }
    }

    @Test
    void testPendingNotificationsAreBoundedAndReadLatestState() {
        RequestChannel requests = new RequestChannel(1);
        TestingSubscriber subscriber = new TestingSubscriber(requests);
        for (int i = 0; i < 1000; i++) {
            requests.putShutdownRequest();
            requests.pollRequest(0);
        }
        requests.putShutdownRequest();
        assertThat(subscriber.tasks).hasSize(1);
        subscriber.runTasks();
        assertThat(subscriber.states).containsExactly(true);
        assertThat(subscriber.tasks).isEmpty();

        requests.pollRequest(0);
        requests.putShutdownRequest();
        requests.pollRequest(0);
        assertThat(subscriber.tasks).hasSize(1);
        subscriber.runTasks();
        assertThat(subscriber.states).containsExactly(true, false);
        subscriber.subscription.close();
    }

    @Test
    void testCloseCancelsPendingAndFutureNotifications() {
        RequestChannel requests = new RequestChannel(1);
        TestingSubscriber subscriber = new TestingSubscriber(requests);
        subscriber.subscription.close();
        subscriber.subscription.close();
        subscriber.runTasks();
        requests.putShutdownRequest();
        assertThat(subscriber.states).isEmpty();
        assertThat(subscriber.tasks).isEmpty();
    }

    @Test
    void testClosingOneSubscriptionDoesNotAffectAnother() {
        RequestChannel requests = new RequestChannel(1);
        TestingSubscriber closedSubscriber = new TestingSubscriber(requests);
        TestingSubscriber activeSubscriber = new TestingSubscriber(requests);
        closedSubscriber.runTasks();
        activeSubscriber.runTasks();
        requests.putShutdownRequest();
        closedSubscriber.subscription.close();
        closedSubscriber.runTasks();
        activeSubscriber.runTasks();
        assertThat(closedSubscriber.states).containsExactly(false);
        assertThat(activeSubscriber.states).containsExactly(false, true);
        activeSubscriber.subscription.close();
    }

    @Test
    void testDisconnectedChannelDoesNotReceivePendingNotification() {
        RequestChannel requests = new RequestChannel(1);
        TestingSubscriber subscriber = new TestingSubscriber(requests);
        subscriber.active.set(false);
        subscriber.runTasks();
        requests.putShutdownRequest();
        assertThat(subscriber.states).isEmpty();
        assertThat(subscriber.tasks).isEmpty();
    }

    @Test
    void testListenerFailureDoesNotAffectEnqueueNativeChannelOrOtherSubscribers() {
        RequestChannel requests = new RequestChannel(1);
        EmbeddedChannel failingConnection = new EmbeddedChannel();
        EmbeddedChannel nativeConnection = new EmbeddedChannel();
        AtomicBoolean fail = new AtomicBoolean(true);
        List<Boolean> recoveredStates = new ArrayList<>();
        try (QueuePressureSubscription ignored =
                requests.subscribeQueuePressure(
                        failingConnection,
                        pressure -> {
                            if (fail.get()) {
                                throw new IllegalStateException("expected listener failure");
                            }
                            recoveredStates.add(pressure);
                        })) {
            requests.registerChannel(nativeConnection);
            TestingSubscriber healthy = new TestingSubscriber(requests);
            failingConnection.runPendingTasks();
            healthy.runTasks();
            requests.putShutdownRequest();
            failingConnection.runPendingTasks();
            nativeConnection.runPendingTasks();
            healthy.runTasks();
            assertThat(requests.requestsCount()).isEqualTo(1);
            assertThat(nativeConnection.config().isAutoRead()).isFalse();
            assertThat(healthy.states).containsExactly(false, true);

            fail.set(false);
            assertThat(requests.pollRequest(0)).isNotNull();
            failingConnection.runPendingTasks();
            nativeConnection.runPendingTasks();
            healthy.runTasks();
            assertThat(recoveredStates).containsExactly(false);
            assertThat(nativeConnection.config().isAutoRead()).isTrue();
            assertThat(healthy.states).containsExactly(false, true, false);
            healthy.subscription.close();
        } finally {
            requests.unregisterChannel(nativeConnection);
            failingConnection.finishAndReleaseAll();
            nativeConnection.finishAndReleaseAll();
        }
    }

    @Test
    void testRejectedNotificationDoesNotChangeRequestOwnership() {
        RequestChannel requests = new RequestChannel(1);
        TestingSubscriber rejected = new TestingSubscriber(requests);
        TestingSubscriber healthy = new TestingSubscriber(requests);
        rejected.runTasks();
        healthy.runTasks();
        doThrow(new RejectedExecutionException("event loop stopped"))
                .when(rejected.eventLoop)
                .execute(any(Runnable.class));

        requests.putShutdownRequest();
        healthy.runTasks();
        assertThat(requests.requestsCount()).isEqualTo(1);
        assertThat(healthy.states).containsExactly(false, true);
        assertThat(requests.pollRequest(0)).isNotNull();
        healthy.runTasks();
        assertThat(rejected.states).containsExactly(false);
        assertThat(healthy.states).containsExactly(false, true, false);
        healthy.subscription.close();
    }

    @Test
    void testPressureChangedDuringCallbackIsDeliveredWithoutQueueLock() {
        RequestChannel requests = new RequestChannel(1);
        Channel connection = mock(Channel.class);
        EventLoop eventLoop = mock(EventLoop.class);
        when(connection.isActive()).thenReturn(true);
        when(connection.eventLoop()).thenReturn(eventLoop);
        // Inline execution makes invoking listeners under the queue lock observable: the callback
        // below would wait on a worker unable to acquire that lock to release queue pressure.
        doAnswer(
                        invocation -> {
                            invocation.<Runnable>getArgument(0).run();
                            return null;
                        })
                .when(eventLoop)
                .execute(any(Runnable.class));
        AtomicReference<Throwable> failure = new AtomicReference<>();
        List<Boolean> states = new ArrayList<>();
        try (QueuePressureSubscription ignored =
                requests.subscribeQueuePressure(
                        connection,
                        pressure -> {
                            states.add(pressure);
                            if (pressure) {
                                try {
                                    CompletableFuture.runAsync(() -> requests.pollRequest(0))
                                            .get(5, TimeUnit.SECONDS);
                                } catch (Exception error) {
                                    failure.set(error);
                                }
                            }
                        })) {
            requests.putShutdownRequest();
            assertThat(failure.get()).isNull();
            assertThat(requests.requestsCount()).isZero();
            assertThat(states).containsExactly(false, true, false);
        }
    }

    @Test
    void testCallbackMayCancelItself() {
        RequestChannel requests = new RequestChannel(1);
        TestingSubscriber subscriber = new TestingSubscriber(requests);
        AtomicReference<QueuePressureSubscription> subscription = new AtomicReference<>();
        List<Boolean> states = new ArrayList<>();
        subscription.set(
                requests.subscribeQueuePressure(
                        subscriber.channel,
                        pressure -> {
                            states.add(pressure);
                            subscription.get().close();
                        }));
        subscriber.runTasks();
        requests.putShutdownRequest();
        subscriber.runTasks();
        assertThat(states).containsExactly(false);
        assertThat(subscriber.states).containsExactly(false, true);
        subscriber.subscription.close();
    }

    private static final class TestingSubscriber {
        private final Channel channel = mock(Channel.class);
        private final EventLoop eventLoop = mock(EventLoop.class);
        private final AtomicBoolean active = new AtomicBoolean(true);
        private final Queue<Runnable> tasks = new ConcurrentLinkedQueue<>();
        private final List<Boolean> states = new ArrayList<>();
        private final QueuePressureSubscription subscription;

        private TestingSubscriber(RequestChannel requests) {
            when(channel.isActive()).thenAnswer(ignored -> active.get());
            when(channel.eventLoop()).thenReturn(eventLoop);
            doAnswer(
                            invocation -> {
                                tasks.add(invocation.getArgument(0));
                                return null;
                            })
                    .when(eventLoop)
                    .execute(any(Runnable.class));
            subscription = requests.subscribeQueuePressure(channel, states::add);
        }

        private void runTasks() {
            Runnable task;
            while ((task = tasks.poll()) != null) {
                task.run();
            }
        }
    }
}
