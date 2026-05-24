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

package org.apache.fluss.server.coordinator;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableBucketReplica;
import org.apache.fluss.server.coordinator.event.CoordinatorEvent;
import org.apache.fluss.server.coordinator.event.StopReplicaSendFailedEvent;
import org.apache.fluss.server.coordinator.event.TestingEventManager;
import org.apache.fluss.server.zk.ZkEpoch;
import org.apache.fluss.server.zk.data.LeaderAndIsr;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.fluss.server.coordinator.CoordinatorTestUtils.createServers;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for {@link CoordinatorRequestBatch}, focusing on the pre-check in {@code sendStopRequest}
 * that surfaces a {@link StopReplicaSendFailedEvent} when a target tablet server is not in the live
 * set.
 */
class CoordinatorRequestBatchTest {

    private static final long TABLE_ID = 1L;
    private static final int BUCKET = 0;
    private static final int LIVE_SERVER = 1;
    private static final int DEAD_SERVER = 2;
    private static final int LEADER_EPOCH = 0;
    private static final int COORDINATOR_EPOCH = 0;

    private CoordinatorContext coordinatorContext;
    private TestCoordinatorChannelManager testCoordinatorChannelManager;
    private TestingEventManager testingEventManager;
    private CoordinatorRequestBatch requestBatch;
    private TableBucket tableBucket;

    @BeforeEach
    void beforeEach() {
        coordinatorContext = new CoordinatorContext(ZkEpoch.INITIAL_EPOCH);
        // servers 0 and 1 are live; server 2 is intentionally NOT in the live set
        coordinatorContext.setLiveTabletServers(createServers(Arrays.asList(0, LIVE_SERVER)));

        // bucket has replicas on the live server and the dead server
        tableBucket = new TableBucket(TABLE_ID, BUCKET);
        coordinatorContext.updateBucketReplicaAssignment(
                tableBucket, Arrays.asList(LIVE_SERVER, DEAD_SERVER));
        coordinatorContext.putBucketLeaderAndIsr(
                tableBucket,
                new LeaderAndIsr(
                        LIVE_SERVER,
                        LEADER_EPOCH,
                        Arrays.asList(LIVE_SERVER, DEAD_SERVER),
                        Collections.emptyList(),
                        COORDINATOR_EPOCH,
                        0));

        // no gateways are set on the channel manager; the live-set pre-check is what matters here
        testCoordinatorChannelManager = new TestCoordinatorChannelManager();
        testingEventManager = new TestingEventManager();
        requestBatch =
                new CoordinatorRequestBatch(
                        testCoordinatorChannelManager, testingEventManager, coordinatorContext);
    }

    @Test
    void testSendStopRequestEmitsFailedEventWhenServerNotInLiveSet() {
        // schedule a delete=true stop-replica targeting BOTH the live and dead server
        Set<Integer> targetServers = new HashSet<>(Arrays.asList(LIVE_SERVER, DEAD_SERVER));
        requestBatch.newBatch();
        requestBatch.addStopReplicaRequestForTabletServers(
                targetServers, tableBucket, true, true, LEADER_EPOCH);

        // fire sendStopRequest internally
        requestBatch.sendRequestToTabletServers(COORDINATOR_EPOCH);

        // exactly one StopReplicaSendFailedEvent should be emitted (for the dead server)
        List<StopReplicaSendFailedEvent> failedEvents =
                testingEventManager.getEvents().stream()
                        .filter(e -> e instanceof StopReplicaSendFailedEvent)
                        .map(e -> (StopReplicaSendFailedEvent) e)
                        .collect(Collectors.toList());
        assertThat(failedEvents).hasSize(1);

        Set<TableBucketReplica> failedReplicas = failedEvents.get(0).getFailedReplicas();
        assertThat(failedReplicas)
                .containsExactly(new TableBucketReplica(tableBucket, DEAD_SERVER))
                .doesNotContain(new TableBucketReplica(tableBucket, LIVE_SERVER));
    }

    @Test
    void testSendStopRequestSilentlySkipsNonDeleteForDeadServer() {
        // schedule a non-deletion stop-replica (delete=false, deleteRemote=false) targeting the
        // dead server; this should be silently skipped without surfacing any event
        requestBatch.newBatch();
        requestBatch.addStopReplicaRequestForTabletServers(
                Collections.singleton(DEAD_SERVER), tableBucket, false, false, LEADER_EPOCH);

        requestBatch.sendRequestToTabletServers(COORDINATOR_EPOCH);

        // no StopReplicaSendFailedEvent should be emitted for non-delete stop replicas
        List<CoordinatorEvent> failedEvents =
                testingEventManager.getEvents().stream()
                        .filter(e -> e instanceof StopReplicaSendFailedEvent)
                        .collect(Collectors.toList());
        assertThat(failedEvents).isEmpty();
    }
}
