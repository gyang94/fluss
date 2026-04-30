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

package org.apache.fluss.server.coordinator.group;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.rpc.messages.CommitOffsetsRequest;
import org.apache.fluss.rpc.messages.CommitOffsetsResponse;
import org.apache.fluss.rpc.messages.FetchOffsetsRequest;
import org.apache.fluss.rpc.messages.FetchOffsetsResponse;
import org.apache.fluss.rpc.messages.FindCoordinatorRequest;
import org.apache.fluss.rpc.messages.FindCoordinatorResponse;

import java.util.concurrent.CompletableFuture;

/**
 * The group coordinator interface that handles consumer group operations including finding the
 * coordinator, committing offsets, and fetching offsets.
 */
@Internal
public interface GroupCoordinator {

    /**
     * Finds the coordinator server responsible for the given group.
     *
     * @param request the find coordinator request
     * @return a future containing the coordinator location
     */
    CompletableFuture<FindCoordinatorResponse> findCoordinator(FindCoordinatorRequest request);

    /**
     * Commits offsets for a consumer group.
     *
     * @param request the commit offsets request
     * @return a future containing the commit result
     */
    CompletableFuture<CommitOffsetsResponse> commitOffsets(CommitOffsetsRequest request);

    /**
     * Fetches committed offsets for a consumer group.
     *
     * @param request the fetch offsets request
     * @return a future containing the fetched offsets
     */
    CompletableFuture<FetchOffsetsResponse> fetchOffsets(FetchOffsetsRequest request);
}
