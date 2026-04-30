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
import org.apache.fluss.exception.InvalidGroupIdException;
import org.apache.fluss.rpc.messages.CommitOffsetsRequest;
import org.apache.fluss.rpc.messages.CommitOffsetsResponse;
import org.apache.fluss.rpc.messages.FetchOffsetsRequest;
import org.apache.fluss.rpc.messages.FetchOffsetsResponse;
import org.apache.fluss.rpc.messages.FindCoordinatorRequest;
import org.apache.fluss.rpc.messages.FindCoordinatorResponse;
import org.apache.fluss.rpc.protocol.Errors;

import java.util.concurrent.CompletableFuture;

/**
 * Implementation of {@link GroupCoordinator} that validates requests and routes them to the correct
 * shard in the {@link CoordinatorRuntime}.
 *
 * <p>Each consumer group is mapped to a bucket via consistent hashing of the group ID. The bucket
 * ID determines which {@link GroupCoordinatorShard} handles the request.
 */
@Internal
public class GroupCoordinatorService implements GroupCoordinator {

    private final CoordinatorRuntime runtime;
    private final int numBuckets;

    public GroupCoordinatorService(CoordinatorRuntime runtime, int numBuckets) {
        this.runtime = runtime;
        this.numBuckets = numBuckets;
    }

    @Override
    public CompletableFuture<FindCoordinatorResponse> findCoordinator(
            FindCoordinatorRequest request) {
        String groupId = request.getGroupId();
        try {
            validateGroupId(groupId);
        } catch (InvalidGroupIdException e) {
            CompletableFuture<FindCoordinatorResponse> failed = new CompletableFuture<>();
            failed.completeExceptionally(e);
            return failed;
        }

        // Metadata cache integration is not yet implemented; return COORDINATOR_NOT_AVAILABLE.
        FindCoordinatorResponse response = new FindCoordinatorResponse();
        response.setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE_EXCEPTION.code());
        response.setErrorMessage("Coordinator not available for group " + groupId);
        return CompletableFuture.completedFuture(response);
    }

    @Override
    public CompletableFuture<CommitOffsetsResponse> commitOffsets(CommitOffsetsRequest request) {
        String groupId = request.getGroupId();
        try {
            validateGroupId(groupId);
        } catch (InvalidGroupIdException e) {
            CompletableFuture<CommitOffsetsResponse> failed = new CompletableFuture<>();
            failed.completeExceptionally(e);
            return failed;
        }

        int bucketId = computeBucketId(groupId, numBuckets);
        return runtime.scheduleWriteOperation(bucketId, shard -> shard.commitOffset(request));
    }

    @Override
    public CompletableFuture<FetchOffsetsResponse> fetchOffsets(FetchOffsetsRequest request) {
        String groupId = request.getGroupId();
        try {
            validateGroupId(groupId);
        } catch (InvalidGroupIdException e) {
            CompletableFuture<FetchOffsetsResponse> failed = new CompletableFuture<>();
            failed.completeExceptionally(e);
            return failed;
        }

        int bucketId = computeBucketId(groupId, numBuckets);
        return runtime.scheduleReadOperation(bucketId, shard -> shard.fetchOffsets(request));
    }

    /**
     * Computes the bucket ID for a given group ID using consistent hashing.
     *
     * @param groupId the group ID
     * @param numBuckets the total number of buckets
     * @return the bucket ID in the range [0, numBuckets)
     */
    static int computeBucketId(String groupId, int numBuckets) {
        return (groupId.hashCode() & 0x7FFFFFFF) % numBuckets;
    }

    /**
     * Validates that the group ID is not null or empty.
     *
     * @param groupId the group ID to validate
     * @throws InvalidGroupIdException if the group ID is null or empty
     */
    private static void validateGroupId(String groupId) {
        if (groupId == null || groupId.trim().isEmpty()) {
            throw new InvalidGroupIdException("Group ID must not be null or empty.");
        }
    }
}
