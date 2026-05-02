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

package org.apache.fluss.client.table.scanner.log;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.exception.ApiException;
import org.apache.fluss.exception.CoordinatorLoadInProgressException;
import org.apache.fluss.exception.CoordinatorNotAvailableException;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.NotCoordinatorException;
import org.apache.fluss.exception.NotCoordinatorLeaderException;
import org.apache.fluss.exception.RetriableException;
import org.apache.fluss.exception.TimeoutException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.CommitOffsetsRequest;
import org.apache.fluss.rpc.messages.CommitOffsetsResponse;
import org.apache.fluss.rpc.messages.FindCoordinatorRequest;
import org.apache.fluss.rpc.messages.FindCoordinatorResponse;
import org.apache.fluss.rpc.messages.PbCommitOffsetEntry;
import org.apache.fluss.rpc.messages.PbCommitOffsetResultEntry;
import org.apache.fluss.rpc.protocol.ApiError;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.utils.clock.Clock;
import org.apache.fluss.utils.clock.SystemClock;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Handles synchronous offset commits for assign-mode log scanners. */
@Internal
@NotThreadSafe
class OffsetCommitter implements AutoCloseable {

    private static final int ASSIGN_MODE_GENERATION_ID = -1;
    private static final String ASSIGN_MODE_MEMBER_ID = "";
    private static final int ASSIGN_MODE_LEADER_EPOCH = -1;

    private final String groupId;
    private final MetadataUpdater metadataUpdater;
    private final long retryBackoffMs;
    private final Clock clock;
    private final Sleeper sleeper;

    private @Nullable Integer coordinatorServerId;
    private @Nullable TabletServerGateway coordinatorGateway;

    OffsetCommitter(String groupId, MetadataUpdater metadataUpdater, long retryBackoffMs) {
        this(
                groupId,
                metadataUpdater,
                retryBackoffMs,
                SystemClock.getInstance(),
                new ThreadSleeper());
    }

    @VisibleForTesting
    OffsetCommitter(
            String groupId,
            MetadataUpdater metadataUpdater,
            long retryBackoffMs,
            Clock clock,
            Sleeper sleeper) {
        this.groupId = checkNotNull(groupId, "groupId must not be null");
        this.metadataUpdater = checkNotNull(metadataUpdater, "metadataUpdater must not be null");
        this.retryBackoffMs = retryBackoffMs;
        this.clock = checkNotNull(clock, "clock must not be null");
        this.sleeper = checkNotNull(sleeper, "sleeper must not be null");
    }

    void commitOffsetsSync(Map<TableBucket, Long> offsets, long timeoutMs) {
        checkNotNull(offsets, "offsets must not be null");
        if (offsets.isEmpty()) {
            return;
        }

        long deadlineNanos = clock.nanoseconds() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        ApiException lastRetriableFailure = null;

        while (true) {
            if (remainingTimeMs(deadlineNanos) <= 0) {
                throw timeoutException(timeoutMs, offsets, lastRetriableFailure);
            }

            try {
                ensureCoordinatorReady(deadlineNanos, timeoutMs, offsets);
                CommitOffsetsResponse response =
                        awaitResponse(
                                coordinatorGateway.commitOffsets(
                                        buildCommitOffsetsRequest(offsets)),
                                deadlineNanos,
                                timeoutMs,
                                offsets,
                                "committing offsets");
                handleCommitResponse(response, offsets.size());
                return;
            } catch (ApiException exception) {
                if (!shouldRetry(exception)) {
                    throw exception;
                }

                lastRetriableFailure = exception;
                if (shouldClearCoordinator(exception)) {
                    clearCoordinator();
                }
                backoffBeforeRetry(deadlineNanos, timeoutMs, offsets, exception);
            }
        }
    }

    void commitOffsetsAsync(
            Map<TableBucket, Long> offsets,
            OffsetCommitCallback callback,
            ConcurrentLinkedQueue<OffsetCommitCompletion> completionQueue) {
        checkNotNull(offsets, "offsets must not be null");
        checkNotNull(completionQueue, "completionQueue must not be null");
        if (offsets.isEmpty()) {
            if (callback != null) {
                completionQueue.add(new OffsetCommitCompletion(callback, offsets, null));
            }
            return;
        }

        if (coordinatorGateway != null) {
            sendAsyncCommit(offsets, callback, completionQueue);
            return;
        }

        // Need to discover coordinator first
        TabletServerGateway bootstrapGateway = metadataUpdater.newRandomTabletServerClient();
        if (bootstrapGateway == null) {
            if (callback != null) {
                completionQueue.add(
                        new OffsetCommitCompletion(
                                callback,
                                offsets,
                                new CoordinatorNotAvailableException(
                                        "No tablet server is available to resolve coordinator for"
                                                + " group "
                                                + groupId
                                                + ".")));
            }
            return;
        }

        bootstrapGateway
                .findCoordinator(buildFindCoordinatorRequest())
                .whenComplete(
                        (findResponse, findError) -> {
                            if (findError != null) {
                                if (callback != null) {
                                    completionQueue.add(
                                            new OffsetCommitCompletion(
                                                    callback, offsets, wrapAsException(findError)));
                                }
                                return;
                            }

                            ApiError responseError = ApiError.fromErrorMessage(findResponse);
                            if (responseError.isFailure()) {
                                if (callback != null) {
                                    completionQueue.add(
                                            new OffsetCommitCompletion(
                                                    callback, offsets, responseError.exception()));
                                }
                                return;
                            }

                            if (!findResponse.hasCoordinatorServerId()) {
                                if (callback != null) {
                                    completionQueue.add(
                                            new OffsetCommitCompletion(
                                                    callback,
                                                    offsets,
                                                    new CoordinatorNotAvailableException(
                                                            "FindCoordinator response is missing"
                                                                    + " coordinator server id for"
                                                                    + " group "
                                                                    + groupId
                                                                    + ".")));
                                }
                                return;
                            }

                            int serverId = findResponse.getCoordinatorServerId();
                            TabletServerGateway gateway =
                                    metadataUpdater.newTabletServerClientForNode(serverId);
                            if (gateway == null) {
                                if (callback != null) {
                                    completionQueue.add(
                                            new OffsetCommitCompletion(
                                                    callback,
                                                    offsets,
                                                    new CoordinatorNotAvailableException(
                                                            "Coordinator server "
                                                                    + serverId
                                                                    + " is not available in"
                                                                    + " client metadata for"
                                                                    + " group "
                                                                    + groupId
                                                                    + ".")));
                                }
                                return;
                            }

                            coordinatorServerId = serverId;
                            coordinatorGateway = gateway;
                            sendAsyncCommit(offsets, callback, completionQueue);
                        });
    }

    private void sendAsyncCommit(
            Map<TableBucket, Long> offsets,
            @Nullable OffsetCommitCallback callback,
            ConcurrentLinkedQueue<OffsetCommitCompletion> completionQueue) {
        coordinatorGateway
                .commitOffsets(buildCommitOffsetsRequest(offsets))
                .whenComplete(
                        (commitResponse, commitError) -> {
                            if (commitError != null) {
                                clearCoordinator();
                                if (callback != null) {
                                    completionQueue.add(
                                            new OffsetCommitCompletion(
                                                    callback,
                                                    offsets,
                                                    wrapAsException(commitError)));
                                }
                                return;
                            }

                            Exception resultException = null;
                            try {
                                handleCommitResponse(commitResponse, offsets.size());
                            } catch (Exception e) {
                                resultException = e;
                                if (shouldClearCoordinator(e)) {
                                    clearCoordinator();
                                }
                            }

                            if (callback != null) {
                                completionQueue.add(
                                        new OffsetCommitCompletion(
                                                callback, offsets, resultException));
                            }
                        });
    }

    private static Exception wrapAsException(Throwable t) {
        Throwable cause = Errors.maybeUnwrapException(t);
        if (cause instanceof Exception) {
            return (Exception) cause;
        }
        return new FlussRuntimeException(cause);
    }

    private void ensureCoordinatorReady(
            long deadlineNanos, long timeoutMs, Map<TableBucket, Long> offsets) {
        if (coordinatorGateway != null) {
            return;
        }

        ensureTabletServerSnapshotAvailable(deadlineNanos, timeoutMs, offsets);

        TabletServerGateway bootstrapGateway = metadataUpdater.newRandomTabletServerClient();
        if (bootstrapGateway == null) {
            throw new CoordinatorNotAvailableException(
                    "No tablet server is available to resolve coordinator for group "
                            + groupId
                            + ".");
        }

        FindCoordinatorResponse response =
                awaitResponse(
                        bootstrapGateway.findCoordinator(buildFindCoordinatorRequest()),
                        deadlineNanos,
                        timeoutMs,
                        offsets,
                        "finding coordinator");

        ApiError responseError = ApiError.fromErrorMessage(response);
        if (responseError.isFailure()) {
            throw responseError.exception();
        }
        if (!response.hasCoordinatorServerId()) {
            throw new CoordinatorNotAvailableException(
                    "FindCoordinator response is missing coordinator server id for group "
                            + groupId
                            + ".");
        }

        int serverId = response.getCoordinatorServerId();
        TabletServerGateway gateway = metadataUpdater.newTabletServerClientForNode(serverId);
        if (gateway == null) {
            refreshMetadata(deadlineNanos, timeoutMs, offsets);
            gateway = metadataUpdater.newTabletServerClientForNode(serverId);
        }
        if (gateway == null) {
            throw new CoordinatorNotAvailableException(
                    "Coordinator server "
                            + serverId
                            + " is not available in client metadata for group "
                            + groupId
                            + ".");
        }

        coordinatorServerId = serverId;
        coordinatorGateway = gateway;
    }

    private void ensureTabletServerSnapshotAvailable(
            long deadlineNanos, long timeoutMs, Map<TableBucket, Long> offsets) {
        if (!metadataUpdater.getCluster().getAliveTabletServers().isEmpty()) {
            return;
        }

        refreshMetadata(deadlineNanos, timeoutMs, offsets);
        if (metadataUpdater.getCluster().getAliveTabletServers().isEmpty()) {
            throw new CoordinatorNotAvailableException(
                    "No tablet server is available in client metadata for group " + groupId + ".");
        }
    }

    private void refreshMetadata(
            long deadlineNanos, long timeoutMs, Map<TableBucket, Long> offsets) {
        long remainingMs = remainingTimeMs(deadlineNanos);
        if (remainingMs <= 0) {
            throw timeoutException(timeoutMs, offsets, null);
        }
        metadataUpdater.updateMetadata(null, null, null, remainingMs);
    }

    private CommitOffsetsRequest buildCommitOffsetsRequest(Map<TableBucket, Long> offsets) {
        CommitOffsetsRequest request = new CommitOffsetsRequest();
        request.setGroupId(groupId);
        request.setGenerationId(ASSIGN_MODE_GENERATION_ID);
        request.setMemberId(ASSIGN_MODE_MEMBER_ID);

        for (Map.Entry<TableBucket, Long> entry : offsets.entrySet()) {
            TableBucket tableBucket = entry.getKey();
            PbCommitOffsetEntry offsetEntry = request.addOffset();
            offsetEntry.setTableId(tableBucket.getTableId());
            if (tableBucket.getPartitionId() != null) {
                offsetEntry.setPartitionId(tableBucket.getPartitionId());
            }
            offsetEntry.setBucketId(tableBucket.getBucket());
            offsetEntry.setOffset(entry.getValue());
            offsetEntry.setLeaderEpoch(ASSIGN_MODE_LEADER_EPOCH);
            offsetEntry.setMetadata("");
        }
        return request;
    }

    private FindCoordinatorRequest buildFindCoordinatorRequest() {
        FindCoordinatorRequest request = new FindCoordinatorRequest();
        request.setGroupId(groupId);
        return request;
    }

    private void handleCommitResponse(CommitOffsetsResponse response, int expectedEntries) {
        ApiError responseError = ApiError.fromErrorMessage(response);
        if (responseError.isFailure()) {
            throw responseError.exception();
        }
        if (response.getResultsCount() != expectedEntries) {
            throw new FlussRuntimeException(
                    "CommitOffsets response returned "
                            + response.getResultsCount()
                            + " results for "
                            + expectedEntries
                            + " request entries.");
        }

        for (int i = 0; i < response.getResultsCount(); i++) {
            PbCommitOffsetResultEntry resultEntry = response.getResultAt(i);
            ApiError entryError = ApiError.fromErrorMessage(resultEntry);
            if (entryError.isFailure()) {
                throw entryError.exception();
            }
        }
    }

    private void backoffBeforeRetry(
            long deadlineNanos,
            long timeoutMs,
            Map<TableBucket, Long> offsets,
            ApiException cause) {
        long remainingMs = remainingTimeMs(deadlineNanos);
        if (remainingMs <= 0) {
            throw timeoutException(timeoutMs, offsets, cause);
        }

        long sleepMs = Math.min(retryBackoffMs, remainingMs);
        if (sleepMs <= 0) {
            throw timeoutException(timeoutMs, offsets, cause);
        }

        try {
            sleeper.sleep(sleepMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new FlussRuntimeException(
                    "Interrupted while backing off before retrying offset commit.", e);
        }
    }

    private <T> T awaitResponse(
            CompletableFuture<T> future,
            long deadlineNanos,
            long timeoutMs,
            Map<TableBucket, Long> offsets,
            String action) {
        long remainingMs = remainingTimeMs(deadlineNanos);
        if (remainingMs <= 0) {
            throw timeoutException(timeoutMs, offsets, null);
        }

        try {
            return future.get(remainingMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new FlussRuntimeException("Interrupted while " + action + ".", e);
        } catch (java.util.concurrent.TimeoutException e) {
            throw timeoutException(timeoutMs, offsets, e);
        } catch (ExecutionException e) {
            throw translateFailure(e, action);
        }
    }

    private ApiException translateFailure(Throwable failure, String action) {
        Throwable cause = Errors.maybeUnwrapException(failure);
        if (cause instanceof ApiException) {
            return (ApiException) cause;
        }
        if (cause instanceof java.util.concurrent.TimeoutException) {
            return new TimeoutException("Timed out while " + action + ".", cause);
        }
        return ApiError.fromThrowable(cause).exception();
    }

    private boolean shouldRetry(ApiException exception) {
        return exception instanceof RetriableException
                || exception instanceof NotCoordinatorException
                || exception instanceof NotCoordinatorLeaderException;
    }

    private boolean shouldClearCoordinator(Exception exception) {
        return exception instanceof CoordinatorNotAvailableException
                || exception instanceof NotCoordinatorException
                || exception instanceof NotCoordinatorLeaderException
                || exception instanceof RetriableException
                        && !(exception instanceof CoordinatorLoadInProgressException);
    }

    private void clearCoordinator() {
        coordinatorServerId = null;
        coordinatorGateway = null;
    }

    private long remainingTimeMs(long deadlineNanos) {
        return TimeUnit.NANOSECONDS.toMillis(Math.max(0L, deadlineNanos - clock.nanoseconds()));
    }

    private TimeoutException timeoutException(
            long timeoutMs, Map<TableBucket, Long> offsets, @Nullable Throwable cause) {
        String message =
                "Timeout of "
                        + timeoutMs
                        + "ms expired before successfully committing offsets "
                        + offsets
                        + ".";
        return cause == null ? new TimeoutException(message) : new TimeoutException(message, cause);
    }

    @Override
    public void close() {
        clearCoordinator();
    }

    @VisibleForTesting
    interface Sleeper {
        void sleep(long sleepMs) throws InterruptedException;
    }

    private static final class ThreadSleeper implements Sleeper {
        @Override
        public void sleep(long sleepMs) throws InterruptedException {
            Thread.sleep(sleepMs);
        }
    }

    /** Holds a completed async offset commit result for deferred callback invocation. */
    static final class OffsetCommitCompletion {
        private final OffsetCommitCallback callback;
        private final Map<TableBucket, Long> offsets;
        private final @Nullable Exception exception;

        OffsetCommitCompletion(
                OffsetCommitCallback callback,
                Map<TableBucket, Long> offsets,
                @Nullable Exception exception) {
            this.callback = checkNotNull(callback, "callback must not be null");
            this.offsets = checkNotNull(offsets, "offsets must not be null");
            this.exception = exception;
        }

        void invoke() {
            callback.onComplete(offsets, exception);
        }
    }
}
