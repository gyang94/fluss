/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.tablet;

import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.exception.UnknownTableOrBucketException;
import org.apache.fluss.rpc.GatewayClientProxy;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.gateway.RoutedKvGateway;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.MetadataResponse;
import org.apache.fluss.rpc.messages.PbBucketMetadata;
import org.apache.fluss.rpc.messages.PbPutKvReqForBucket;
import org.apache.fluss.rpc.messages.PbPutKvRespForBucket;
import org.apache.fluss.rpc.messages.PbServerNode;
import org.apache.fluss.rpc.messages.PbTableMetadata;
import org.apache.fluss.rpc.messages.PutKvRequest;
import org.apache.fluss.rpc.messages.PutKvResponse;
import org.apache.fluss.rpc.protocol.Errors;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Routes one authorized KV write with at most one native subrequest in flight. */
final class KvWriteRouter {
    private final RpcClient rpcClient;

    KvWriteRouter(RpcClient rpcClient) {
        this.rpcClient = checkNotNull(rpcClient);
    }

    RoutedKvGateway.Route prepare(long tableId, MetadataResponse metadata) {
        Map<Integer, ServerNode> servers = new HashMap<>();
        for (PbServerNode server : metadata.getTabletServersList()) {
            servers.put(
                    server.getNodeId(),
                    new ServerNode(
                            server.getNodeId(),
                            server.getHost(),
                            server.getPort(),
                            ServerType.TABLET_SERVER));
        }
        Map<Integer, Integer> leaders = new HashMap<>();
        boolean matchingTable = false;
        for (PbTableMetadata table : metadata.getTableMetadatasList()) {
            if (table.getTableId() == tableId) {
                matchingTable = true;
                for (PbBucketMetadata bucket : table.getBucketMetadatasList()) {
                    if (bucket.hasLeaderId()) {
                        leaders.put(bucket.getBucketId(), bucket.getLeaderId());
                    }
                }
            }
        }
        if (!matchingTable) {
            throw new UnknownTableOrBucketException(
                    "KV routing table identity changed or is unavailable.");
        }
        return request -> {
            checkArgument(
                    request.getTableId() == tableId,
                    "KV route cannot be reused for another table.");
            long deadline =
                    System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(request.getTimeoutMs());
            CompletableFuture<PutKvResponse> result =
                    CompletableFuture.completedFuture(new PutKvResponse());
            for (PbPutKvReqForBucket bucket : request.getBucketsReqsList()) {
                result =
                        result.thenCompose(
                                response ->
                                        send(
                                                        request,
                                                        bucket,
                                                        servers.get(
                                                                leaders.get(bucket.getBucketId())),
                                                        deadline)
                                                .thenApply(
                                                        current -> {
                                                            response.addAllBucketsResps(
                                                                    current.getBucketsRespsList());
                                                            return response;
                                                        }));
            }
            return result;
        };
    }

    private CompletableFuture<PutKvResponse> send(
            PutKvRequest request, PbPutKvReqForBucket bucket, ServerNode server, long deadline) {
        if (bucket.hasPartitionId()) {
            return failed(
                    bucket.getBucketId(),
                    Errors.INVALID_TABLE_EXCEPTION,
                    "Partitioned KV routing is not supported.");
        }
        if (server == null) {
            return failed(
                    bucket.getBucketId(),
                    Errors.LEADER_NOT_AVAILABLE_EXCEPTION,
                    "KV bucket leader is unavailable.");
        }
        long remainingMillis = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
        if (remainingMillis <= 0) {
            return failed(
                    bucket.getBucketId(), Errors.REQUEST_TIME_OUT, "KV routing request timed out.");
        }
        PutKvRequest forwarded =
                new PutKvRequest()
                        .setTableId(request.getTableId())
                        .setAcks(request.getAcks())
                        .setTimeoutMs((int) Math.min(Integer.MAX_VALUE, remainingMillis))
                        .addAllBucketsReqs(Collections.singletonList(bucket));
        try {
            TabletServerGateway target =
                    GatewayClientProxy.createGatewayProxy(
                            () -> server, rpcClient, TabletServerGateway.class);
            return target.putKv(forwarded)
                    .handle(
                            (response, failure) -> {
                                if (failure != null) {
                                    return failureResponse(bucket.getBucketId(), failure);
                                }
                                return response;
                            });
        } catch (Throwable failure) {
            return CompletableFuture.completedFuture(
                    failureResponse(bucket.getBucketId(), failure));
        }
    }

    private static PutKvResponse failureResponse(int bucketId, Throwable failure) {
        while (failure instanceof CompletionException && failure.getCause() != null) {
            failure = failure.getCause();
        }
        return response(bucketId, Errors.forException(failure), failure.getMessage());
    }

    private static CompletableFuture<PutKvResponse> failed(
            int bucket, Errors error, String message) {
        return CompletableFuture.completedFuture(response(bucket, error, message));
    }

    private static PutKvResponse response(int bucket, Errors error, String message) {
        PbPutKvRespForBucket result = new PbPutKvRespForBucket().setBucketId(bucket);
        result.setError(error.code(), message);
        return new PutKvResponse().addAllBucketsResps(Collections.singletonList(result));
    }
}
