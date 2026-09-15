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

package org.apache.fluss.server.tablet;

import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.exception.UnknownTableOrBucketException;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.gateway.RoutedKvGateway;
import org.apache.fluss.rpc.messages.ApiMessage;
import org.apache.fluss.rpc.messages.MetadataResponse;
import org.apache.fluss.rpc.messages.PbBucketMetadata;
import org.apache.fluss.rpc.messages.PbPutKvRespForBucket;
import org.apache.fluss.rpc.messages.PbServerNode;
import org.apache.fluss.rpc.messages.PbTableMetadata;
import org.apache.fluss.rpc.messages.PbTablePath;
import org.apache.fluss.rpc.messages.PutKvRequest;
import org.apache.fluss.rpc.messages.PutKvResponse;
import org.apache.fluss.rpc.protocol.ApiKeys;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class KvWriteRouterTest {
    @Test
    void testSubrequestsAreSequentialAndRetainNativeFailures() {
        RpcClient client = mock(RpcClient.class);
        CompletableFuture<ApiMessage> first = new CompletableFuture<>();
        when(client.sendRequest(any(ServerNode.class), eq(ApiKeys.PUT_KV), any(PutKvRequest.class)))
                .thenReturn(first, CompletableFuture.completedFuture(response(1)));
        RoutedKvGateway.Route route = new KvWriteRouter(client).prepare(10L, metadata());
        PutKvRequest request = new PutKvRequest().setTableId(10L).setAcks(-1).setTimeoutMs(10000);
        request.addBucketsReq().setBucketId(0).setRecords(new byte[] {1});
        request.addBucketsReq().setBucketId(1).setRecords(new byte[] {2});
        CompletableFuture<PutKvResponse> result = route.write(request);
        assertThat(result).isNotDone();
        verify(client, times(1))
                .sendRequest(any(ServerNode.class), eq(ApiKeys.PUT_KV), any(PutKvRequest.class));
        PutKvResponse failure = response(0);
        failure.getBucketsRespsList()
                .get(0)
                .setError(
                        org.apache.fluss.rpc.protocol.Errors.NOT_ENOUGH_REPLICAS_EXCEPTION.code(),
                        "not replicated");
        first.complete(failure);
        assertThat(result.join().getBucketsRespsList()).hasSize(2);
        assertThat(result.join().getBucketsRespsList().get(0).getErrorCode())
                .isEqualTo(
                        org.apache.fluss.rpc.protocol.Errors.NOT_ENOUGH_REPLICAS_EXCEPTION.code());
        verify(client, times(2))
                .sendRequest(any(ServerNode.class), eq(ApiKeys.PUT_KV), any(PutKvRequest.class));
    }

    @Test
    void testRouteCannotWriteAnotherTableOrARecreatedIdentity() {
        RpcClient client = mock(RpcClient.class);
        KvWriteRouter router = new KvWriteRouter(client);
        assertThatThrownBy(() -> router.prepare(11L, metadata()))
                .isInstanceOf(UnknownTableOrBucketException.class);
        RoutedKvGateway.Route route = router.prepare(10L, metadata());
        assertThatThrownBy(() -> route.write(new PutKvRequest().setTableId(11L)))
                .isInstanceOf(IllegalArgumentException.class);
        verifyNoInteractions(client);
    }

    private static PutKvResponse response(int bucket) {
        return new PutKvResponse()
                .addAllBucketsResps(
                        Collections.singletonList(new PbPutKvRespForBucket().setBucketId(bucket)));
    }

    private static MetadataResponse metadata() {
        return new MetadataResponse()
                .addAllTabletServers(
                        Arrays.asList(
                                new PbServerNode().setNodeId(0).setHost("localhost").setPort(1234),
                                new PbServerNode().setNodeId(1).setHost("localhost").setPort(1235)))
                .addAllTableMetadatas(
                        Collections.singletonList(
                                new PbTableMetadata()
                                        .setTableId(10L)
                                        .setTablePath(
                                                new PbTablePath()
                                                        .setDatabaseName("db")
                                                        .setTableName("table"))
                                        .addAllBucketMetadatas(
                                                Arrays.asList(
                                                        new PbBucketMetadata()
                                                                .setBucketId(0)
                                                                .setLeaderId(0),
                                                        new PbBucketMetadata()
                                                                .setBucketId(1)
                                                                .setLeaderId(1)))));
    }
}
