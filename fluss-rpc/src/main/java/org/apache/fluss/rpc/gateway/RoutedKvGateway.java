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

package org.apache.fluss.rpc.gateway;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.PutKvRequest;
import org.apache.fluss.rpc.messages.PutKvResponse;
import org.apache.fluss.rpc.netty.server.Session;

import java.util.concurrent.CompletableFuture;

/** A local capability for authorized KV writes spanning multiple tablet leaders. */
@Internal
public interface RoutedKvGateway {

    /** Resolves one authorized table route on the caller's metadata/conversion executor. */
    Route prepareKvWrite(TablePath tablePath, long tableId, Session session);

    /**
     * Immutable, request-scoped routing state; submission must not perform blocking metadata work.
     */
    @FunctionalInterface
    interface Route {
        /** Submits non-partitioned KV batches for the authorized table identity. */
        CompletableFuture<PutKvResponse> write(PutKvRequest request);
    }
}
