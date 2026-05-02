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

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.metadata.TableBucket;

import javax.annotation.Nullable;

import java.util.Map;

/**
 * A callback interface for asynchronous offset commit completion notification.
 *
 * <p>The callback is invoked on the user's thread (during {@code poll()}, {@code commitSync()},
 * {@code commitAsync()}, or {@code close()}), never on the I/O thread. This ensures thread safety
 * without synchronization in user code.
 *
 * @since 0.7
 */
@PublicEvolving
@FunctionalInterface
public interface OffsetCommitCallback {

    /**
     * Called when the asynchronous offset commit completes.
     *
     * @param offsets the offsets that were committed
     * @param exception null if the commit succeeded, or the exception that caused the failure
     */
    void onComplete(Map<TableBucket, Long> offsets, @Nullable Exception exception);
}
