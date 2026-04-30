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

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;

/**
 * Generic container holding a list of {@link KvRecord}s to be written and a response to return
 * after the write completes.
 *
 * @param <T> the type of the response
 */
@Internal
public final class CoordinatorResult<T> {

    private final List<KvRecord> records;
    private final T response;

    public CoordinatorResult(List<KvRecord> records, T response) {
        this.records = Collections.unmodifiableList(records);
        this.response = response;
    }

    public List<KvRecord> records() {
        return records;
    }

    public T response() {
        return response;
    }

    /** A key-value record to be written to the coordinator's state store. */
    @Internal
    public static final class KvRecord {

        private final byte[] key;
        @Nullable private final byte[] value;

        public KvRecord(byte[] key, @Nullable byte[] value) {
            this.key = key;
            this.value = value;
        }

        public byte[] key() {
            return key;
        }

        @Nullable
        public byte[] value() {
            return value;
        }
    }
}
