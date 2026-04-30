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

import java.util.Objects;

/** Immutable value object holding an offset along with associated metadata. */
@Internal
public final class OffsetAndMetadata {

    public static final OffsetAndMetadata EMPTY = new OffsetAndMetadata(-1L, -1, "", -1L);

    private final long offset;
    private final int leaderEpoch;
    private final String metadata;
    private final long commitTimestampMs;

    public OffsetAndMetadata(
            long offset, int leaderEpoch, String metadata, long commitTimestampMs) {
        this.offset = offset;
        this.leaderEpoch = leaderEpoch;
        this.metadata = metadata;
        this.commitTimestampMs = commitTimestampMs;
    }

    public long offset() {
        return offset;
    }

    public int leaderEpoch() {
        return leaderEpoch;
    }

    public String metadata() {
        return metadata;
    }

    public long commitTimestampMs() {
        return commitTimestampMs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        OffsetAndMetadata that = (OffsetAndMetadata) o;
        return offset == that.offset
                && leaderEpoch == that.leaderEpoch
                && commitTimestampMs == that.commitTimestampMs
                && Objects.equals(metadata, that.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(offset, leaderEpoch, metadata, commitTimestampMs);
    }

    @Override
    public String toString() {
        return "OffsetAndMetadata{"
                + "offset="
                + offset
                + ", leaderEpoch="
                + leaderEpoch
                + ", metadata='"
                + metadata
                + '\''
                + ", commitTimestampMs="
                + commitTimestampMs
                + '}';
    }
}
