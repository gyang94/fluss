/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.lakehouse.paimon.source.split;

import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;

import javax.annotation.Nullable;

import java.util.Objects;

/** The split for snapshot. It's used to describe a snapshot of a table bucket. */
public abstract class SnapshotSplit extends SourceSplitBase {

    /** The records to skip when reading the snapshot. */
    private final long recordsToSkip;

    /** The snapshot id. It's used to identify the snapshot for a kv bucket. */
    protected final long snapshotId;

    public SnapshotSplit(
            TablePath tablePath,
            TableBucket tableBucket,
            @Nullable String partitionName,
            long snapshotId,
            long recordsToSkip) {
        super(tablePath, tableBucket, partitionName);
        this.snapshotId = snapshotId;
        this.recordsToSkip = recordsToSkip;
    }

    public long getSnapshotId() {
        return snapshotId;
    }

    public long recordsToSkip() {
        return recordsToSkip;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SnapshotSplit)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        SnapshotSplit that = (SnapshotSplit) o;
        return recordsToSkip == that.recordsToSkip && Objects.equals(snapshotId, that.snapshotId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), recordsToSkip, snapshotId);
    }

    @Override
    public String toString() {
        return "SnapshotSplit{"
                + "recordsToSkip="
                + recordsToSkip
                + ", snapshotId="
                + snapshotId
                + ", tablePath="
                + tablePath
                + ", tableBucket="
                + tableBucket
                + '}';
    }
}
