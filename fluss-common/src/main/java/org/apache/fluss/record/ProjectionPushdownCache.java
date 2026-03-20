/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.record;

import org.apache.fluss.record.FileLogProjection.ProjectionInfo;
import org.apache.fluss.shaded.guava32.com.google.common.cache.Cache;
import org.apache.fluss.shaded.guava32.com.google.common.cache.CacheBuilder;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;

/**
 * A cache for projection pushdown information. The cache key is composed of table id, schema id,
 * the stored partial-batch shape, the resolved latest-schema column ids, and the latest projected
 * output signature. The cache is designed to be shared across different tables and schemas.
 */
@ThreadSafe
public class ProjectionPushdownCache {

    final Cache<ProjectionKey, ProjectionInfo> projectionCache;

    public ProjectionPushdownCache() {
        // currently, the cache is shared at TabletServer level, so we give a large max size, but
        // give a short expiration time.
        // TODO: make the cache parameter configurable
        this.projectionCache =
                CacheBuilder.newBuilder()
                        .maximumSize(1000)
                        .expireAfterAccess(Duration.ofMinutes(3))
                        .build();
    }

    @Nullable
    public ProjectionInfo getProjectionInfo(
            long tableId,
            short schemaId,
            @Nullable int[] storedTargetColumns,
            int[] requestedColumnIds,
            String requestedOutputSignature) {
        ProjectionKey key =
                new ProjectionKey(
                        tableId,
                        schemaId,
                        storedTargetColumns,
                        requestedColumnIds,
                        requestedOutputSignature);
        return projectionCache.getIfPresent(key);
    }

    public void setProjectionInfo(
            long tableId,
            short schemaId,
            @Nullable int[] storedTargetColumns,
            int[] requestedColumnIds,
            String requestedOutputSignature,
            ProjectionInfo projectionInfo) {
        ProjectionKey key =
                new ProjectionKey(
                        tableId,
                        schemaId,
                        storedTargetColumns,
                        requestedColumnIds,
                        requestedOutputSignature);
        projectionCache.put(key, projectionInfo);
    }

    static final class ProjectionKey {
        private final long tableId;
        private final short schemaId;
        @Nullable private final int[] storedTargetColumns;
        private final int[] requestedColumnIds;
        private final String requestedOutputSignature;

        ProjectionKey(
                long tableId,
                short schemaId,
                @Nullable int[] storedTargetColumns,
                int[] requestedColumnIds,
                String requestedOutputSignature) {
            this.tableId = tableId;
            this.schemaId = schemaId;
            this.storedTargetColumns =
                    storedTargetColumns == null
                            ? null
                            : Arrays.copyOf(storedTargetColumns, storedTargetColumns.length);
            this.requestedColumnIds = Arrays.copyOf(requestedColumnIds, requestedColumnIds.length);
            this.requestedOutputSignature = requestedOutputSignature;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof ProjectionKey)) {
                return false;
            }
            ProjectionKey that = (ProjectionKey) o;
            return tableId == that.tableId
                    && schemaId == that.schemaId
                    && Arrays.equals(storedTargetColumns, that.storedTargetColumns)
                    && Arrays.equals(requestedColumnIds, that.requestedColumnIds)
                    && requestedOutputSignature.equals(that.requestedOutputSignature);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    tableId,
                    schemaId,
                    Arrays.hashCode(storedTargetColumns),
                    Arrays.hashCode(requestedColumnIds),
                    requestedOutputSignature);
        }
    }
}
