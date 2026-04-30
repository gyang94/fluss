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
import org.apache.fluss.exception.IllegalGenerationException;

/**
 * Manages consumer group metadata and validates offset commit requests against group state.
 *
 * <p>Currently only supports assign mode (generationId &lt; 0). Rebalance mode will be added in a
 * future release.
 */
@Internal
public class GroupMetadataManager {

    /**
     * Validates that the given offset commit is allowed for the group.
     *
     * <p>In assign mode (generationId &lt; 0), commits are always allowed. Rebalance mode
     * (generationId &gt;= 0) is not yet supported.
     *
     * @param groupId the consumer group ID
     * @param memberId the member ID within the group
     * @param generationId the generation ID; negative values indicate assign mode
     * @throws IllegalGenerationException if rebalance mode is requested
     */
    public void validateOffsetCommit(String groupId, String memberId, int generationId) {
        if (generationId < 0) {
            return;
        }
        throw new IllegalGenerationException(
                "Group "
                        + groupId
                        + ": rebalance mode not yet supported, use generationId=-1 for assign mode");
    }
}
