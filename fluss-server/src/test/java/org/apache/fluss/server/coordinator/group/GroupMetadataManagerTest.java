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

import org.apache.fluss.exception.IllegalGenerationException;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link GroupMetadataManager}. */
class GroupMetadataManagerTest {

    private final GroupMetadataManager manager = new GroupMetadataManager();

    @Test
    void testAssignModeWithNegativeGenerationIdPasses() {
        assertThatCode(() -> manager.validateOffsetCommit("test-group", "member-1", -1))
                .doesNotThrowAnyException();
    }

    @Test
    void testAssignModeWithMinValueGenerationIdPasses() {
        assertThatCode(
                        () ->
                                manager.validateOffsetCommit(
                                        "test-group", "member-1", Integer.MIN_VALUE))
                .doesNotThrowAnyException();
    }

    @Test
    void testRebalanceModeWithZeroGenerationIdThrows() {
        assertThatThrownBy(() -> manager.validateOffsetCommit("test-group", "member-1", 0))
                .isInstanceOf(IllegalGenerationException.class)
                .hasMessageContaining("rebalance mode not yet supported");
    }

    @Test
    void testRebalanceModeWithPositiveGenerationIdThrows() {
        assertThatThrownBy(() -> manager.validateOffsetCommit("test-group", "member-1", 5))
                .isInstanceOf(IllegalGenerationException.class)
                .hasMessageContaining("test-group")
                .hasMessageContaining("rebalance mode not yet supported");
    }
}
