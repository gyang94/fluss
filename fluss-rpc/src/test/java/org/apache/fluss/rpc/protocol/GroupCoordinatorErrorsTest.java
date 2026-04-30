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

package org.apache.fluss.rpc.protocol;

import org.apache.fluss.exception.CoordinatorLoadInProgressException;
import org.apache.fluss.exception.CoordinatorNotAvailableException;
import org.apache.fluss.exception.IllegalGenerationException;
import org.apache.fluss.exception.InvalidCommitOffsetException;
import org.apache.fluss.exception.InvalidGroupIdException;
import org.apache.fluss.exception.NotCoordinatorException;
import org.apache.fluss.exception.UnknownMemberIdException;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for GroupCoordinator-related error codes. */
class GroupCoordinatorErrorsTest {

    @Test
    void testNotCoordinatorErrorCode() {
        Errors error = Errors.forException(new NotCoordinatorException("test"));
        assertThat(error).isEqualTo(Errors.NOT_COORDINATOR_EXCEPTION);
        assertThat(error.code()).isEqualTo(70);
    }

    @Test
    void testCoordinatorLoadInProgressErrorCode() {
        Errors error = Errors.forException(new CoordinatorLoadInProgressException("test"));
        assertThat(error).isEqualTo(Errors.COORDINATOR_LOAD_IN_PROGRESS_EXCEPTION);
        assertThat(error.code()).isEqualTo(71);
    }

    @Test
    void testCoordinatorNotAvailableErrorCode() {
        Errors error = Errors.forException(new CoordinatorNotAvailableException("test"));
        assertThat(error).isEqualTo(Errors.COORDINATOR_NOT_AVAILABLE_EXCEPTION);
        assertThat(error.code()).isEqualTo(72);
    }

    @Test
    void testInvalidGroupIdErrorCode() {
        Errors error = Errors.forException(new InvalidGroupIdException("test"));
        assertThat(error).isEqualTo(Errors.INVALID_GROUP_ID_EXCEPTION);
        assertThat(error.code()).isEqualTo(73);
    }

    @Test
    void testInvalidCommitOffsetErrorCode() {
        Errors error = Errors.forException(new InvalidCommitOffsetException("test"));
        assertThat(error).isEqualTo(Errors.INVALID_COMMIT_OFFSET_EXCEPTION);
        assertThat(error.code()).isEqualTo(74);
    }

    @Test
    void testIllegalGenerationErrorCode() {
        Errors error = Errors.forException(new IllegalGenerationException("test"));
        assertThat(error).isEqualTo(Errors.ILLEGAL_GENERATION_EXCEPTION);
        assertThat(error.code()).isEqualTo(75);
    }

    @Test
    void testUnknownMemberIdErrorCode() {
        Errors error = Errors.forException(new UnknownMemberIdException("test"));
        assertThat(error).isEqualTo(Errors.UNKNOWN_MEMBER_ID_EXCEPTION);
        assertThat(error.code()).isEqualTo(76);
    }

    @Test
    void testRoundTripByCode() {
        assertThat(Errors.forCode(70)).isEqualTo(Errors.NOT_COORDINATOR_EXCEPTION);
        assertThat(Errors.forCode(71)).isEqualTo(Errors.COORDINATOR_LOAD_IN_PROGRESS_EXCEPTION);
        assertThat(Errors.forCode(72)).isEqualTo(Errors.COORDINATOR_NOT_AVAILABLE_EXCEPTION);
        assertThat(Errors.forCode(73)).isEqualTo(Errors.INVALID_GROUP_ID_EXCEPTION);
        assertThat(Errors.forCode(74)).isEqualTo(Errors.INVALID_COMMIT_OFFSET_EXCEPTION);
        assertThat(Errors.forCode(75)).isEqualTo(Errors.ILLEGAL_GENERATION_EXCEPTION);
        assertThat(Errors.forCode(76)).isEqualTo(Errors.UNKNOWN_MEMBER_ID_EXCEPTION);
    }
}
