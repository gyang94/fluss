# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Error types mapped to stable CLI exit codes."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, ClassVar


@dataclass
class E2EError(Exception):
    message: str
    details: dict[str, Any] = field(default_factory=dict)
    section: dict[str, Any] | None = None

    exit_code: ClassVar[int] = 1
    error_type: ClassVar[str] = "E2EError"

    def __post_init__(self) -> None:
        super().__init__(self.message)

    def to_error_block(self) -> dict[str, Any]:
        error = {"type": self.error_type, "message": self.message}
        if self.details:
            error["details"] = self.details
        return error


class ScenarioFailure(E2EError):
    exit_code = 1
    error_type = "ScenarioFailure"



class BuildError(E2EError):
    exit_code = 2
    error_type = "BuildError"


class ClusterError(E2EError):
    exit_code = 3
    error_type = "ClusterError"


class ServiceControlError(ClusterError):
    error_type = "ServiceControlError"


class ServiceCrashLoopError(ClusterError):
    error_type = "ServiceCrashLoopError"


class ServiceStartupTimeoutError(ClusterError):
    error_type = "ServiceStartupTimeoutError"


class ConfigError(E2EError):
    exit_code = 4
    error_type = "ConfigError"
