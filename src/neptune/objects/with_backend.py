#
# Copyright (c) 2024, Neptune Labs Sp. z o.o.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
__all__ = ["WithBackend"]

import abc
import traceback
from contextlib import AbstractContextManager
from typing import (
    Optional,
    Union,
)

from neptune.internal.backends.api_model import Project
from neptune.internal.backends.factory import get_backend
from neptune.internal.backends.neptune_backend import NeptuneBackend
from neptune.internal.backends.project_name_lookup import project_name_lookup
from neptune.internal.container_type import ContainerType
from neptune.internal.id_formats import (
    QualifiedName,
    UniqueId,
    conform_optional,
)
from neptune.internal.state import ContainerState
from neptune.internal.utils import verify_type
from neptune.objects.mode import Mode


class WithBackend(AbstractContextManager, abc.ABC):
    container_type: ContainerType

    def __init__(
        self,
        api_token: Optional[str] = None,
        project: Optional[str] = None,
        mode: Mode = Mode.ASYNC,
        proxies: Optional[dict] = None,
    ) -> None:
        verify_type("api_token", api_token, (str, type(None)))
        verify_type("mode", mode, Mode)
        verify_type("proxies", proxies, (dict, type(None)))
        verify_type("project", project, (str, type(None)))

        self._mode = mode
        self._backend: NeptuneBackend = get_backend(mode=mode, api_token=api_token, proxies=proxies)
        self._project_qualified_name: Optional[str] = conform_optional(project, QualifiedName)
        self._project_api_object: Project = project_name_lookup(
            backend=self._backend, name=self._project_qualified_name
        )
        self._workspace: str = self._project_api_object.workspace
        self._project_name: str = self._project_api_object.name
        self._project_id: UniqueId = self._project_api_object.id

        self._state: ContainerState = ContainerState.CREATED

    @abc.abstractmethod
    def stop(self, *, seconds: Optional[Union[float, int]] = None) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def _raise_if_stopped(self) -> None:
        raise NotImplementedError

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if exc_tb is not None:
            traceback.print_exception(exc_type, exc_val, exc_tb)
        self.stop()
