#
# Copyright (c) 2020, Neptune Labs Sp. z o.o.
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

import uuid
from typing import Union, Optional, Iterable

from neptune.new.internal.backends.hosted_neptune_backend import HostedNeptuneBackend
from neptune.new.internal.utils import verify_type, verify_collection_type
from neptune.new.runs_table import RunsTable


class Project:

    def __init__(self,
                 _uuid: uuid.UUID,
                 backend: HostedNeptuneBackend):
        self._uuid = _uuid
        self._backend = backend

    # pylint:disable=redefined-builtin
    def fetch_runs_table(self,
                         id: Optional[Union[str, Iterable[str]]] = None,
                         state: Optional[Union[str, Iterable[str]]] = None,
                         owner: Optional[Union[str, Iterable[str]]] = None,
                         tag: Optional[Union[str, Iterable[str]]] = None
                         ) -> RunsTable:
        id = self._as_list("id", id)
        state = self._as_list("state", state)
        owner = self._as_list("owner", owner)
        tags = self._as_list("tag", tag)

        leaderboard_entries = self._backend.get_leaderboard(self._uuid, id, state, owner, tags)

        return RunsTable(self._backend, leaderboard_entries)

    @staticmethod
    def _as_list(name: str, value: Optional[Union[str, Iterable[str]]]) -> Optional[Iterable[str]]:
        verify_type(name, value, (type(None), str, Iterable))
        if value is None:
            return None
        if isinstance(value, str):
            return [value]
        verify_collection_type(name, value, str)
        return value
