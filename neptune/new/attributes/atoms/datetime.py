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
from datetime import datetime
from typing import Union

from neptune.new.attributes.atoms.atom import Atom
from neptune.new.internal.operation import AssignDatetime
from neptune.new.internal.utils import verify_type
from neptune.new.types.atoms.datetime import Datetime as DatetimeVal


class Datetime(Atom):

    def assign(self, value: Union[DatetimeVal, datetime], wait: bool = False):
        verify_type("value", value, (DatetimeVal, datetime))
        if isinstance(value, DatetimeVal):
            value = value.value
        else:
            value = value.replace(microsecond=1000 * int(value.microsecond / 1000))
        with self._run.lock():
            self._enqueue_operation(AssignDatetime(self._path, value), wait)

    def fetch(self) -> datetime:
        # pylint: disable=protected-access
        val = self._backend.get_datetime_attribute(self._run_uuid, self._path)
        return val.value
