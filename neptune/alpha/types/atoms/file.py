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
from io import IOBase
from typing import TypeVar, TYPE_CHECKING, Optional

from neptune.alpha.internal.utils import verify_type
from neptune.alpha.types.atoms.atom import Atom

if TYPE_CHECKING:
    from neptune.alpha.types.value_visitor import ValueVisitor

Ret = TypeVar('Ret')


class File(Atom):

    def __init__(self, file_path: Optional[str] = None, stream: Optional[IOBase] = None):
        verify_type("file_path", file_path, (str, type(None)))
        verify_type("stream", stream, (IOBase, type(None)))
        self.file_path = file_path
        self.stream = stream

    def accept(self, visitor: 'ValueVisitor[Ret]') -> Ret:
        return visitor.visit_file(self)

    def __str__(self):
        if self.file_path is not None:
            return "File({})".format(str(self.file_path))
        else:
            return "File(stream)"
