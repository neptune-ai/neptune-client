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

from .experiment_view import ExperimentView, Namespace
from .variable import *

# pylint: disable=protected-access


class Experiment:

    def __init__(self, name=None):
        super().__init__()
        self.name = name
        self._members = {}

    def _log(self, string):
        print(f'Experiment {self.name}: {string}')

    def _get_variable(self, path):
        """
        path: list of strings
        """
        ref = self._members
        for segment in path:
            if segment in ref:
                ref = ref[segment]
            else:
                return None
        return ref

    def _set_variable(self, path, variable):
        """
        path: list of strings
        variable: Structure
        """
        namespace = self._members
        location, variable_name = path[:-1], path[-1]
        for segment in location:
            if segment in namespace:
                namespace = namespace[segment]
            else:
                namespace[segment] = Namespace()
                namespace = namespace[segment]
        namespace[variable_name] = variable

    def __getitem__(self, path):
        """
        path: string
        """
        return ExperimentView(self, parse_path(path))
