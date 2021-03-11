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

from neptune.new.experiment import Experiment


class Logger(object):

    def __init__(self, experiment: Experiment, attribute_name: str):
        self._experiment = experiment
        self._attribute_name = attribute_name

    def log(self, msg: str):
        self._experiment[self._attribute_name].log(msg)
