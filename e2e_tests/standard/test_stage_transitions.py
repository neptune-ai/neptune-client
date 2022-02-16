#
# Copyright (c) 2021, Neptune Labs Sp. z o.o.
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
import pytest

from neptune.new.metadata_containers import ModelVersion
from neptune.new.exceptions import NeptuneCannotChangeStageManually

from e2e_tests.base import BaseE2ETest


class TestStageTransitions(BaseE2ETest):
    @pytest.mark.parametrize("container", ["model_version"], indirect=True)
    def test_transitions(self, container: ModelVersion):
        container["a"] = 14

        assert container["sys/stage"].fetch() == "none"

        container.change_stage("staging")
        container.sync()

        assert container["sys/stage"].fetch() == "staging"

        container.change_stage("production")
        container.sync()

        assert container["sys/stage"].fetch() == "production"

        container.change_stage("none")
        container.sync()

        assert container["sys/stage"].fetch() == "none"

    @pytest.mark.parametrize("container", ["model_version"], indirect=True)
    def test_fail_on_unknown_stage_value(self, container: ModelVersion):
        with pytest.raises(ValueError):
            container.change_stage("unknown")
            container.sync()

    @pytest.mark.parametrize("container", ["model_version"], indirect=True)
    def test_fail_on_manual(self, container: ModelVersion):
        with pytest.raises(NeptuneCannotChangeStageManually):
            container["sys/stage"] = "staging"
            container.sync()
