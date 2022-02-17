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
import re
import json
from pathlib import Path

import pytest
from click.testing import CliRunner

import neptune.new as neptune
from neptune.new.exceptions import NeptuneException
from neptune.new.sync import sync

from e2e_tests.base import BaseE2ETest, fake, AVAILABLE_CONTAINERS
from e2e_tests.utils import (
    DISABLE_SYSLOG_KWARGS,
    initialize_container,
    reinitialize_container,
    tmp_context,
)

runner = CliRunner()


class TestSync(BaseE2ETest):
    SYNCHRONIZED_SYSID_RE = r"[\w-]+/[\w-]+/([\w-]+)"

    @pytest.mark.parametrize("container_type", AVAILABLE_CONTAINERS)
    def test_sync_container(self, container_type, environment):
        with tmp_context() as tmp:
            key = self.gen_key()
            original_value = fake.unique.word()
            updated_value = fake.unique.word()

            with initialize_container(
                container_type=container_type, project=environment.project
            ) as container:
                # assign original value
                container[key] = original_value
                container.wait()
                # pylint: disable=protected-access
                container_id = container._id
                container_sys_id = container._sys_id

            # manually add operations to queue
            queue_dir = list(
                Path(f"./.neptune/async/{container_type}__{container_id}/").glob(
                    "exec-*"
                )
            )[0]
            with open(
                queue_dir / "last_put_version", encoding="utf-8"
            ) as last_put_version_f:
                last_put_version = int(last_put_version_f.read())
                with open(queue_dir / "data-1.log", "a", encoding="utf-8") as queue_f:
                    queue_f.write(
                        json.dumps(
                            {
                                "obj": {
                                    "type": "AssignString",
                                    "path": key.split("/"),
                                    "value": updated_value,
                                },
                                "version": last_put_version + 1,
                            }
                        )
                    )
                    queue_f.write(
                        json.dumps(
                            {
                                "obj": {
                                    "type": "CopyAttribute",
                                    "path": ["copy"] + key.split("/"),
                                    "container_id": container_id,
                                    "container_type": container_type,
                                    "source_path": key.split("/"),
                                    "source_attr_name": "String",
                                },
                                "version": last_put_version + 2,
                            }
                        )
                    )
                with open(
                    queue_dir / "last_put_version", "w", encoding="utf-8"
                ) as last_put_version_f:
                    last_put_version_f.write(str(last_put_version + 2))

            with reinitialize_container(
                container_sys_id, container_type, project=environment.project
            ) as container:
                # server should have the original value
                assert container[key].fetch() == original_value

            # run neptune sync
            result = runner.invoke(sync, ["--path", tmp])
            assert result.exit_code == 0

            with reinitialize_container(
                container_sys_id, container_type, project=environment.project
            ) as container:
                # and we should get the updated value from server
                assert container[key].fetch() == updated_value
                assert container["copy/" + key].fetch() == updated_value

    def test_offline_sync(self, environment):
        with tmp_context() as tmp:
            # create run in offline mode
            run = neptune.init(
                mode="offline",
                project=environment.project,
                **DISABLE_SYSLOG_KWARGS,
            )
            # assign some values
            key = self.gen_key()
            val = fake.word()
            run[key] = val

            # and stop it
            run.stop()

            # run asynchronously
            result = runner.invoke(sync, ["--path", tmp, "-p", environment.project])
            assert result.exit_code == 0

            # we'll have to parse sync output to determine short_id
            sys_id_found = re.search(self.SYNCHRONIZED_SYSID_RE, result.stdout)
            assert len(sys_id_found.groups()) == 1
            sys_id = sys_id_found.group(1)

            run2 = neptune.init_run(run=sys_id, project=environment.project)
            assert run2[key].fetch() == val

    @pytest.mark.parametrize("container_type", ["model", "model_version", "project"])
    def test_cannot_offline_non_runs(self, environment, container_type):
        with pytest.raises(NeptuneException) as e:
            initialize_container(
                container_type=container_type,
                project=environment.project,
                mode="offline",
            )
        assert "can't be initialized in OFFLINE mode" in str(e.value)
