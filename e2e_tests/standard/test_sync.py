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
import json
import re
from pathlib import Path

import neptune.new as neptune
from click.testing import CliRunner
from faker import Faker
from neptune.new.sync import sync

from e2e_tests.base import BaseE2ETest
from e2e_tests.utils import DISABLE_SYSLOG_KWARGS, tmp_context

fake = Faker()
runner = CliRunner()


class TestSync(BaseE2ETest):
    SYNCHRONIZED_SYSID_RE = r"\w+/[\w-]+/([\w-]+)"

    def test_sync_run(self, environment):
        custom_run_id = "-".join((fake.word() for _ in range(3)))

        with tmp_context() as tmp:
            # with test values
            key = self.gen_key()
            original_value = fake.word()
            updated_value = fake.word()

            # init run
            run = neptune.init(
                custom_run_id=custom_run_id,
                project=environment.project,
                **DISABLE_SYSLOG_KWARGS,
            )

            def get_next_run():
                return neptune.init(
                    custom_run_id=custom_run_id,
                    project=environment.project,
                    **DISABLE_SYSLOG_KWARGS,
                )

            self._test_sync(
                exp=run,
                get_next_exp=get_next_run,
                path=tmp,
                key=key,
                original_value=original_value,
                updated_value=updated_value,
            )

    def test_sync_project(self, environment):
        with tmp_context() as tmp:
            # with test values
            key = f"{self.gen_key()}-" + "-".join((fake.word() for _ in range(3)))
            original_value = fake.word()
            updated_value = fake.word()

            # init run
            project = neptune.init_project(name=environment.project)

            def get_next_project():
                return neptune.init_project(name=environment.project)

            self._test_sync(
                exp=project,
                get_next_exp=get_next_project,
                path=tmp,
                key=key,
                original_value=original_value,
                updated_value=updated_value,
            )

    @staticmethod
    def _test_sync(exp, get_next_exp, path, key, original_value, updated_value):
        # assign original value
        exp[key] = original_value
        exp.sync()

        # stop run
        exp.stop()

        # pylint: disable=protected-access
        queue_dir = list(Path(f"./.neptune/async/{exp._id}/").glob("exec-*"))[0]
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
                            "container_id": exp._id,
                            "container_type": exp.container_type.value,
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

        # other exp should see only original value from server
        exp2 = get_next_exp()
        assert exp2[key].fetch() == original_value

        # run neptune sync
        result = runner.invoke(sync, ["--path", path])
        assert result.exit_code == 0

        # other exp should see updated value from server
        exp3 = get_next_exp()
        assert exp3[key].fetch() == updated_value
        assert exp3["copy/" + key].fetch() == updated_value

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

            # offline mode doesn't support custom_run_id, we'll have to parse sync output to determine short_id
            sys_id_found = re.search(self.SYNCHRONIZED_SYSID_RE, result.stdout)
            assert len(sys_id_found.groups()) == 1
            sys_id = sys_id_found.group(1)

            run2 = neptune.init(run=sys_id, project=environment.project)
            assert run2[key].fetch() == val
