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

# pylint: disable=protected-access

import os
import unittest
import uuid
from datetime import datetime

from mock import Mock, patch

from neptune.new import ANONYMOUS, Run, get_last_run, get_project, init
from neptune.new.attributes.atoms import String
from neptune.new.envs import API_TOKEN_ENV_NAME, PROJECT_ENV_NAME
from neptune.new.exceptions import (
    MetadataInconsistency, MissingFieldException, NeptuneOfflineModeFetchException, NeptuneUninitializedException,
)
from neptune.new.internal.backends.api_model import (
    ApiRun,
    Attribute,
    AttributeType,
    AttributeWithProperties,
    LeaderboardEntry,
    IntAttribute,
)
from neptune.new.internal.backends.neptune_backend_mock import NeptuneBackendMock


@patch('neptune.new.internal.init_impl.HostedNeptuneBackend', NeptuneBackendMock)
class TestClient(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        os.environ[PROJECT_ENV_NAME] = "organization/project"
        os.environ[API_TOKEN_ENV_NAME] = ANONYMOUS

    def test_incorrect_mode(self):
        with self.assertRaises(ValueError):
            init(mode='srtgj')

    def test_debug_mode(self):
        exp = init(mode='debug')
        exp["some/variable"] = 13
        self.assertEqual(13, exp["some/variable"].fetch())
        self.assertNotIn(str(exp._uuid), os.listdir(".neptune"))

    def test_offline_mode(self):
        exp = init(mode='offline')
        exp["some/variable"] = 13
        with self.assertRaises(NeptuneOfflineModeFetchException):
            exp["some/variable"].fetch()
        self.assertIn(str(exp._uuid), os.listdir(".neptune/offline"))
        self.assertIn("data-1.log", os.listdir(".neptune/offline/{}".format(exp._uuid)))

    def test_sync_mode(self):
        exp = init(mode='sync')
        exp["some/variable"] = 13
        self.assertEqual(13, exp["some/variable"].fetch())
        self.assertNotIn(str(exp._uuid), os.listdir(".neptune"))

    def test_async_mode(self):
        exp = init(mode='async', flush_period=0.5)
        exp["some/variable"] = 13
        with self.assertRaises(MetadataInconsistency):
            exp["some/variable"].fetch()
        exp.wait()
        self.assertEqual(13, exp["some/variable"].fetch())
        self.assertIn(str(exp._uuid), os.listdir(".neptune/async"))
        execution_dir = os.listdir(".neptune/async/{}".format(exp._uuid))[0]
        self.assertIn("data-1.log", os.listdir(".neptune/async/{}/{}".format(exp._uuid, execution_dir)))

    @patch("neptune.new.internal.backends.neptune_backend_mock.NeptuneBackendMock.get_run",
           new=lambda _, _id:
           ApiRun(uuid.UUID('12345678-1234-5678-1234-567812345678'), "SAN-94", "workspace", "sandbox", False))
    @patch("neptune.new.internal.backends.neptune_backend_mock.NeptuneBackendMock.get_attributes",
           new=lambda _, _uuid: [Attribute("some/variable", AttributeType.INT)])
    @patch("neptune.new.internal.backends.neptune_backend_mock.NeptuneBackendMock.get_int_attribute",
           new=lambda _, _uuid, _path: IntAttribute(42))
    def test_read_only_mode(self):
        exp = init(mode='read-only', run="SAN-94")

        with self.assertLogs() as caplog:
            exp["some/variable"] = 13
            exp["some/other_variable"] = 11
            self.assertEqual(
                caplog.output,
                ['WARNING:neptune.new.internal.operation_processors.read_only_operation_processor:'
                 'Client in read-only mode, nothing will be saved to server.']
            )

        self.assertEqual(42, exp["some/variable"].fetch())
        self.assertNotIn(str(exp._uuid), os.listdir(".neptune"))

    @patch("neptune.new.internal.backends.neptune_backend_mock.NeptuneBackendMock.get_run",
           new=lambda _, _id:
           ApiRun(uuid.UUID('12345678-1234-5678-1234-567812345678'), "SAN-94", "workspace", "sandbox", False))
    @patch("neptune.new.internal.backends.neptune_backend_mock.NeptuneBackendMock.get_attributes",
           new=lambda _, _uuid: [Attribute("test", AttributeType.STRING)])
    def test_resume(self):
        exp = init(flush_period=0.5, run="SAN-94")
        self.assertEqual(exp._uuid, uuid.UUID('12345678-1234-5678-1234-567812345678'))
        self.assertIsInstance(exp.get_structure()["test"], String)

    @patch("neptune.new.internal.utils.source_code.sys.argv", ["main.py"])
    @patch("neptune.new.internal.init_impl.os.path.isfile", new=lambda file: "." in file)
    @patch('neptune.new.internal.utils.glob', new=lambda path, recursive=False: [path.replace('*', 'file.txt')])
    @patch('neptune.new.internal.utils.os.path.abspath',
           new=lambda path: os.path.normpath("/home/user/main_dir/" + path))
    @patch('neptune.new.internal.utils.os.getcwd', new=lambda: "/home/user/main_dir")
    def test_entrypoint(self):
        exp = init(mode='debug')
        self.assertEqual(exp["source_code/entrypoint"].fetch(), "main.py")

        exp = init(mode='debug', source_files=[])
        self.assertEqual(exp["source_code/entrypoint"].fetch(), "main.py")

        exp = init(mode='debug', source_files=["../*"])
        self.assertEqual(exp["source_code/entrypoint"].fetch(), "main_dir/main.py")

        exp = init(mode='debug', source_files=["internal/*"])
        self.assertEqual(exp["source_code/entrypoint"].fetch(), "main.py")

        exp = init(mode='debug', source_files=["../other_dir/*"])
        self.assertEqual(exp["source_code/entrypoint"].fetch(), "../main_dir/main.py")

    @patch("neptune.new.internal.utils.source_code.sys.argv", ["main.py"])
    @patch("neptune.new.internal.utils.source_code.is_ipython", new=lambda: True)
    def test_entrypoint_in_interactive_python(self):
        exp = init(mode='debug')
        with self.assertRaises(MissingFieldException):
            exp["source_code/entrypoint"].get()

        exp = init(mode='debug', source_files=[])
        with self.assertRaises(MissingFieldException):
            exp["source_code/entrypoint"].get()

        exp = init(mode='debug', source_files=["../*"])
        with self.assertRaises(MissingFieldException):
            exp["source_code/entrypoint"].get()

        exp = init(mode='debug', source_files=["internal/*"])
        with self.assertRaises(MissingFieldException):
            exp["source_code/entrypoint"].get()

    @patch("neptune.new.internal.utils.source_code.sys.argv", ["main.py"])
    @patch("neptune.new.internal.utils.source_code.get_common_root", new=lambda _: None)
    @patch("neptune.new.internal.init_impl.os.path.isfile", new=lambda file: "." in file)
    @patch('neptune.new.internal.utils.glob', new=lambda path, recursive=False: [path.replace('*', 'file.txt')])
    @patch('neptune.new.internal.utils.os.path.abspath',
           new=lambda path: os.path.normpath("/home/user/main_dir/" + path))
    def test_entrypoint_without_common_root(self):
        exp = init(mode='debug', source_files=["../*"])
        self.assertEqual(exp["source_code/entrypoint"].fetch(), "/home/user/main_dir/main.py")

        exp = init(mode='debug', source_files=["internal/*"])
        self.assertEqual(exp["source_code/entrypoint"].fetch(), "/home/user/main_dir/main.py")

    @patch("neptune.new.internal.get_project_impl.HostedNeptuneBackend")
    def test_get_table_as_pandas(self, backend_init_mock):
        # given
        backend_mock = Mock()
        backend_init_mock.return_value = backend_mock

        # and
        attributes = []
        now = datetime.now()
        attributes.append(AttributeWithProperties("run/state",
                                                  AttributeType.RUN_STATE,
                                                  Mock(value="idle")))
        attributes.append(AttributeWithProperties("float", AttributeType.FLOAT, Mock(value=12.5)))
        attributes.append(AttributeWithProperties("string", AttributeType.STRING, Mock(value="some text")))
        attributes.append(AttributeWithProperties("datetime", AttributeType.DATETIME, Mock(value=now)))
        attributes.append(AttributeWithProperties("float/series", AttributeType.FLOAT_SERIES, Mock(last=8.7)))
        attributes.append(AttributeWithProperties("string/series", AttributeType.STRING_SERIES, Mock(last="last text")))
        attributes.append(AttributeWithProperties("string/set", AttributeType.STRING_SET, Mock(values=["a", "b"])))
        attributes.append(AttributeWithProperties("git/ref",
                                                  AttributeType.GIT_REF,
                                                  Mock(commit=Mock(commitId="abcdef0123456789"))))
        attributes.append(AttributeWithProperties("file", AttributeType.FILE, None))
        attributes.append(AttributeWithProperties("file/set", AttributeType.FILE_SET, None))
        attributes.append(AttributeWithProperties("image/series", AttributeType.IMAGE_SERIES, None))

        # and
        empty_entry = LeaderboardEntry(uuid.uuid4(), [])
        filled_entry = LeaderboardEntry(uuid.uuid4(), attributes)
        backend_mock.get_leaderboard = Mock(return_value=[empty_entry, filled_entry])

        # when
        df = get_project().fetch_runs_table().to_pandas()

        # then
        self.assertEqual("idle", df['run/state'][1])
        self.assertEqual(12.5, df['float'][1])
        self.assertEqual("some text", df['string'][1])
        self.assertEqual(now, df['datetime'][1])
        self.assertEqual(8.7, df['float/series'][1])
        self.assertEqual("last text", df['string/series'][1])
        self.assertEqual("a,b", df['string/set'][1])
        self.assertEqual("abcdef0123456789", df['git/ref'][1])

        with self.assertRaises(KeyError):
            self.assertTrue(df['file'])
        with self.assertRaises(KeyError):
            self.assertTrue(df['file/set'])
        with self.assertRaises(KeyError):
            self.assertTrue(df['image/series'])

    @patch("neptune.new.internal.get_project_impl.HostedNeptuneBackend")
    def test_get_table_as_runs(self, backend_init_mock):
        # given
        backend_mock = Mock()
        backend_init_mock.return_value = backend_mock

        # and
        exp_id = uuid.uuid4()
        attributes = []
        now = datetime.now()
        attributes.append(AttributeWithProperties("run/state",
                                                  AttributeType.RUN_STATE,
                                                  Mock(value="idle")))
        attributes.append(AttributeWithProperties("float", AttributeType.FLOAT, Mock(value=12.5)))
        attributes.append(AttributeWithProperties("string", AttributeType.STRING, Mock(value="some text")))
        attributes.append(AttributeWithProperties("datetime", AttributeType.DATETIME, Mock(value=now)))
        attributes.append(AttributeWithProperties("float/series", AttributeType.FLOAT_SERIES, Mock(last=8.7)))
        attributes.append(AttributeWithProperties("string/series", AttributeType.STRING_SERIES, Mock(last="last text")))
        attributes.append(AttributeWithProperties("string/set", AttributeType.STRING_SET, Mock(values=["a", "b"])))
        attributes.append(AttributeWithProperties("git/ref",
                                                  AttributeType.GIT_REF,
                                                  Mock(commit=Mock(commitId="abcdef0123456789"))))
        attributes.append(AttributeWithProperties("file", AttributeType.FILE, None))
        attributes.append(AttributeWithProperties("file/set", AttributeType.FILE_SET, None))
        attributes.append(AttributeWithProperties("image/series", AttributeType.IMAGE_SERIES, None))

        # and
        backend_mock.get_leaderboard = Mock(return_value=[LeaderboardEntry(exp_id, attributes)])

        # when
        exp = get_project().fetch_runs_table().to_runs()[0]

        # then
        self.assertEqual("idle", exp['run/state'].get())
        self.assertEqual("idle", exp['run']['state'].get())
        self.assertEqual(12.5, exp['float'].get())
        self.assertEqual("some text", exp['string'].get())
        self.assertEqual(now, exp['datetime'].get())
        self.assertEqual(8.7, exp['float/series'].get())
        self.assertEqual("last text", exp['string/series'].get())
        self.assertEqual({"a", "b"}, exp['string/set'].get())
        self.assertEqual("abcdef0123456789", exp['git/ref'].get())

        with self.assertRaises(MetadataInconsistency):
            exp['file'].get()
        with self.assertRaises(MetadataInconsistency):
            exp['file/set'].get()
        with self.assertRaises(MetadataInconsistency):
            exp['image/series'].get()

        exp['file'].download("some_directory")
        backend_mock.download_file.assert_called_with(exp_id, ["file"], "some_directory")

        exp['file/set'].download("some_directory")
        backend_mock.download_file_set.assert_called_with(exp_id, ["file", "set"], "some_directory")

    def test_last_exp_is_raising_exception_when_non_initialized(self):
        # given uninitialized run
        Run.last_run = None

        # expect: raises NeptuneUninitializedException
        with self.assertRaises(NeptuneUninitializedException):
            get_last_run()

    def test_last_exp_is_the_latest_initialized(self):
        # given two initialized runs
        exp1 = init()
        exp2 = init()

        # expect: `neptune.latest_run` to be the latest initialized one
        self.assertIsNot(exp1, get_last_run())
        self.assertIs(exp2, get_last_run())
