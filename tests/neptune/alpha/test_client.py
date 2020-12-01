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

from mock import patch, Mock

from neptune.alpha import init, ANONYMOUS, get_project
from neptune.alpha.attributes.atoms import String
from neptune.alpha.envs import PROJECT_ENV_NAME, API_TOKEN_ENV_NAME
from neptune.alpha.exceptions import MetadataInconsistency, OfflineModeFetchException
from neptune.alpha.internal.backends.api_model import Experiment, Attribute, AttributeType, LeaderboardEntry, \
    AttributeWithProperties
from neptune.alpha.internal.backends.neptune_backend_mock import NeptuneBackendMock


@patch('neptune.alpha.internal.init_impl.HostedNeptuneBackend', NeptuneBackendMock)
class TestClient(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        os.environ[PROJECT_ENV_NAME] = "organization/project"
        os.environ[API_TOKEN_ENV_NAME] = ANONYMOUS

    def test_incorrect_mode(self):
        with self.assertRaises(ValueError):
            init(connection_mode='srtgj')

    def test_debug_mode(self):
        exp = init(connection_mode='debug')
        exp["some/variable"] = 13
        self.assertEqual(13, exp["some/variable"].get())
        self.assertNotIn(str(exp._uuid), os.listdir(".neptune"))

    def test_offline_mode(self):
        exp = init(connection_mode='offline')
        exp["some/variable"] = 13
        with self.assertRaises(OfflineModeFetchException):
            exp["some/variable"].get()
        self.assertIn(str(exp._uuid), os.listdir(".neptune/offline"))
        self.assertIn("data-1.log", os.listdir(".neptune/offline/{}".format(exp._uuid)))

    def test_sync_mode(self):
        exp = init(connection_mode='sync')
        exp["some/variable"] = 13
        self.assertEqual(13, exp["some/variable"].get())
        self.assertNotIn(str(exp._uuid), os.listdir(".neptune"))

    def test_async_mode(self):
        exp = init(connection_mode='async', flush_period=0.5)
        exp["some/variable"] = 13
        # TODO: Should be None or exception?
        # self.assertEqual(None, exp["some/variable"].get())
        with self.assertRaises(MetadataInconsistency):
            exp["some/variable"].get()
        exp.wait()
        self.assertEqual(13, exp["some/variable"].get())
        self.assertIn(str(exp._uuid), os.listdir(".neptune/async"))
        execution_dir = os.listdir(".neptune/async/{}".format(exp._uuid))[0]
        self.assertIn("data-1.log", os.listdir(".neptune/async/{}/{}".format(exp._uuid, execution_dir)))

    @patch("neptune.alpha.internal.backends.neptune_backend_mock.NeptuneBackendMock.get_experiment",
           new=lambda _, _id:
           Experiment(uuid.UUID('12345678-1234-5678-1234-567812345678'), "SAN-94", "workspace", "sandbox"))
    @patch("neptune.alpha.internal.backends.neptune_backend_mock.NeptuneBackendMock.get_attributes",
           new=lambda _, _uuid: [Attribute("test", AttributeType.STRING)])
    def test_resume(self):
        exp = init(flush_period=0.5, experiment="SAN-94")
        self.assertEqual(exp._uuid, uuid.UUID('12345678-1234-5678-1234-567812345678'))
        self.assertIsInstance(exp.get_structure()["test"], String)

    @patch("neptune.alpha.internal.get_project_impl.HostedNeptuneBackend")
    def test_get_table_as_pands(self, backend_init_mock):
        # given
        backend_mock = Mock()
        backend_init_mock.return_value = backend_mock

        # and
        attributes = []
        now = datetime.now()
        attributes.append(AttributeWithProperties("experiment/state",
                                                  AttributeType.EXPERIMENT_STATE,
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
        df = get_project().get_experiments_table().as_pandas()

        # then
        self.assertEqual("idle", df['experiment/state'][1])
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

    @patch("neptune.alpha.internal.get_project_impl.HostedNeptuneBackend")
    def test_get_table_as_experiments(self, backend_init_mock):
        # given
        backend_mock = Mock()
        backend_init_mock.return_value = backend_mock

        # and
        exp_id = uuid.uuid4()
        attributes = []
        now = datetime.now()
        attributes.append(AttributeWithProperties("experiment/state",
                                                  AttributeType.EXPERIMENT_STATE,
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
        exp = get_project().get_experiments_table().as_experiments()[0]

        # then
        self.assertEqual("idle", exp['experiment/state'].get())
        self.assertEqual("idle", exp['experiment']['state'].get())
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

        exp['file/set'].download_zip("some_directory")
        backend_mock.download_file_set.assert_called_with(exp_id, ["file", "set"], "some_directory")
