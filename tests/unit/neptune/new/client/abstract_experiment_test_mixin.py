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
import contextlib
import os
import time
import unittest
from abc import abstractmethod
from io import StringIO
from unittest.mock import (
    Mock,
    patch,
)

from neptune.exceptions import (
    MetadataInconsistency,
    MissingFieldException,
    NeptuneOfflineModeFetchException,
    NeptuneSynchronizationAlreadyStoppedException,
    TypeDoesNotSupportAttributeException,
)


class AbstractExperimentTestMixin:
    @staticmethod
    @abstractmethod
    def call_init(**kwargs):
        pass

    def test_incorrect_mode(self):
        with self.assertRaises(ValueError):
            with self.call_init(mode="srtgj"):
                pass

    def test_debug_mode(self):
        with self.call_init(mode="debug") as exp:
            exp["some/variable"] = 13
            self.assertEqual(13, exp["some/variable"].fetch())
            self.assertNotIn(str(exp._id), os.listdir(".neptune"))

    @patch("neptune.internal.operation_processors.utils.random_key")
    @patch("neptune.internal.operation_processors.utils.os.getpid")
    def test_offline_mode(self, getpid_mock, random_mock):
        random_mock.return_value = "test"
        getpid_mock.return_value = 1234

        with self.call_init(mode="offline") as exp:
            exp["some/variable"] = 13
            with self.assertRaises(NeptuneOfflineModeFetchException):
                exp["some/variable"].fetch()

            exp_dir = f"{exp.container_type.value}__{exp._id}__1234__test"

            self.assertIn(exp_dir, os.listdir(".neptune/offline"))
            self.assertIn("data-1.log", os.listdir(f".neptune/offline/{exp_dir}"))

    def test_sync_mode(self):
        with self.call_init(mode="sync") as exp:
            exp["some/variable"] = 13
            exp["copied/variable"] = exp["some/variable"]
            self.assertEqual(13, exp["some/variable"].fetch())
            self.assertEqual(13, exp["copied/variable"].fetch())
            self.assertNotIn(str(exp._id), os.listdir(".neptune"))

    def test_async_mode(self):
        with patch("neptune.internal.operation_processors.utils.random_key") as random_mock:
            with patch("neptune.internal.operation_processors.utils.os.getpid") as getpid_mock:
                random_mock.return_value = "test"
                getpid_mock.return_value = 1234

                with self.call_init(mode="async", flush_period=0.5) as exp:
                    exp["some/variable"] = 13
                    exp["copied/variable"] = exp["some/variable"]
                    with self.assertRaises(MetadataInconsistency):
                        exp["some/variable"].fetch()
                    exp.wait()
                    self.assertEqual(13, exp["some/variable"].fetch())
                    self.assertEqual(13, exp["copied/variable"].fetch())

                    exp_dir = f"{exp.container_type.value}__{exp._id}__1234__test"
                    self.assertIn(exp_dir, os.listdir(".neptune/async"))
                    self.assertIn("data-1.log", os.listdir(f".neptune/async/{exp_dir}"))

    def test_async_mode_wait_on_dead(self):
        with self.call_init(mode="async", flush_period=0.5) as exp:
            exp._backend.execute_operations = Mock(side_effect=ValueError)
            exp["some/variable"] = 13
            # wait for the process to die
            time.sleep(1)
            with self.assertRaises(NeptuneSynchronizationAlreadyStoppedException):
                exp.wait()

    def test_async_mode_die_during_wait(self):
        with self.call_init(mode="async", flush_period=1) as exp:
            exp._backend.execute_operations = Mock(side_effect=ValueError)
            exp["some/variable"] = 13
            with self.assertRaises(NeptuneSynchronizationAlreadyStoppedException):
                exp.wait()

    @unittest.skip("NPT-12753 Flaky test")
    def test_async_mode_stop_on_dead(self):
        stream = StringIO()
        with contextlib.redirect_stdout(stream):
            with self.call_init(mode="async", flush_period=0.5) as exp:
                update_freq = 1
                default_freq = exp._op_processor.STOP_QUEUE_STATUS_UPDATE_FREQ_SECONDS
                try:
                    exp._op_processor.STOP_QUEUE_STATUS_UPDATE_FREQ_SECONDS = update_freq
                    exp._op_processor._backend.execute_operations = Mock(side_effect=ValueError)
                    exp["some/variable"] = 13
                    exp.stop()
                finally:
                    exp._op_processor.STOP_QUEUE_STATUS_UPDATE_FREQ_SECONDS = default_freq

        self.assertIn("NeptuneSynchronizationAlreadyStopped", stream.getvalue())

    def test_missing_attribute(self):
        with self.call_init(mode="debug") as exp:
            with self.assertRaises(MissingFieldException):
                exp["non/existing/path"].fetch()

    def test_wrong_function(self):
        with self.call_init(mode="debug") as exp:
            with self.assertRaises(AttributeError):
                exp["non/existing/path"].foo()

    def test_wrong_per_type_function(self):
        with self.call_init(mode="debug") as exp:
            exp["some/path"] = "foo"
            with self.assertRaises(TypeDoesNotSupportAttributeException):
                exp["some/path"].download()

    def test_clean_data_on_stop(self):
        with self.call_init(mode="async", flush_period=0.5) as exp:
            container_path = exp._op_processor.data_path

            assert os.path.exists(container_path)

            exp.stop()

            assert not os.path.exists(container_path)

    @abstractmethod
    def test_read_only_mode(self):
        pass
