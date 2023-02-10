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
import sys
import threading
import unittest
from contextlib import redirect_stdout
from io import StringIO
from unittest.mock import MagicMock

from neptune.internal.streams.std_stream_capture_logger import (
    StdoutCaptureLogger,
    StdStreamCaptureLogger,
)
from neptune.logging import Logger as NeptuneLogger


class FakeLogger:
    def __init__(self, event_to_wait: threading.Event, real_logger: NeptuneLogger):
        self._event_to_wait = event_to_wait
        self._real_logger = real_logger

    def log(self, data):
        self._event_to_wait.wait()
        self._real_logger.log(data)


class TestStdStreamCaptureLogger(unittest.TestCase):
    def test_catches_stdout(self):
        stdout = StringIO()
        with redirect_stdout(stdout):
            mock_run = MagicMock()
            attr_name = "sys/stdout"
            logger = StdoutCaptureLogger(mock_run, attr_name)
            stdout_fp = sys.stdout
            print("testing", file=stdout_fp)
            logger.close()

            self.assertListEqual(
                mock_run[attr_name].log.call_args_list,
                [
                    (("testing",), {}),
                    (("\n",), {}),
                ],
            )
        stdout.seek(0)
        self.assertEqual(stdout.read(), "testing\n")

    def test_does_not_report_if_used_after_stop(self):
        stdout = StringIO()
        with redirect_stdout(stdout):
            mock_run = MagicMock()
            attr_name = "sys/stdout"
            logger = StdoutCaptureLogger(mock_run, attr_name)
            stdout_fp = sys.stdout
            logger.close()

            print("testing", file=stdout_fp)
            mock_run[attr_name].log.assert_not_called()
        stdout.seek(0)
        self.assertEqual(stdout.read(), "testing\n")

    def test_logger_with_lock_does_not_cause_deadlock(self):
        stream = StringIO()
        mock_run = MagicMock()
        attr_name = "sys/stdout"

        logger = StdStreamCaptureLogger(mock_run, attr_name, stream)
        done_waiting = threading.Event()
        logger._logger = FakeLogger(done_waiting, logger._logger)

        # The logger is blocked in background, the main thread is still awake
        logger.write("testing")
        self.assertListEqual(
            mock_run[attr_name].log.call_args_list,
            [],
        )

        done_waiting.set()
        logger.close()
        self.assertListEqual(
            mock_run[attr_name].log.call_args_list,
            [
                (("testing",), {}),
            ],
        )
        stream.seek(0)
        self.assertEqual(stream.read(), "testing")
