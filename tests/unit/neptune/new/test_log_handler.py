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
import logging
import os
import unittest
from contextlib import redirect_stdout
from io import StringIO

from mock import patch

from neptune import (
    ANONYMOUS_API_TOKEN,
    init_run,
)
from neptune.envs import (
    API_TOKEN_ENV_NAME,
    PROJECT_ENV_NAME,
)
from neptune.integrations.python_logger import NeptuneHandler
from neptune.internal.utils.logger import (
    LOGGER_NAME,
    CommonPrefixLogger,
)


@patch("neptune.metadata_containers.run.generate_hash", lambda *vals, length: "some_hash")
class TestLogHandler(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        os.environ[PROJECT_ENV_NAME] = "organization/project"
        os.environ[API_TOKEN_ENV_NAME] = ANONYMOUS_API_TOKEN

    def _log_messages(self, logger):
        logger.error("error message")
        logger.debug("debug message")
        logger.warning("test %s", "message")

    def test_no_run(self):
        with self.assertRaises(TypeError):
            NeptuneHandler(run="PET-1")

    def test_default_attribute(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            handler = NeptuneHandler(run=exp)
            logger = logging.getLogger()
            logger.addHandler(handler)

            self._log_messages(logger)

            log_entries = list(exp["monitoring"]["some_hash"]["python_logger"].fetch_values().value)
            self.assertListEqual(log_entries, ["error message", "test message"])

    def test_custom_monitoring_namespace(self):
        with init_run(mode="debug", flush_period=0.5, monitoring_namespace="watching") as exp:
            handler = NeptuneHandler(run=exp)
            logger = logging.getLogger()
            logger.addHandler(handler)

            self._log_messages(logger)

            log_entries = list(exp["watching"]["python_logger"].fetch_values().value)
            self.assertListEqual(log_entries, ["error message", "test message"])

    def test_custom_target_attribute(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            handler = NeptuneHandler(run=exp, path="logging/my/logger")
            logger = logging.getLogger()
            logger.addHandler(handler)

            self._log_messages(logger)

            log_entries = list(exp["logging"]["my"]["logger"].fetch_values().value)
            self.assertListEqual(log_entries, ["error message", "test message"])
            self.assertNotIn("python_logger", exp.get_structure()["monitoring"]["some_hash"])

    def test_custom_level(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            handler = NeptuneHandler(run=exp, level=logging.ERROR)
            logger = logging.getLogger()
            logger.addHandler(handler)

            self._log_messages(logger)

            log_entries = list(exp["monitoring"]["some_hash"]["python_logger"].fetch_values().value)
            self.assertListEqual(log_entries, ["error message"])

    def test_formatter_works(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            handler = NeptuneHandler(run=exp)
            handler.setFormatter(logging.Formatter("%(levelname)s|%(name)s: %(message)s"))
            logger = logging.getLogger()
            logger.addHandler(handler)

            self._log_messages(logger)

            log_entries = list(exp["monitoring"]["some_hash"]["python_logger"].fetch_values().value)
            self.assertListEqual(log_entries, ["ERROR|root: error message", "WARNING|root: test message"])

    def test_log_level_works(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            handler = NeptuneHandler(run=exp)
            logger = logging.getLogger()
            logger.addHandler(handler)
            logger.setLevel(logging.DEBUG)

            self._log_messages(logger)

            log_entries = list(exp["monitoring"]["some_hash"]["python_logger"].fetch_values().value)
            self.assertListEqual(log_entries, ["error message", "debug message", "test message"])

            self._log_messages(logger)

    def test_log_level_works_with_level(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            handler = NeptuneHandler(run=exp, level=logging.WARNING)
            logger = logging.getLogger()
            logger.addHandler(handler)
            logger.setLevel(logging.DEBUG)

            self._log_messages(logger)

            log_entries = list(exp["monitoring"]["some_hash"]["python_logger"].fetch_values().value)
            self.assertListEqual(log_entries, ["error message", "test message"])

            handler.setLevel(logging.ERROR)

            self._log_messages(logger)

            log_entries = list(exp["monitoring"]["some_hash"]["python_logger"].fetch_values().value)
            self.assertListEqual(log_entries, ["error message", "test message", "error message"])

    def test_logger_default_handler_stdout_format(self):
        # given
        local_logger_name = "local-logger"
        logger = logging.getLogger(local_logger_name)
        stream = StringIO()

        # when
        with redirect_stdout(stream):
            logger.info("message")

        # then
        self.assertEqual(stream.getvalue(), f"{LOGGER_NAME}:{local_logger_name} message\n")

    def test_logger_is_correct_instance(self):
        # given
        logger = logging.getLogger("local-logger")

        # then
        assert isinstance(logger, CommonPrefixLogger)
