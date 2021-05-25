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

from neptune.new import ANONYMOUS, init
from neptune.new.envs import API_TOKEN_ENV_NAME, PROJECT_ENV_NAME
from neptune.new.integrations.python_logger import NeptuneHandler


class TestLogHandler(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        os.environ[PROJECT_ENV_NAME] = "organization/project"
        os.environ[API_TOKEN_ENV_NAME] = ANONYMOUS

    def _log_messages(self, logger):
        logger.error("error message")
        logger.debug("debug message")
        logger.warning("test %s", "message")

    def test_no_run(self):
        with self.assertRaises(TypeError):
            NeptuneHandler(run="PET-1")

    def test_default_attribute(self):
        exp = init(mode="debug", flush_period=0.5)
        handler = NeptuneHandler(run=exp)
        logger = logging.getLogger()
        logger.addHandler(handler)

        self._log_messages(logger)

        log_entries = list(exp['monitoring']['python_logger'].fetch_values().value)
        self.assertListEqual(log_entries, ["error message", "test message"])

    def test_custom_monitoring_namespace(self):
        exp = init(mode="debug", flush_period=0.5, monitoring_namespace="watching")
        handler = NeptuneHandler(run=exp)
        logger = logging.getLogger()
        logger.addHandler(handler)

        self._log_messages(logger)

        log_entries = list(exp['watching']['python_logger'].fetch_values().value)
        self.assertListEqual(log_entries, ["error message", "test message"])

    def test_custom_target_attribute(self):
        exp = init(mode="debug", flush_period=0.5)
        handler = NeptuneHandler(run=exp, path="logging/my/logger")
        logger = logging.getLogger()
        logger.addHandler(handler)

        self._log_messages(logger)

        log_entries = list(exp['logging']['my']['logger'].fetch_values().value)
        self.assertListEqual(log_entries, ["error message", "test message"])
        self.assertNotIn('python_logger', exp.get_structure()['monitoring'])

    def test_custom_level(self):
        exp = init(mode="debug", flush_period=0.5)
        handler = NeptuneHandler(run=exp, level=logging.ERROR)
        logger = logging.getLogger()
        logger.addHandler(handler)

        self._log_messages(logger)

        log_entries = list(exp['monitoring']['python_logger'].fetch_values().value)
        self.assertListEqual(log_entries, ["error message"])

    def test_formatter_works(self):
        exp = init(mode="debug", flush_period=0.5)
        handler = NeptuneHandler(run=exp)
        handler.setFormatter(logging.Formatter("%(levelname)s|%(name)s: %(message)s"))
        logger = logging.getLogger()
        logger.addHandler(handler)

        self._log_messages(logger)

        log_entries = list(exp['monitoring']['python_logger'].fetch_values().value)
        self.assertListEqual(log_entries, ["ERROR|root: error message", "WARNING|root: test message"])

    def test_log_level_works(self):
        exp = init(mode="debug", flush_period=0.5)
        handler = NeptuneHandler(run=exp)
        logger = logging.getLogger()
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)

        self._log_messages(logger)

        log_entries = list(exp['monitoring']['python_logger'].fetch_values().value)
        self.assertListEqual(log_entries, ["error message", "debug message", "test message"])

        self._log_messages(logger)

    def test_log_level_works_with_level(self):
        exp = init(mode="debug", flush_period=0.5)
        handler = NeptuneHandler(run=exp, level=logging.WARNING)
        logger = logging.getLogger()
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)

        self._log_messages(logger)

        log_entries = list(exp['monitoring']['python_logger'].fetch_values().value)
        self.assertListEqual(log_entries, ["error message", "test message"])

        handler.setLevel(logging.ERROR)

        self._log_messages(logger)

        log_entries = list(exp['monitoring']['python_logger'].fetch_values().value)
        self.assertListEqual(log_entries, ["error message", "test message", "error message"])
