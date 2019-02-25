#
# Copyright (c) 2019, Neptune Labs Sp. z o.o.
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
import unittest

from click.testing import CliRunner

from cli import main


class TestCLI(unittest.TestCase):
    runner = CliRunner()

    def test_cli(self):
        result = self.runner.invoke(main.hello)
        self.assertEqual(result.exit_code, 0)
        self.assertIsNone(result.exception)
        self.assertEqual(result.output.strip(), 'Hello, world.')

    def test_cli_with_option(self):
        result = self.runner.invoke(main.hello, ['--as-cowboy'])
        self.assertIsNone(result.exception)
        self.assertEqual(result.exit_code, 0)
        self.assertEqual(result.output.strip(), 'Howdy, world.')

    def test_cli_with_arg(self):
        result = self.runner.invoke(main.hello, ['Neptune'])
        self.assertEqual(result.exit_code, 0)
        self.assertIsNone(result.exception)
        self.assertEqual(result.output.strip(), 'Hello, Neptune.')
