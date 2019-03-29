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

from neptune.internal.utils.git_info import get_git_info


class TestGitInfo(unittest.TestCase):
    def test_git_info(self):
        """This test gets current repository info, which is impossible to predict, but not null"""
        # when
        git_info = get_git_info()

        # then
        self.assertNotEqual(git_info, None)


if __name__ == '__main__':
    unittest.main()
