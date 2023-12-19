#
# Copyright (c) 2023, Neptune Labs Sp. z o.o.
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
from unittest.mock import patch

import pytest

from neptune.progress_bar import (
    ClickProgressBar,
    IPythonProgressBar,
    TqdmNotebookProgressBar,
    TqdmProgressBar,
    _handle_import_error,
)


@patch.dict("sys.modules", {"click": None})
def test_handle_import_error():
    @_handle_import_error(dependency="click")
    def func(x):
        import click  # noqa: F401

    with pytest.raises(ModuleNotFoundError) as exc_info:
        func(5)
        assert (
            str(exc_info.value) == "Required dependency for progress bar not found. Run 'pip install some_dependency'."
        )

    @_handle_import_error(dependency="urllib3")
    def func(x):
        import urllib3  # noqa: F401

    func(5)


def test_tqdm_progress_bar():
    with TqdmProgressBar(description="test_description") as progress_bar:
        progress_bar.update(by=10, total=100)
        assert progress_bar._progress_bar.total == 100
        assert progress_bar._progress_bar.desc == "test_description"

    with TqdmNotebookProgressBar(description="test_description") as progress_bar:
        progress_bar.update(by=10, total=100)
        assert progress_bar._progress_bar.total == 100
        assert progress_bar._progress_bar.desc == "test_description"


def test_click_progress_bar():
    with ClickProgressBar(description="test_description") as progress_bar:
        progress_bar.update(by=10, total=100)
        assert progress_bar._progress_bar.length == 100
        assert progress_bar._progress_bar.label == "test_description"


def test_ipython_progress_bar():
    with IPythonProgressBar(description="test_description") as progress_bar:
        progress_bar.update(by=10, total=100)
        assert progress_bar._progress_bar.max == 100
        assert progress_bar._progress_bar.description == "test_description"
