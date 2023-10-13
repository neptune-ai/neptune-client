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
from io import StringIO
from pathlib import Path

from mock import (
    MagicMock,
    patch,
)

from neptune.internal.metadata_file import MetadataFile

sample_content = """
{
  "version": 5,
  "dependencies": [
    "a==1.0",
    "b==2.0"
  ]
}""".lstrip()


@patch("os.makedirs")
@patch("builtins.open")
def test_saving(open_mock, makedirs):
    # given
    output_file = StringIO()
    open_mock.return_value = output_file

    # and
    file_path = MagicMock(
        spec=Path,
        resolve=lambda: MagicMock(
            spec=Path,
            exists=lambda: False,
        ),
    )
    data_path = MagicMock(spec=Path, __truediv__=lambda self, key: file_path)

    # when
    with MetadataFile(data_path=data_path) as metadata:
        # and
        metadata["version"] = 5
        metadata["dependencies"] = ["a==1.0", "b==2.0"]

        # and
        metadata.flush()

        # then
        makedirs.assert_called_with(data_path, exist_ok=True)
        assert output_file.getvalue() == sample_content


@patch("os.makedirs")
@patch("builtins.open")
def test_loading_existing_state(open_mock, makedirs):
    # given
    initial_file = StringIO(sample_content)
    output_file = StringIO()
    open_mock.side_effect = (initial_file, output_file)

    # and
    file_path = MagicMock(
        spec=Path,
        resolve=lambda: MagicMock(
            spec=Path,
            exists=lambda: True,
        ),
    )
    data_path = MagicMock(spec=Path, __truediv__=lambda self, key: file_path)

    # when
    with MetadataFile(data_path=data_path) as metadata:
        # then
        makedirs.assert_called_with(data_path, exist_ok=True)
        assert metadata["version"] == 5
        assert metadata["dependencies"] == ["a==1.0", "b==2.0"]

        # when
        metadata["value"] = 2501

        # and
        metadata.flush()

        # then
        assert (
            output_file.getvalue()
            == """
{
  "version": 5,
  "dependencies": [
    "a==1.0",
    "b==2.0"
  ],
  "value": 2501
}""".lstrip()
        )


@patch("os.makedirs")
@patch("os.remove")
@patch("builtins.open", return_value=StringIO())
def test_cleaning(open_mock, remove, makedirs):
    # given
    resolved_path = MagicMock(
        spec=Path,
        exists=lambda: False,
    )
    file_path = MagicMock(spec=Path, resolve=lambda: resolved_path)
    data_path = MagicMock(spec=Path, __truediv__=lambda self, key: file_path)

    # when
    with MetadataFile(data_path=data_path) as metadata:
        # when
        metadata.cleanup()

        # then
        makedirs.assert_called_with(data_path, exist_ok=True)
        remove.assert_called_with(resolved_path)
