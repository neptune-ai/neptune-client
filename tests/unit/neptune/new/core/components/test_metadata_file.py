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
from pathlib import Path

from mock import (
    MagicMock,
    mock_open,
    patch,
)

from neptune.core.components.metadata_file import MetadataFile

sample_content = """
{
  "version": 5,
  "dependencies": [
    "a==1.0",
    "b==2.0"
  ]
}""".lstrip()


@patch("builtins.open", new_callable=mock_open)
def test_saving(mock_file):
    # given
    resolved_path = MagicMock(
        spec=Path,
        exists=lambda: False,
    )
    file_path = MagicMock(spec=Path, resolve=lambda strict: resolved_path)
    data_path = MagicMock(spec=Path, __truediv__=lambda self, key: file_path)

    # when
    with MetadataFile(data_path=data_path) as metadata:
        # and
        metadata["version"] = 5
        metadata["dependencies"] = ["a==1.0", "b==2.0"]

        # and
        metadata.flush()

        # then
        mock_file.assert_called_with(resolved_path, "w")

        # and - concatenate all written content
        write_calls = mock_file().write.call_args_list
        written_content = "".join(call[0][0] for call in write_calls)
        assert written_content == sample_content


@patch("builtins.open", new_callable=mock_open, read_data=sample_content)
def test_loading_existing_state(mock_file):
    # given
    resolved_path = MagicMock(spec=Path, exists=lambda: True)
    file_path = MagicMock(spec=Path, resolve=lambda strict: resolved_path)
    data_path = MagicMock(spec=Path, __truediv__=lambda self, key: file_path)

    # when
    with MetadataFile(data_path=data_path) as metadata:
        # then
        mock_file.assert_called_with(resolved_path, "r")

        # and
        assert metadata["version"] == 5
        assert metadata["dependencies"] == ["a==1.0", "b==2.0"]


@patch("os.remove")
@patch("builtins.open", MagicMock())
def test_cleaning(remove):
    # given
    resolved_path = MagicMock(
        spec=Path,
        exists=lambda: False,
    )
    file_path = MagicMock(spec=Path, resolve=lambda strict: resolved_path)
    data_path = MagicMock(spec=Path, __truediv__=lambda self, key: file_path)

    # when
    with MetadataFile(data_path=data_path) as metadata:
        # when
        metadata.cleanup()

        # then
        remove.assert_called_with(resolved_path)


@patch("builtins.open", new_callable=mock_open)
def test_initial_metadata(mock_file):
    # given
    resolved_path = MagicMock(
        spec=Path,
        exists=lambda: False,
    )
    file_path = MagicMock(spec=Path, resolve=lambda strict: resolved_path)
    data_path = MagicMock(spec=Path, __truediv__=lambda self, key: file_path)

    # when
    with MetadataFile(data_path=data_path, metadata={"version": 5, "dependencies": ["a==1.0", "b==2.0"]}):
        # then
        mock_file.assert_called_with(resolved_path, "w")

        # and - concatenate all written content
        write_calls = mock_file().write.call_args_list
        written_content = "".join(call[0][0] for call in write_calls)
        assert written_content == sample_content
