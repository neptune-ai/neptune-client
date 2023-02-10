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
import pickle
import unittest
from io import (
    BytesIO,
    StringIO,
)

import numpy
from bokeh.plotting import figure
from PIL import Image

from neptune.exceptions import (
    NeptuneException,
    StreamAlreadyUsedException,
)
from neptune.internal.types.file_types import (
    FileType,
    InMemoryComposite,
)
from neptune.internal.utils.images import _get_pil_image_data
from neptune.types import File
from tests.e2e.utils import tmp_context


class TestFile(unittest.TestCase):
    def test_file_constructor(self):
        """Almost all usages of `File` class consists of passing `path` to `File` constructor"""
        paths = ["some/path.png", "file.ext", "file/without/ext"]
        for path in paths:
            file1 = File(path)
            file2 = File.from_path(path)
            self.assertEqual(file1.path, file2.path)
            self.assertEqual(file1.extension, file2.extension)
            self.assertEqual(file1.file_type, file2.file_type)

    def _test_local_file(self, path: str, expected_ext: str, custom_ext=None):
        file = File.from_path(path, extension=custom_ext)
        self.assertIs(file.file_type, FileType.LOCAL_FILE)
        self.assertEqual(path, file.path)
        with self.assertRaises(NeptuneException):
            _ = file.content
        with self.assertRaises(NeptuneException):
            file._save(None)
        self.assertEqual(expected_ext, file.extension)

    def test_create_from_path(self):
        self._test_local_file("some/path.ext", expected_ext="ext")
        self._test_local_file("some/path.txt.ext", expected_ext="ext")
        self._test_local_file("so.me/path", expected_ext="")
        self._test_local_file("some/path.ext", expected_ext="txt", custom_ext="txt")

    @staticmethod
    def _save_and_return_content(file: File):
        """Saves content in tmp location and returns saved data"""
        with tmp_context() as tmp:
            file_name = "saved_file"
            file_path = f"{tmp}/{file_name}.{file.extension}"
            file._save(file_path)
            with open(file_path, "rb") as f:
                return f.read()

    def _test_in_memory_file(self, content, expected_ext, custom_extension=None):
        file = File.from_content(content, extension=custom_extension)
        bin_content = content.encode() if isinstance(content, str) else content

        self.assertIs(file.file_type, FileType.IN_MEMORY)
        with self.assertRaises(NeptuneException):
            _ = file.path
        self.assertEqual(bin_content, file.content)
        self.assertEqual(bin_content, self._save_and_return_content(file))
        self.assertEqual(expected_ext, file.extension)

    def test_create_from_string_content(self):
        self._test_in_memory_file("some_content", expected_ext="txt")
        self._test_in_memory_file("some_content", expected_ext="png", custom_extension="png")

    def test_create_from_bytes_content(self):
        self._test_in_memory_file(b"some_content", expected_ext="bin")
        self._test_in_memory_file(b"some_content", expected_ext="png", custom_extension="png")

    def _test_stream_content(self, file_producer, expected_content, expected_ext):
        """We can read content of the stream only once, so expect `stream_producer`"""
        file = file_producer()
        self.assertIs(file.file_type, FileType.STREAM)
        with self.assertRaises(NeptuneException):
            _ = file.path
        self.assertEqual(expected_ext, file.extension)

        file = file_producer()
        self.assertEqual(expected_content, file.content)
        file = file_producer()
        self.assertEqual(expected_content, self._save_and_return_content(file))

    def test_create_from_string_io(self):
        def _file_from_seeked_stream():
            stream = StringIO("aaabbbccc")
            stream.seek(3)  # should not affect created `File`
            return File.from_stream(stream)

        self._test_stream_content(
            lambda: File.from_stream(StringIO("aaabbbccc")), expected_content=b"aaabbbccc", expected_ext="txt"
        )
        self._test_stream_content(_file_from_seeked_stream, expected_content=b"aaabbbccc", expected_ext="txt")
        self._test_stream_content(
            lambda: File.from_stream(StringIO("aaabbbccc"), extension="png"),
            expected_content=b"aaabbbccc",
            expected_ext="png",
        )
        self._test_stream_content(
            lambda: File.from_stream(StringIO("aaabbbccc"), seek=5), expected_content=b"bccc", expected_ext="txt"
        )

    def test_create_from_bytes_io(self):
        def _file_from_seeked_stream():
            stream = BytesIO(b"aaabbbccc")
            stream.seek(3)  # should not affect created `File`
            return File.from_stream(stream)

        self._test_stream_content(
            lambda: File.from_stream(BytesIO(b"aaabbbccc")), expected_content=b"aaabbbccc", expected_ext="bin"
        )
        self._test_stream_content(_file_from_seeked_stream, expected_content=b"aaabbbccc", expected_ext="bin")
        self._test_stream_content(
            lambda: File.from_stream(BytesIO(b"aaabbbccc"), extension="png"),
            expected_content=b"aaabbbccc",
            expected_ext="png",
        )
        self._test_stream_content(
            lambda: File.from_stream(BytesIO(b"aaabbbccc"), seek=5), expected_content=b"bccc", expected_ext="bin"
        )

    def test_stream_read_once(self):
        file = File.from_stream(StringIO("aaabbbccc"))
        _ = file.content
        with self.assertRaises(StreamAlreadyUsedException):
            _ = file.content

        file = File.from_stream(StringIO("aaabbbccc"))
        self._save_and_return_content(file)
        with self.assertRaises(StreamAlreadyUsedException):
            self._save_and_return_content(file)

        file = File.from_stream(StringIO("aaabbbccc"))
        _ = file.content
        with self.assertRaises(StreamAlreadyUsedException):
            self._save_and_return_content(file)

        file = File.from_stream(StringIO("aaabbbccc"))
        self._save_and_return_content(file)
        with self.assertRaises(StreamAlreadyUsedException):
            _ = file.content

    def test_as_image(self):
        # given
        image_array = numpy.random.rand(10, 10) * 255
        expected_image = Image.fromarray(image_array.astype(numpy.uint8))

        # when
        file = File.as_image(expected_image)

        # then
        self.assertEqual(file.extension, "png")
        self.assertEqual(file.content, _get_pil_image_data(expected_image))

    def test_as_html(self):
        # given
        p = figure(width=400, height=400)
        p.circle(size=20, color="navy", alpha=0.5)

        # when
        file = File.as_html(p)

        # then
        self.assertEqual(file.extension, "html")
        self.assertTrue(file.content.lstrip().startswith('<!DOCTYPE html>\n<html lang="en">'.encode("utf-8")))

    def test_as_pickle(self):
        # given
        obj = {"a": [b"xyz", 34], "b": 1246}

        # when
        file = File.as_pickle(obj)

        # then
        self.assertEqual(file.extension, "pkl")
        self.assertEqual(file.content, pickle.dumps(obj))

    def test_raise_exception_in_constructor(self):
        with self.assertRaises(ValueError):
            File(path="path", file_composite=InMemoryComposite(b"some_content"))
        with self.assertRaises(ValueError):
            File()
