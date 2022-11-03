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

from neptune.new.exceptions import NeptuneException
from neptune.new.internal.utils.images import _get_pil_image_data
from neptune.new.types import File
from neptune.new.types.atoms.file import FileType


class TestFile(unittest.TestCase):
    def test_create_from_path(self):
        file = File("some/path.ext")
        self.assertIs(file.file_type, FileType.LOCAL_FILE)
        self.assertEqual("some/path.ext", file.path)
        with self.assertRaises(NeptuneException):
            _ = file.content
        self.assertEqual("ext", file.extension)

        file = File("some/path.txt.ext")
        self.assertIs(file.file_type, FileType.LOCAL_FILE)
        self.assertEqual("some/path.txt.ext", file.path)
        with self.assertRaises(NeptuneException):
            _ = file.content
        self.assertEqual("ext", file.extension)

        file = File("so.me/path")
        self.assertIs(file.file_type, FileType.LOCAL_FILE)
        self.assertEqual("so.me/path", file.path)
        with self.assertRaises(NeptuneException):
            _ = file.content
        self.assertEqual("", file.extension)

        file = File("some/path.ext", extension="txt")
        self.assertIs(file.file_type, FileType.LOCAL_FILE)
        self.assertEqual("some/path.ext", file.path)
        with self.assertRaises(NeptuneException):
            _ = file.content
        self.assertEqual("txt", file.extension)

    def test_create_from_string_content(self):
        file = File.from_content("some_content")
        self.assertIs(file.file_type, FileType.IN_MEMORY)
        with self.assertRaises(NeptuneException):
            _ = file.path
        self.assertEqual("some_content".encode("utf-8"), file.content)
        self.assertEqual("txt", file.extension)

        file = File.from_content("some_content", extension="png")
        self.assertIs(file.file_type, FileType.IN_MEMORY)
        with self.assertRaises(NeptuneException):
            _ = file.path
        self.assertEqual("some_content".encode("utf-8"), file.content)
        self.assertEqual("png", file.extension)

    def test_create_from_bytes_content(self):
        file = File.from_content(b"some_content")
        self.assertIs(file.file_type, FileType.IN_MEMORY)
        with self.assertRaises(NeptuneException):
            _ = file.path
        self.assertEqual(b"some_content", file.content)
        self.assertEqual("bin", file.extension)

        file = File.from_content(b"some_content", extension="png")
        self.assertIs(file.file_type, FileType.IN_MEMORY)
        with self.assertRaises(NeptuneException):
            _ = file.path
        self.assertEqual(b"some_content", file.content)
        self.assertEqual("png", file.extension)

    def test_create_from_string_io(self):
        file = File.from_stream(StringIO("aaabbbccc"))
        self.assertIs(file.file_type, FileType.IN_MEMORY)
        with self.assertRaises(NeptuneException):
            _ = file.path
        self.assertEqual(b"aaabbbccc", file.content)
        self.assertEqual("txt", file.extension)

        stream = StringIO("aaabbbccc")
        stream.seek(3)
        file = File.from_stream(stream)
        self.assertIs(file.file_type, FileType.IN_MEMORY)
        with self.assertRaises(NeptuneException):
            _ = file.path
        self.assertEqual(b"aaabbbccc", file.content)
        self.assertEqual("txt", file.extension)

        file = File.from_stream(StringIO("aaabbbccc"), extension="png")
        self.assertIs(file.file_type, FileType.IN_MEMORY)
        with self.assertRaises(NeptuneException):
            _ = file.path
        self.assertEqual(b"aaabbbccc", file.content)
        self.assertEqual("png", file.extension)

        file = File.from_stream(StringIO("aaabbbccc"), seek=5)
        self.assertIs(file.file_type, FileType.IN_MEMORY)
        with self.assertRaises(NeptuneException):
            _ = file.path
        self.assertEqual(b"bccc", file.content)
        self.assertEqual("txt", file.extension)

    def test_create_from_bytes_io(self):
        file = File.from_stream(BytesIO(b"aaabbbccc"))
        self.assertIs(file.file_type, FileType.IN_MEMORY)
        with self.assertRaises(NeptuneException):
            _ = file.path
        self.assertEqual(b"aaabbbccc", file.content)
        self.assertEqual("bin", file.extension)

        stream = BytesIO(b"aaabbbccc")
        stream.seek(3)
        file = File.from_stream(stream)
        self.assertIs(file.file_type, FileType.IN_MEMORY)
        with self.assertRaises(NeptuneException):
            _ = file.path
        self.assertEqual(b"aaabbbccc", file.content)
        self.assertEqual("bin", file.extension)

        file = File.from_stream(BytesIO(b"aaabbbccc"), extension="png")
        self.assertIs(file.file_type, FileType.IN_MEMORY)
        with self.assertRaises(NeptuneException):
            _ = file.path
        self.assertEqual(b"aaabbbccc", file.content)
        self.assertEqual("png", file.extension)

        file = File.from_stream(BytesIO(b"aaabbbccc"), seek=5)
        self.assertIs(file.file_type, FileType.IN_MEMORY)
        with self.assertRaises(NeptuneException):
            _ = file.path
        self.assertEqual(b"bccc", file.content)
        self.assertEqual("bin", file.extension)

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
            File(path="path", content=b"some_content")
        with self.assertRaises(ValueError):
            File()
