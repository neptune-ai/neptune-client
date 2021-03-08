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

# pylint: disable=protected-access
from io import StringIO, BytesIO

from neptune.alpha.types import File

from tests.neptune.alpha.attributes.test_attribute_base import TestAttributeBase


class TestFile(TestAttributeBase):

    def test_create_from_path(self):
        file = File("some/path.ext")
        self.assertEqual("some/path.ext", file.path)
        self.assertEqual(None, file.content)
        self.assertEqual("ext", file.extension)

        file = File("some/path.txt.ext")
        self.assertEqual("some/path.txt.ext", file.path)
        self.assertEqual(None, file.content)
        self.assertEqual("ext", file.extension)

        file = File("so.me/path")
        self.assertEqual("so.me/path", file.path)
        self.assertEqual(None, file.content)
        self.assertEqual("", file.extension)

        file = File("some/path.ext", extension="txt")
        self.assertEqual("some/path.ext", file.path)
        self.assertEqual(None, file.content)
        self.assertEqual("txt", file.extension)

    def test_create_from_content(self):
        file = File.from_content("some_content")
        self.assertEqual(None, file.path)
        self.assertEqual("some_content".encode("utf-8"), file.content)
        self.assertEqual("", file.extension)

        file = File.from_content("some_content", extension="txt")
        self.assertEqual(None, file.path)
        self.assertEqual("some_content".encode("utf-8"), file.content)
        self.assertEqual("txt", file.extension)

    def test_create_from_string_io(self):
        file = File.from_stream(StringIO("aaabbbccc"))
        self.assertEqual(None, file.path)
        self.assertEqual(b"aaabbbccc", file.content)
        self.assertEqual("txt", file.extension)

        stream = StringIO("aaabbbccc")
        stream.seek(3)
        file = File.from_stream(stream)
        self.assertEqual(None, file.path)
        self.assertEqual(b"aaabbbccc", file.content)
        self.assertEqual("txt", file.extension)

        file = File.from_stream(StringIO("aaabbbccc"), extension="png")
        self.assertEqual(None, file.path)
        self.assertEqual(b"aaabbbccc", file.content)
        self.assertEqual("png", file.extension)

        file = File.from_stream(StringIO("aaabbbccc"), seek=5)
        self.assertEqual(None, file.path)
        self.assertEqual(b"bccc", file.content)
        self.assertEqual("txt", file.extension)

    def test_create_from_bytes_io(self):
        file = File.from_stream(BytesIO(b"aaabbbccc"))
        self.assertEqual(None, file.path)
        self.assertEqual(b"aaabbbccc", file.content)
        self.assertEqual("bin", file.extension)

        stream = BytesIO(b"aaabbbccc")
        stream.seek(3)
        file = File.from_stream(stream)
        self.assertEqual(None, file.path)
        self.assertEqual(b"aaabbbccc", file.content)
        self.assertEqual("bin", file.extension)

        file = File.from_stream(BytesIO(b"aaabbbccc"), extension="png")
        self.assertEqual(None, file.path)
        self.assertEqual(b"aaabbbccc", file.content)
        self.assertEqual("png", file.extension)

        file = File.from_stream(BytesIO(b"aaabbbccc"), seek=5)
        self.assertEqual(None, file.path)
        self.assertEqual(b"bccc", file.content)
        self.assertEqual("bin", file.extension)

    def test_raise_exception_in_constructor(self):
        with self.assertRaises(ValueError):
            File(path="path", content=b"some_content")
        with self.assertRaises(ValueError):
            File()
