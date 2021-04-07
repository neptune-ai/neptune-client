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
import os
from io import IOBase
from typing import TypeVar, TYPE_CHECKING, Optional, Union

from neptune.new.internal.utils.images import get_image_content, get_html_content, get_pickle_content, is_pil_image, \
    is_matplotlib_figure, is_plotly_figure, is_altair_chart, is_bokeh_figure, is_numpy_array, is_pandas_dataframe


from neptune.new.internal.utils import verify_type, get_stream_content
from neptune.new.types.atoms.atom import Atom

if TYPE_CHECKING:
    from neptune.new.types.value_visitor import ValueVisitor

Ret = TypeVar('Ret')


class File(Atom):

    def __init__(self,
                 path: Optional[str] = None,
                 content: Optional[bytes] = None,
                 extension: Optional[str] = None):
        verify_type("path", path, (str, type(None)))
        verify_type("content", content, (bytes, type(None)))
        verify_type("extension", extension, (str, type(None)))

        if path is not None and content is not None:
            raise ValueError("path and content are mutually exclusive")
        if path is None and content is None:
            raise ValueError("path or content is required")

        self.path = path
        self.content = content

        if extension is None and path is not None:
            try:
                ext = os.path.splitext(path)[1]
                self.extension = ext[1:] if ext else ""
            except ValueError:
                self.extension = ""
        else:
            self.extension = extension or ""

    def accept(self, visitor: 'ValueVisitor[Ret]') -> Ret:
        return visitor.visit_file(self)

    def __str__(self):
        if self.path is not None:
            return "File(path={})".format(str(self.path))
        else:
            return "File(content=...)"

    @staticmethod
    def from_content(content: Union[str, bytes], extension: Optional[str] = None) -> 'File':
        if isinstance(content, str):
            ext = "txt"
            content = content.encode("utf-8")
        else:
            ext = "bin"
        return File(content=content, extension=extension or ext)

    @staticmethod
    def from_stream(stream: IOBase, seek: Optional[int] = 0, extension: Optional[str] = None) -> 'File':
        verify_type("stream", stream, IOBase)
        content, stream_default_ext = get_stream_content(stream, seek)
        return File(content=content, extension=extension or stream_default_ext)

    @staticmethod
    def as_image(image) -> 'File':
        content_bytes = get_image_content(image)
        return File.from_content(content_bytes if content_bytes is not None else b"", extension="png")

    @staticmethod
    def as_html(chart) -> 'File':
        content = get_html_content(chart)
        return File.from_content(content if content is not None else "", extension="html")

    @staticmethod
    def as_pickle(obj) -> 'File':
        content = get_pickle_content(obj)
        return File.from_content(content if content is not None else b"", extension="pkl")

    @staticmethod
    def create_from(value) -> 'File':
        if isinstance(value, str):
            return File(path=value)
        elif is_pil_image(value) or is_matplotlib_figure(value):
            return File.as_image(value)
        elif is_plotly_figure(value) or is_altair_chart(value) or is_bokeh_figure(value):
            return File.as_html(value)
        elif is_numpy_array(value):
            raise TypeError("Value of type {} is not supported. Please use File.as_image().".format(type(value)))
        elif is_pandas_dataframe(value):
            raise TypeError("Value of type {} is not supported. Please use File.as_html().".format(type(value)))
        elif isinstance(value, File):
            return value
        raise TypeError("Value of type {} is not supported.".format(type(value)))

    @staticmethod
    def is_convertable(value):
        return is_pil_image(value) \
               or is_matplotlib_figure(value) \
               or is_plotly_figure(value) \
               or is_altair_chart(value) \
               or is_bokeh_figure(value) \
               or is_numpy_array(value) \
               or is_pandas_dataframe(value) \
               or isinstance(value, File)
