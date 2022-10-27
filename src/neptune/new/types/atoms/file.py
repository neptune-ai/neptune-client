#
# Copyright (c) 2022, Neptune Labs Sp. z o.o.
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
import enum
import io
import os
from io import IOBase
from typing import (
    TYPE_CHECKING,
    Optional,
    TypeVar,
    Union,
)

from neptune.new.exceptions import NeptuneException
from neptune.new.internal.utils import (
    limits,
    verify_type,
)
from neptune.new.internal.utils.images import (
    get_html_content,
    get_image_content,
    get_pickle_content,
    is_altair_chart,
    is_bokeh_figure,
    is_matplotlib_figure,
    is_numpy_array,
    is_pandas_dataframe,
    is_pil_image,
    is_plotly_figure,
)
from neptune.new.types.atoms.atom import Atom

if TYPE_CHECKING:
    from neptune.new.types.value_visitor import ValueVisitor

Ret = TypeVar("Ret")


class FileType(enum.Enum):
    LOCAL_FILE = "LOCAL_FILE"
    IN_MEMORY = "IN_MEMORY"
    STREAM = "STREAM"


class File(Atom):
    def __init__(
        self,
        path: Optional[str] = None,
        content: Optional[bytes] = None,
        stream: Optional[IOBase] = None,
        extension: Optional[str] = None,
    ):
        verify_type("path", path, (str, type(None)))
        verify_type("content", content, (bytes, type(None)))
        verify_type("stream", stream, (IOBase, type(None)))
        verify_type("extension", extension, (str, type(None)))

        exclusive_parameters_no = sum(1 if paramter is not None else 0 for paramter in (path, content, stream))
        if exclusive_parameters_no > 1:
            raise ValueError("path, content and stream are mutually exclusive")
        if exclusive_parameters_no == 0:
            raise ValueError("path, content or stream is required")
        if path is not None:
            self.file_type = FileType.LOCAL_FILE
        elif content is not None:
            self.file_type = FileType.IN_MEMORY
        else:
            self.file_type = FileType.STREAM

        self._path = path
        self._content = content
        self._stream = stream

        if self.file_type is FileType.LOCAL_FILE and extension is None:
            try:
                ext = os.path.splitext(path)[1]
                self.extension = ext[1:] if ext else ""
            except ValueError:
                self.extension = ""
        else:
            self.extension = extension or ""

    @property
    def path(self):
        if self.file_type is FileType.LOCAL_FILE:
            return self._path
        else:
            raise NeptuneException(f"`path` attribute is not supported for {self.file_type}")

    @property
    def content(self):
        if self.file_type is FileType.IN_MEMORY:
            return self._content
        elif self.file_type is FileType.STREAM:
            val = self._stream.read()
            if isinstance(self._stream, io.TextIOBase):
                val = val.encode()
            return val
        else:
            raise NeptuneException(f"`content` attribute is not supported for {self.file_type}")

    def _save(self, path):
        if self.file_type is FileType.IN_MEMORY:
            with open(path, "wb") as f:
                f.write(self._content)
        elif self.file_type is FileType.STREAM:
            with open(path, "wb") as f:
                buffer_ = self._stream.read(io.DEFAULT_BUFFER_SIZE)
                while buffer_:
                    # TODO: replace with Walrus Operator once python3.7 support is dropped
                    if isinstance(self._stream, io.TextIOBase):
                        buffer_ = buffer_.encode()
                    f.write(buffer_)
                    buffer_ = self._stream.read(io.DEFAULT_BUFFER_SIZE)
        else:
            raise NeptuneException(f"`_save` method is not supported for {self.file_type}")

    def accept(self, visitor: "ValueVisitor[Ret]") -> Ret:
        return visitor.visit_file(self)

    def __str__(self):
        if self.file_type is FileType.LOCAL_FILE:
            return f"File(path={self.path})"
        elif self.file_type is FileType.IN_MEMORY:
            return "File(content=...)"
        elif self.file_type is FileType.STREAM:
            return f"File(stream={self._stream})"
        else:
            raise ValueError(f"Unexpected FileType: {self.file_type}")

    @staticmethod
    def from_content(content: Union[str, bytes], extension: Optional[str] = None) -> "File":
        """Factory method for creating File value objects directly from binary and text content.

        In the case of text content, UTF-8 encoding will be used.

        Args:
            content	(str or bytes): Text or binary content to stored in the `File` value object.
            extension (str, optional, default is None): Extension of the created file.
                File will be used for interpreting the type of content for visualization.
                If `None` it will be bin for binary content and txt for text content.
                Defaults to `None`.

        Returns:
            ``File``: value object created from the content

        You may also want to check `from_content docs page`_.

        .. _from_content docs page:
           https://docs.neptune.ai/api/field_types#from_content
        """
        if isinstance(content, str):
            ext = "txt"
            content = content.encode("utf-8")
        else:
            ext = "bin"

        if limits.file_size_exceeds_limit(len(content)):
            content = b""

        return File(content=content, extension=extension or ext)

    @staticmethod
    def from_stream(stream: IOBase, seek: Optional[int] = 0, extension: Optional[str] = None) -> "File":
        # TODO: docs (update)
        """Factory method for creating File value objects directly from binary and text streams.

        In the case of text stream, UTF-8 encoding will be used.

        Args:
            stream (IOBase): Stream to be converted.
            seek (int, optional): See IOBase documentation.
                Defaults to `0`.
            extension (str, optional): Extension of the file created that will be used for interpreting the type
                of content for visualization.
                If `None` it will be bin for binary stream and txt for text stream.
                Defaults to `None`.

        Returns:
            ``File``: value object created from the stream.

        You may also want to check `from_stream docs page`_ and `IOBase documentation`_.

        .. _from_stream docs page:
           https://docs.neptune.ai/api/field_types#from_stream
        .. _IOBase documentation:
            https://docs.python.org/3/library/io.html#io.IOBase
        """
        verify_type("stream", stream, IOBase)
        if seek is not None and stream.seekable():
            stream.seek(seek)
        if extension is None:
            extension = "txt" if isinstance(stream, io.TextIOBase) else "bin"
        return File(stream=stream, extension=extension)

    @staticmethod
    def as_image(image) -> "File":
        """Static method for converting image objects or image-like objects to an image File value object.

        This way you can upload `Matplotlib` figures, `PIL` images, `NumPy` arrays, as static images.

        Args:
            image: Image-like object to be converted.
                Supported are `PyTorch` tensors, `TensorFlow/Keras` tensors, `NumPy` arrays, `PIL` images
                and `Matplotlib` figures.

        Returns:
            ``File``: value object with converted image

        Examples:
            >>> import neptune.new as neptune
            >>> from neptune.new.types import File
            >>> run = neptune.init_run()

            Convert NumPy array to File value object and upload it

            >>> run["train/prediction_example"].upload(File.as_image(numpy_array))

            Convert PIL image to File value object and upload it

            >>> pil_file = File.as_image(pil_image)
            >>> run["dataset/data_sample/img1"].upload(pil_file)

            You can upload PIL image without explicit conversion

            >>> run["dataset/data_sample/img2"].upload(pil_image)

        You may also want to check `as_image docs page`_.

        .. _as_image docs page:
           https://docs.neptune.ai/api/field_types#as_image
        """
        content_bytes = get_image_content(image)
        return File.from_content(content_bytes if content_bytes is not None else b"", extension="png")

    @staticmethod
    def as_html(chart) -> "File":
        """Converts an object to an HTML File value object.

        This way you can upload `Altair`, `Bokeh`, `Plotly`, `Matplotlib` interactive charts
        or upload directly `Pandas` `DataFrame` objects to explore them in Neptune UI.

        Args:
            chart: An object to be converted.
                Supported are `Altair`, `Bokeh`, `Plotly`, `Matplotlib` interactive charts,
                and `Pandas` `DataFrame` objects.

        Returns:
            ``File``: value object with converted object.

        Examples:
            >>> import neptune.new as neptune
            >>> from neptune.new.types import File
            >>> run = neptune.init_run()

            Convert Pandas DataFrame to File value object and upload it

            >>> run["train/results"].upload(File.as_html(df_predictions))

            Convert Altair interactive chart to File value object and upload it

            >>> altair_file = File.as_html(altair_chart)
            >>> run["dataset/data_sample/img1"].upload(altair_file)

            You can upload Altair interactive chart without explicit conversion

            >>> run["dataset/data_sample/img2"].upload(altair_chart)

        You may also want to check `as_html docs page`_.

        .. _as_html docs page:
           https://docs.neptune.ai/api/field_types#as_html
        """
        content = get_html_content(chart)
        return File.from_content(content if content is not None else "", extension="html")

    @staticmethod
    def as_pickle(obj) -> "File":
        """Pickles a Python object and stores it in `File` value object.

        This way you can upload any Python object for future use.

        Args:
            obj: An object to be converted.
                Supported are `Altair`, `Bokeh`, `Plotly`, `Matplotlib` interactive charts,
                and `Pandas` `DataFrame` objects.

        Returns:
            ``File``: value object with pickled object.

        Examples:
            >>> import neptune.new as neptune
            >>> from neptune.new.types import File
            >>> run = neptune.init_run()

            Pickle model object and upload it

            >>> run["results/pickled_model"].upload(File.as_pickle(trained_model))

        You may also want to check `as_pickle docs page`_.

        .. _as_pickle docs page:
           https://docs.neptune.ai/api/field_types#as_pickle
        """
        content = get_pickle_content(obj)
        return File.from_content(content if content is not None else b"", extension="pkl")

    @staticmethod
    def create_from(value) -> "File":
        if isinstance(value, str):
            return File(path=value)
        elif File.is_convertable_to_image(value):
            return File.as_image(value)
        elif File.is_convertable_to_html(value):
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
        return (
            is_pil_image(value)
            or is_matplotlib_figure(value)
            or is_plotly_figure(value)
            or is_altair_chart(value)
            or is_bokeh_figure(value)
            or is_numpy_array(value)
            or is_pandas_dataframe(value)
            or isinstance(value, File)
        )

    @staticmethod
    def is_convertable_to_image(value):
        convertable_to_img_predicates = (is_pil_image, is_matplotlib_figure)
        return any(predicate(value) for predicate in convertable_to_img_predicates)

    @staticmethod
    def is_convertable_to_html(value):
        convertable_to_html_predicates = (is_altair_chart, is_bokeh_figure, is_plotly_figure)
        return any(predicate(value) for predicate in convertable_to_html_predicates)
