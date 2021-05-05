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
           https://docs.neptune.ai/api-reference/field-types#from_content
        """
        if isinstance(content, str):
            ext = "txt"
            content = content.encode("utf-8")
        else:
            ext = "bin"
        return File(content=content, extension=extension or ext)

    @staticmethod
    def from_stream(stream: IOBase, seek: Optional[int] = 0, extension: Optional[str] = None) -> 'File':
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
           https://docs.neptune.ai/api-reference/field-types#from_stream
        .. _IOBase documentation:
            https://docs.python.org/3/library/io.html#io.IOBase
        """
        verify_type("stream", stream, IOBase)
        content, stream_default_ext = get_stream_content(stream, seek)
        return File(content=content, extension=extension or stream_default_ext)

    @staticmethod
    def as_image(image) -> 'File':
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
            >>> run = neptune.init()

            Convert NumPy array to File value object and upload it

            >>> run["train/prediction_example"].upload(File.as_image(numpy_array))

            Convert PIL image to File value object and upload it

            >>> pil_file = File.as_image(pil_image)
            >>> run["dataset/data_sample/img1"].upload(pil_file)

            You can upload PIL image without explicit conversion

            >>> run["dataset/data_sample/img2"].upload(pil_image)

        You may also want to check `as_image docs page`_.

        .. _as_image docs page:
           https://docs.neptune.ai/api-reference/field-types#as_image
        """
        content_bytes = get_image_content(image)
        return File.from_content(content_bytes if content_bytes is not None else b"", extension="png")

    @staticmethod
    def as_html(chart) -> 'File':
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
            >>> run = neptune.init()

            Convert Pandas DataFrame to File value object and upload it

            >>> run["train/results"].upload(File.as_html(df_predictions))

            Convert Altair interactive chart to File value object and upload it

            >>> altair_file = File.as_html(altair_chart)
            >>> run["dataset/data_sample/img1"].upload(altair_file)

            You can upload Altair interactive chart without explicit conversion

            >>> run["dataset/data_sample/img2"].upload(altair_chart)

        You may also want to check `as_html docs page`_.

        .. _as_html docs page:
           https://docs.neptune.ai/api-reference/field-types#as_html
        """
        content = get_html_content(chart)
        return File.from_content(content if content is not None else "", extension="html")

    @staticmethod
    def as_pickle(obj) -> 'File':
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
            >>> run = neptune.init()

            Pickle model object and upload it

            >>> run["results/pickled_model"].upload(File.as_pickle(trained_model))

        You may also want to check `as_pickle docs page`_.

        .. _as_pickle docs page:
           https://docs.neptune.ai/api-reference/field-types#as_pickle
        """
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
