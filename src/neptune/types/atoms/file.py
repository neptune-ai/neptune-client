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
__all__ = [
    "File",
]

from io import IOBase
from typing import (
    TYPE_CHECKING,
    Optional,
    TypeVar,
    Union,
)

from neptune.internal.types.file_types import (
    FileComposite,
    InMemoryComposite,
    LocalFileComposite,
    StreamComposite,
)
from neptune.internal.utils import verify_type
from neptune.internal.utils.images import (
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
    is_seaborn_figure,
)
from neptune.types.atoms.atom import Atom

if TYPE_CHECKING:
    from neptune.types.value_visitor import ValueVisitor

Ret = TypeVar("Ret")


class File(Atom):
    def __init__(self, path: Optional[str] = None, file_composite: Optional[FileComposite] = None):
        """We have to support `path` parameter since almost all of `File` usages by our users look like `File(path)`."""
        verify_type("path", path, (str, type(None)))
        verify_type("file_composite", file_composite, (FileComposite, type(None)))

        if path is not None and file_composite is not None:
            raise ValueError("path and file_composite are mutually exclusive")
        if path is None and file_composite is None:
            raise ValueError("path or file_composite is required")
        if path is not None:
            self._file_composite = LocalFileComposite(path)
        else:
            self._file_composite = file_composite

    @property
    def extension(self):
        return self._file_composite.extension

    @property
    def file_type(self):
        return self._file_composite.file_type

    @property
    def path(self):
        return self._file_composite.path

    @property
    def content(self):
        return self._file_composite.content

    def _save(self, path):
        self._file_composite.save(path)

    def __str__(self):
        return str(self._file_composite)

    def accept(self, visitor: "ValueVisitor[Ret]") -> Ret:
        return visitor.visit_file(self)

    @staticmethod
    def from_path(path: str, *, extension: Optional[str] = None) -> "File":
        """Creates a File value object from a given path.

        Equivalent to `File(path)`, but you can specify the extension separately.

        Args:
            path: Path of the file to be stored in the File value object.
            extension (optional): Extension of the file, if not included in the path argument.

        Returns:
            `File` value object created based on the path.

        For more, see the documentation: https://docs.neptune.ai/api/field_types#from_path
        """
        verify_type("path", path, str)
        verify_type("extension", extension, (str, type(None)))

        file_composite = LocalFileComposite(path, extension)
        return File(file_composite=file_composite)

    @staticmethod
    def from_content(content: Union[str, bytes], *, extension: Optional[str] = None) -> "File":
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
        verify_type("content", content, (bytes, str, type(None)))
        verify_type("extension", extension, (str, type(None)))

        file_composite = InMemoryComposite(content, extension)
        return File(file_composite=file_composite)

    @staticmethod
    def from_stream(stream: IOBase, *, seek: Optional[int] = 0, extension: Optional[str] = None) -> "File":
        """Factory method for creating File value objects directly from binary and text streams.

        Note that you can only log content from the same stream once.
        In the case of text streams, UTF-8 encoding will be used.

        Args:
            stream (IOBase): Stream to be converted.
            seek (optional): Change the stream position to the given byte offset. For details,
                see the IOBase documentation.
            extension (optional): Extension of the created file that will be used for interpreting the type
                of content for visualization.
                If None (default), it will be 'bin' for binary streams and 'txt' for text streams.

        Returns:
            `File` value object created from the stream.

        See also:
        - from_stream() documentation: https://docs.neptune.ai/api/field_types#from_stream
        - IOBase documentation: https://docs.python.org/3/library/io.html#io.IOBase
        """
        verify_type("stream", stream, (IOBase, type(None)))
        verify_type("seek", seek, (int, type(None)))
        verify_type("extension", extension, (str, type(None)))

        file_composite = StreamComposite(stream, seek, extension)
        return File(file_composite=file_composite)

    @staticmethod
    def as_image(image, autoscale: bool = True) -> "File":
        """Static method for converting image objects or image-like objects to an image File value object.

        This way you can upload Matplotlib figures, Seaborn figures, PIL images, and NumPy arrays as static images.

        Args:
            image: Image-like object to be converted.
                The input image pixel must be either in range [0.0, 1.0] (float) or [0, 255] (integer).
                Supported are PyTorch tensors, TensorFlow/Keras tensors, NumPy arrays, PIL images,
                Matplotlib figures and Seaborn figures.
            autoscale: Whether Neptune should try to detect the pixel range automatically
                and scale it to an acceptable format.

        Returns:
            `File`: value object with converted image

        Examples:
            >>> import neptune
            >>> from neptune.types import File
            >>> run = neptune.init_run()

            Convert NumPy array to File value object and upload it

            >>> run["train/prediction_example"].upload(File.as_image(numpy_array))

            Convert PIL image to File value object and upload it

            >>> pil_file = File.as_image(pil_image)
            >>> run["dataset/data_sample/img1"].upload(pil_file)

            You can upload PIL images without explicit conversion

            >>> run["dataset/data_sample/img2"].upload(pil_image)

        See also the docs:
            - How to log images: https://docs.neptune.ai/logging/images/
            - API referene: https://docs.neptune.ai/api/field_types#as_image
        """
        content_bytes = get_image_content(image, autoscale=autoscale)
        return File.from_content(content_bytes if content_bytes is not None else b"", extension="png")

    @staticmethod
    def as_html(chart) -> "File":
        """Converts an object to an HTML File value object.

        This way you can upload `Altair`, `Bokeh`, `Plotly`, `Matplotlib`, `Seaborn` interactive charts
        or upload directly `Pandas` `DataFrame` objects to explore them in Neptune UI.

        Args:
            chart: An object to be converted.
                Supported are `Altair`, `Bokeh`, `Plotly`, `Matplotlib`, `Seaborn` interactive charts,
                and `Pandas` `DataFrame` objects.

        Returns:
            ``File``: value object with converted object.

        Examples:
            >>> import neptune
            >>> from neptune.types import File
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
            >>> import neptune
            >>> from neptune.types import File
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
            or is_seaborn_figure(value)
            or isinstance(value, File)
        )

    @staticmethod
    def is_convertable_to_image(value):
        convertable_to_img_predicates = (is_pil_image, is_matplotlib_figure, is_seaborn_figure)
        return any(predicate(value) for predicate in convertable_to_img_predicates)

    @staticmethod
    def is_convertable_to_html(value):
        convertable_to_html_predicates = (is_altair_chart, is_bokeh_figure, is_plotly_figure, is_seaborn_figure)
        return any(predicate(value) for predicate in convertable_to_html_predicates)
