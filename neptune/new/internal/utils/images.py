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
import base64
import io
import logging
import pickle
import warnings
from io import StringIO, BytesIO
from typing import Optional

import click
from packaging import version
from pandas import DataFrame

from neptune.new.exceptions import PlotlyIncompatibilityException
from neptune.new.internal.utils import limits

_logger = logging.getLogger(__name__)

try:
    from numpy import ndarray as numpy_ndarray, array as numpy_array, uint8 as numpy_uint8
except ImportError:
    numpy_ndarray = None
    numpy_array = None
    numpy_uint8 = None

try:
    from PIL.Image import Image as PILImage, fromarray as pilimage_fromarray
except ImportError:
    PILImage = None

    def pilimage_fromarray():
        pass


def get_image_content(image) -> Optional[bytes]:
    content = _image_to_bytes(image)

    if limits.image_size_exceeds_limit(len(content)):
        return None

    return content


def get_html_content(chart) -> Optional[str]:
    content = _to_html(chart)

    if limits.file_size_exceeds_limit(len(content)):
        return None

    return content


def get_pickle_content(obj) -> Optional[bytes]:
    content = _export_pickle(obj)

    if limits.file_size_exceeds_limit(len(content)):
        return None

    return content


def _image_to_bytes(image) -> bytes:
    if image is None:
        raise ValueError("image is None")

    elif is_numpy_array(image):
        return _get_numpy_as_image(image)

    elif is_pil_image(image):
        return _get_pil_image_data(image)

    elif is_matplotlib_figure(image):
        return _get_figure_image_data(image)

    elif _is_torch_tensor(image):
        return _get_numpy_as_image(image.detach().numpy())

    elif _is_tensorflow_tensor(image):
        return _get_numpy_as_image(image.numpy())

    raise TypeError("image is {}".format(type(image)))


def _to_html(chart) -> str:
    if _is_matplotlib_pyplot(chart):
        chart = chart.gcf()

    if is_matplotlib_figure(chart):
        try:
            chart = _matplotlib_to_plotly(chart)
            return _export_plotly_figure(chart)
        except ImportError:
            print("Plotly not installed. Logging plot as an image.")
            return _image_content_to_html(_get_figure_image_data(chart))
        except UserWarning:
            print("Couldn't convert Matplotlib plot to interactive Plotly plot. Logging plot as an image instead.")
            return _image_content_to_html(_get_figure_image_data(chart))

    elif is_pandas_dataframe(chart):
        return _export_pandas_dataframe_to_html(chart)

    elif is_plotly_figure(chart):
        return _export_plotly_figure(chart)

    elif is_altair_chart(chart):
        return _export_altair_chart(chart)

    elif is_bokeh_figure(chart):
        return _export_bokeh_figure(chart)

    else:
        raise ValueError("Currently supported are matplotlib, plotly, altair, and bokeh figures")


def _matplotlib_to_plotly(chart):
    # pylint: disable=import-outside-toplevel
    import plotly
    import matplotlib

    # When Plotly cannot accurately convert a matplotlib plot, it emits a warning.
    # Then we want to fallback on logging the plot as an image.
    #
    # E.g. when trying to convert a Seaborn confusion matrix or a hist2d, it emits a UserWarning with message
    # "Dang! That path collection is out of this world. I totally don't know what to do with it yet!
    # Plotly can only import path collections linked to 'data' coordinates"
    plotly_version = plotly.__version__
    matplotlib_version = matplotlib.__version__
    if version.parse(matplotlib_version) >= version.parse("3.3.0"):
        raise PlotlyIncompatibilityException(matplotlib_version, plotly_version)

    with warnings.catch_warnings():
        warnings.filterwarnings(
            "error",
            category=UserWarning,
            message=".*Plotly can only import path collections linked to 'data' coordinates.*")
        chart = plotly.tools.mpl_to_plotly(chart)

    return chart


def _image_content_to_html(content: bytes) -> str:
    str_equivalent_image = base64.b64encode(content).decode()
    return "<img src='data:image/png;base64," + str_equivalent_image + "'/>"


def _get_numpy_as_image(array):
    data_range_warnings = []
    array_min = array.min()
    array_max = array.max()
    if array_min < 0:
        data_range_warnings.append(f"the smallest value in the array is {array_min}")
    if array_max > 1:
        data_range_warnings.append(f"the largest value in the array is {array_max}")
    if data_range_warnings:
        data_range_warning_message = (" and ".join(data_range_warnings) + ". ").capitalize()
        click.echo(
            f"{data_range_warning_message}"
            f"To be interpreted as colors correctly values in the array need to be in the [0, 1] range.",
            err=True
        )
    array *= 255
    shape = array.shape
    if len(shape) == 2:
        return _get_pil_image_data(pilimage_fromarray(array.astype(numpy_uint8)))
    if len(shape) == 3:
        if shape[2] == 1:
            array2d = numpy_array([[col[0] for col in row] for row in array])
            return _get_pil_image_data(pilimage_fromarray(array2d.astype(numpy_uint8)))
        if shape[2] in (3, 4):
            return _get_pil_image_data(pilimage_fromarray(array.astype(numpy_uint8)))
    raise ValueError("Incorrect size of numpy.ndarray. Should be 2-dimensional or"
                     "3-dimensional with 3rd dimension of size 1, 3 or 4.")


def _get_pil_image_data(image: PILImage) -> bytes:
    with io.BytesIO() as image_buffer:
        image.save(image_buffer, format='PNG')
        return image_buffer.getvalue()


def _get_figure_image_data(figure) -> bytes:
    with io.BytesIO() as image_buffer:
        figure.savefig(image_buffer, format='png', bbox_inches="tight")
        return image_buffer.getvalue()


def _is_torch_tensor(image):
    return image.__class__.__module__.startswith('torch')\
           and image.__class__.__name__ == 'Tensor'\
           and hasattr(image, "numpy")


def _is_tensorflow_tensor(image):
    return image.__class__.__module__.startswith('tensorflow.')\
           and 'Tensor' in image.__class__.__name__\
           and hasattr(image, "numpy")


def _is_matplotlib_pyplot(chart):
    return chart.__class__.__module__.startswith('matplotlib.pyplot')


def is_numpy_array(image) -> bool:
    return numpy_ndarray is not None and isinstance(image, numpy_ndarray)


def is_pil_image(image) -> bool:
    return PILImage is not None and isinstance(image, PILImage)


def is_matplotlib_figure(image):
    return image.__class__.__module__.startswith('matplotlib.') and image.__class__.__name__ == 'Figure'


def is_plotly_figure(chart):
    return chart.__class__.__module__.startswith('plotly.') and chart.__class__.__name__ == 'Figure'


def is_altair_chart(chart):
    return chart.__class__.__module__.startswith('altair.') and 'Chart' in chart.__class__.__name__


def is_bokeh_figure(chart):
    return chart.__class__.__module__.startswith('bokeh.') and chart.__class__.__name__ == 'Figure'


def is_pandas_dataframe(table):
    return isinstance(table, DataFrame)


def _export_pandas_dataframe_to_html(table):
    buffer = StringIO(table.to_html())
    buffer.seek(0)
    return buffer.getvalue()


def _export_plotly_figure(image):
    buffer = StringIO()
    image.write_html(buffer)
    buffer.seek(0)
    return buffer.getvalue()


def _export_altair_chart(chart):
    buffer = StringIO()
    chart.save(buffer, format='html')
    buffer.seek(0)
    return buffer.getvalue()


def _export_bokeh_figure(chart):
    from bokeh.resources import CDN
    from bokeh.embed import file_html

    html = file_html(chart, CDN)
    buffer = StringIO(html)
    buffer.seek(0)
    return buffer.getvalue()


def _export_pickle(obj):
    buffer = BytesIO()
    pickle.dump(obj, buffer)
    buffer.seek(0)
    return buffer.getvalue()
