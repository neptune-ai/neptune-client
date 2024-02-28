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
from __future__ import annotations

__all__ = [
    "get_image_content",
    "get_html_content",
    "get_pickle_content",
    "is_numpy_array",
    "is_pil_image",
    "is_matplotlib_figure",
    "is_plotly_figure",
    "is_altair_chart",
    "is_bokeh_figure",
    "is_pandas_dataframe",
    "is_seaborn_figure",
]

import base64
import io
import pickle
import warnings
from io import (
    BytesIO,
    StringIO,
)
from typing import Optional

import numpy as np
from packaging import version
from pandas import DataFrame

from neptune.exceptions import PlotlyIncompatibilityException
from neptune.internal.utils.logger import get_logger

logger = get_logger()
SEABORN_GRID_CLASSES = {"FacetGrid", "PairGrid", "JointGrid"}
ALLOWED_IMG_PIXEL_RANGES = ("[0, 255]", "[0.0, 1.0]")

try:
    from numpy import array as numpy_array
    from numpy import ndarray as numpy_ndarray
    from numpy import uint8 as numpy_uint8
except ImportError:
    numpy_ndarray = None
    numpy_array = None
    numpy_uint8 = None

try:
    from PIL.Image import Image as PILImage
    from PIL.Image import fromarray as pilimage_fromarray
except ImportError:
    PILImage = None

    def pilimage_fromarray():
        pass


def get_image_content(image, autoscale=True) -> Optional[bytes]:
    content = _image_to_bytes(image, autoscale)

    return content


def get_html_content(chart) -> Optional[str]:
    content = _to_html(chart)

    return content


def get_pickle_content(obj) -> Optional[bytes]:
    content = _export_pickle(obj)

    return content


def _image_to_bytes(image, autoscale) -> bytes:
    if image is None:
        raise ValueError("image is None")

    elif is_numpy_array(image):
        return _get_numpy_as_image(image, autoscale)

    elif is_pil_image(image):
        return _get_pil_image_data(image)

    elif is_matplotlib_figure(image):
        return _get_figure_image_data(image)

    elif _is_torch_tensor(image):
        return _get_numpy_as_image(image.detach().numpy(), autoscale)

    elif _is_tensorflow_tensor(image):
        return _get_numpy_as_image(image.numpy(), autoscale)

    elif is_seaborn_figure(image):
        return _get_figure_image_data(image.figure)

    raise TypeError("image is {}".format(type(image)))


def _to_html(chart) -> str:
    if _is_matplotlib_pyplot(chart):
        chart = chart.gcf()

    if is_matplotlib_figure(chart):
        try:
            chart = _matplotlib_to_plotly(chart)
            return _export_plotly_figure(chart)
        except ImportError:
            logger.warning("Plotly not installed. Logging plot as an image.")
            return _image_content_to_html(_get_figure_image_data(chart))
        except UserWarning:
            logger.warning(
                "Couldn't convert Matplotlib plot to interactive Plotly plot. Logging plot as an image instead."
            )
            return _image_content_to_html(_get_figure_image_data(chart))

    elif is_pandas_dataframe(chart):
        return _export_pandas_dataframe_to_html(chart)

    elif is_plotly_figure(chart):
        return _export_plotly_figure(chart)

    elif is_altair_chart(chart):
        return _export_altair_chart(chart)

    elif is_bokeh_figure(chart):
        return _export_bokeh_figure(chart)

    elif is_seaborn_figure(chart):
        return _export_seaborn_figure(chart)

    else:
        raise ValueError("Currently supported are matplotlib, plotly, altair, bokeh and seaborn figures")


def _matplotlib_to_plotly(chart):
    import matplotlib
    import plotly

    # When Plotly cannot accurately convert a matplotlib plot, it emits a warning.
    # Then we want to fallback on logging the plot as an image.
    #
    # E.g. when trying to convert a Seaborn confusion matrix or a hist2d, it emits a UserWarning with message
    # "Dang! That path collection is out of this world. I totally don't know what to do with it yet!
    # Plotly can only import path collections linked to 'data' coordinates"
    #
    # Plotly is not compatible with the latest matplotlib (3.5.0+)
    # due to fact that mpl_to_plotly uses deprecated matplotlib functionalities
    plotly_version = plotly.__version__
    matplotlib_version = matplotlib.__version__
    if version.parse(matplotlib_version) >= version.parse("3.3.0") and version.parse(plotly_version) < version.parse(
        "5.0.0"
    ):
        raise PlotlyIncompatibilityException(
            matplotlib_version,
            plotly_version,
            "Downgrade matplotlib to version 3.2, upgrade plotly to 5.0+, or upload the chart as a static "
            "image: run['chart'].upload(File.as_image(plotly_chart)). "
            "For details, see https://github.com/plotly/plotly.py/issues/1568.",
        )

    with warnings.catch_warnings():
        warnings.filterwarnings(
            "error",
            category=UserWarning,
            message=".*Plotly can only import path collections linked to 'data' coordinates.*",
        )
        try:
            chart = plotly.tools.mpl_to_plotly(chart)
        except AttributeError as e:
            if "'PathCollection' object has no attribute 'get_offset_position'" in str(e):
                raise PlotlyIncompatibilityException(
                    matplotlib_version,
                    plotly_version,
                    "Due to plotly using some deprecated matplotlib methods, we recommend downgrading matplotlib"
                    " to version 3.4. See https://github.com/plotly/plotly.py/issues/3624 for details.",
                ) from e
            raise e

    return chart


def _image_content_to_html(content: bytes) -> str:
    str_equivalent_image = base64.b64encode(content).decode()
    return "<img src='data:image/png;base64," + str_equivalent_image + "'/>"


def _get_numpy_as_image(array: np.ndarray, autoscale: bool) -> bytes:
    array = array.copy()  # prevent original array from modifying
    if autoscale:
        array = _scale_array(array)

    if len(array.shape) == 2:
        return _get_pil_image_data(pilimage_fromarray(array.astype(numpy_uint8)))
    if len(array.shape) == 3:
        if array.shape[2] == 1:
            array2d = numpy_array([[col[0] for col in row] for row in array])
            return _get_pil_image_data(pilimage_fromarray(array2d.astype(numpy_uint8)))
        if array.shape[2] in (3, 4):
            return _get_pil_image_data(pilimage_fromarray(array.astype(numpy_uint8)))
    raise ValueError(
        "Incorrect size of numpy.ndarray. Should be 2-dimensional or"
        "3-dimensional with 3rd dimension of size 1, 3 or 4."
    )


def _scale_array(array: np.ndarray) -> np.ndarray:
    array_min = array.min()
    array_max = array.max()

    if array_min >= 0 and 1 < array_max <= 255:
        return array

    if array_min >= 0 and array_max <= 1:
        return array * 255

    _warn_about_incorrect_image_data_range(array_min, array_max)
    return array


def _warn_about_incorrect_image_data_range(array_min: int | float, array_max: int | float) -> None:
    msg = f"Image data is in range [{array_min}, {array_max}]."
    logger.warning(
        "%s To be interpreted as colors correctly values in the array need to be in the %s or %s range.",
        msg,
        *ALLOWED_IMG_PIXEL_RANGES,
    )


def _get_pil_image_data(image: PILImage) -> bytes:
    with io.BytesIO() as image_buffer:
        image.save(image_buffer, format="PNG")
        return image_buffer.getvalue()


def _get_figure_image_data(figure) -> bytes:
    with io.BytesIO() as image_buffer:
        figure.savefig(image_buffer, format="png", bbox_inches="tight")
        return image_buffer.getvalue()


def _is_torch_tensor(image):
    return (
        image.__class__.__module__.startswith("torch")
        and image.__class__.__name__ == "Tensor"
        and hasattr(image, "numpy")
    )


def _is_tensorflow_tensor(image):
    return (
        image.__class__.__module__.startswith("tensorflow.")
        and "Tensor" in image.__class__.__name__
        and hasattr(image, "numpy")
    )


def _is_matplotlib_pyplot(chart):
    return chart.__class__.__module__.startswith("matplotlib.pyplot")


def is_numpy_array(image) -> bool:
    return numpy_ndarray is not None and isinstance(image, numpy_ndarray)


def is_pil_image(image) -> bool:
    return PILImage is not None and isinstance(image, PILImage)


def is_matplotlib_figure(image):
    return image.__class__.__module__.startswith("matplotlib.") and image.__class__.__name__ == "Figure"


def is_plotly_figure(chart):
    return chart.__class__.__module__.startswith("plotly.") and chart.__class__.__name__ == "Figure"


def is_altair_chart(chart):
    return chart.__class__.__module__.startswith("altair.") and "Chart" in chart.__class__.__name__


def is_bokeh_figure(chart):
    return chart.__class__.__module__.startswith("bokeh.") and chart.__class__.__name__.lower() == "figure"


def is_seaborn_figure(chart):
    return (
        chart.__class__.__module__.startswith("seaborn.axisgrid") and chart.__class__.__name__ in SEABORN_GRID_CLASSES
    )


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
    chart.save(buffer, format="html")
    buffer.seek(0)
    return buffer.getvalue()


def _export_bokeh_figure(chart):
    from bokeh.embed import file_html
    from bokeh.resources import CDN

    html = file_html(chart, CDN)
    buffer = StringIO(html)
    buffer.seek(0)
    return buffer.getvalue()


def _export_pickle(obj):
    buffer = BytesIO()
    pickle.dump(obj, buffer)
    buffer.seek(0)
    return buffer.getvalue()


def _export_seaborn_figure(chart):
    return _export_plotly_figure(_matplotlib_to_plotly(chart.figure))
