#
# Copyright (c) 2019, Neptune Labs Sp. z o.o.
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
import io
import os

import numpy
import six
from PIL import Image

from neptune.legacy.exceptions import (
    FileNotFound,
    InvalidChannelValue,
)


def get_image_content(image):
    if isinstance(image, six.string_types):
        if not os.path.exists(image):
            raise FileNotFound(image)
        with open(image, "rb") as image_file:
            return image_file.read()

    elif isinstance(image, numpy.ndarray):
        return _get_numpy_as_image(image)

    elif isinstance(image, Image.Image):
        return _get_pil_image_data(image)

    else:
        try:
            from matplotlib import figure

            if isinstance(image, figure.Figure):
                return _get_figure_as_image(image)
        except ImportError:
            pass

        try:
            from torch import Tensor as TorchTensor

            if isinstance(image, TorchTensor):
                return _get_numpy_as_image(image.detach().numpy())
        except ImportError:
            pass

        try:
            from tensorflow import Tensor as TensorflowTensor

            if isinstance(image, TensorflowTensor):
                return _get_numpy_as_image(image.numpy())
        except ImportError:
            pass

    raise InvalidChannelValue(expected_type="image", actual_type=type(image).__name__)


def _get_figure_as_image(figure):
    with io.BytesIO() as image_buffer:
        figure.savefig(image_buffer, format="png", bbox_inches="tight")
        return image_buffer.getvalue()


def _get_pil_image_data(image):
    with io.BytesIO() as image_buffer:
        image.save(image_buffer, format="PNG")
        return image_buffer.getvalue()


def _get_numpy_as_image(array):
    array = array.copy()  # prevent original array from modifying

    array *= 255
    shape = array.shape
    if len(shape) == 2:
        return _get_pil_image_data(Image.fromarray(array.astype(numpy.uint8)))
    if len(shape) == 3:
        if shape[2] == 1:
            array2d = numpy.array([[col[0] for col in row] for row in array])
            return _get_pil_image_data(Image.fromarray(array2d.astype(numpy.uint8)))
        if shape[2] in (3, 4):
            return _get_pil_image_data(Image.fromarray(array.astype(numpy.uint8)))
    raise ValueError(
        "Incorrect size of numpy.ndarray. Should be 2-dimensional or"
        " 3-dimensional with 3rd dimension of size 1, 3 or 4."
    )
