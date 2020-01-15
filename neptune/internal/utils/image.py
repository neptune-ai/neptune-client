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

from PIL import Image
import numpy
import six

from neptune.exceptions import FileNotFound, InvalidChannelValue


def get_image_content(image):
    if isinstance(image, six.string_types):
        if not os.path.exists(image):
            raise FileNotFound(image)
        with open(image, 'rb') as image_file:
            return image_file.read()

    elif isinstance(image, numpy.ndarray):
        shape = image.shape
        if len(shape) == 2:
            return _get_pil_image_data(Image.fromarray(image.astype(numpy.uint8)))
        if len(shape) == 3:
            if shape[2] == 1:
                array2d = numpy.array([[col[0] for col in row] for row in image])
                return _get_pil_image_data(Image.fromarray(array2d.astype(numpy.uint8)))
            if shape[2] in (3, 4):
                return _get_pil_image_data(Image.fromarray(image.astype(numpy.uint8)))
        raise ValueError("Incorrect size of numpy.ndarray. Should be 2-dimensional or"
                         "3-dimensional with 3rd dimension of size 1, 3 or 4.")

    elif isinstance(image, Image.Image):
        return _get_pil_image_data(image)

    else:
        try:
            from matplotlib import figure
            if isinstance(image, figure.Figure):
                return _get_pil_image_data(_figure_to_pil_image(image))
        except ImportError:
            pass

    raise InvalidChannelValue(expected_type='image', actual_type=type(image).__name__)


def _get_pil_image_data(image):
    with io.BytesIO() as image_buffer:
        image.save(image_buffer, format='PNG')
        return image_buffer.getvalue()


def _figure_to_pil_image(figure):
    figure.canvas.draw()
    return Image.frombytes('RGB', figure.canvas.get_width_height(), figure.canvas.tostring_rgb())
