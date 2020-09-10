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

import io
import logging
import os
from typing import NewType, Union, Optional

from neptune.exceptions import FileNotFound
from neptune.internal.utils import base64_encode

_logger = logging.getLogger(__name__)

try:
    from numpy import ndarray as numpy_ndarray, array as numpy_array, uint8 as numpy_uint8
except ImportError:
    numpy_ndarray = NewType("numpy_ndarray", type(None))
    numpy_array = NewType("numpy_array", type(None))
    numpy_uint8 = NewType("numpy_uint8", type(None))

try:
    from PIL.Image import Image as PILImage, fromarray as pilimage_fromarray
except ImportError:
    PILImage = NewType("PILImage", type(None))

    def pilimage_fromarray():
        pass

try:
    from matplotlib.figure import Figure as MPLFigure
except ImportError:
    MPLFigure = NewType("MPLFigure", type(None))


ImageAcceptedTypes = Union[str, numpy_ndarray, PILImage, MPLFigure]

IMAGE_SIZE_LIMIT_MB = 15


def get_image_content(image: ImageAcceptedTypes) -> Optional[str]:
    content = _image_to_bytes(image)

    if len(content) > IMAGE_SIZE_LIMIT_MB * 1024 * 1024:
        _logger.warning('Your image is larger than %dMB. Neptune supports logging images smaller than %dMB. '
                        'Resize or increase compression of this image',
                        IMAGE_SIZE_LIMIT_MB,
                        IMAGE_SIZE_LIMIT_MB)
        return None

    return base64_encode(content)


def _image_to_bytes(image: ImageAcceptedTypes) -> bytes:
    if image is type(None):
        raise ValueError("image is None")

    elif isinstance(image, str):
        if not os.path.exists(image):
            raise FileNotFound(image)
        with open(image, 'rb') as image_file:
            return image_file.read()

    elif isinstance(image, numpy_ndarray):
        shape = image.shape
        if len(shape) == 2:
            return _get_pil_image_data(pilimage_fromarray(image.astype(numpy_uint8)))
        if len(shape) == 3:
            if shape[2] == 1:
                array2d = numpy_array([[col[0] for col in row] for row in image])
                return _get_pil_image_data(pilimage_fromarray(array2d.astype(numpy_uint8)))
            if shape[2] in (3, 4):
                return _get_pil_image_data(pilimage_fromarray(image.astype(numpy_uint8)))
        raise ValueError("Incorrect size of numpy.ndarray. Should be 2-dimensional or"
                         "3-dimensional with 3rd dimension of size 1, 3 or 4.")

    elif isinstance(image, PILImage):
        return _get_pil_image_data(image)

    elif isinstance(image, MPLFigure):
        return _get_figure_image_data(image)

    raise TypeError("image is {}".format(type(image)))


def _get_pil_image_data(image: PILImage) -> bytes:
    with io.BytesIO() as image_buffer:
        image.save(image_buffer, format='PNG')
        return image_buffer.getvalue()


def _get_figure_image_data(figure: MPLFigure) -> bytes:
    with io.BytesIO() as image_buffer:
        figure.savefig(image_buffer, format='png')
        return image_buffer.getvalue()
