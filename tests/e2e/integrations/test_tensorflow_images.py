#
# Copyright (c) 2024, Neptune Labs Sp. z o.o.
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

import numpy
import pytest
import tensorflow as tf
from PIL import Image

from neptune.internal.utils.images import get_image_content


def _encode_pil_image(image: Image) -> bytes:
    with io.BytesIO() as image_buffer:
        image.save(image_buffer, format="PNG")
        return image_buffer.getvalue()


@pytest.mark.integrations
@pytest.mark.tensorflow
def test_get_image_content_from_tensorflow_tensor():
    # given
    image_tensor = tf.random.uniform(shape=[200, 300, 3])
    expected_array = image_tensor.numpy() * 255
    expected_image = Image.fromarray(expected_array.astype(numpy.uint8))

    # expect
    assert get_image_content(image_tensor) == _encode_pil_image(expected_image)
