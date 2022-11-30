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
import os
import sys
import unittest
from uuid import uuid4

import matplotlib
import numpy
from matplotlib import pyplot
from PIL import Image

from neptune.common.utils import (
    IS_MACOS,
    IS_WINDOWS,
)
from neptune.legacy.internal.utils.image import (
    _get_figure_as_image,
    _get_pil_image_data,
    get_image_content,
)

matplotlib.use("agg")


class TestImage(unittest.TestCase):
    TEST_DIR = "/tmp/neptune/{}".format(uuid4())

    def setUp(self):
        if not os.path.exists(self.TEST_DIR):
            os.makedirs(self.TEST_DIR)

    def test_get_image_content_from_string(self):
        # given
        filename = "{}/image.png".format(self.TEST_DIR)
        image_array = numpy.random.rand(200, 300, 3)
        scaled_array = image_array * 255
        expected_image = Image.fromarray(scaled_array.astype(numpy.uint8))
        expected_image.save(filename)

        # expect
        self.assertEqual(get_image_content(filename), _get_pil_image_data(expected_image))

    def test_get_image_content_from_pil_image(self):
        # given
        image_array = numpy.random.rand(200, 300, 3)
        scaled_array = image_array * 255
        expected_image = Image.fromarray(scaled_array.astype(numpy.uint8))

        # expect
        self.assertEqual(get_image_content(expected_image), _get_pil_image_data(expected_image))

    def test_get_image_content_from_2d_grayscale_array(self):
        # given
        image_array = numpy.random.rand(200, 300)
        scaled_array = image_array * 255
        expected_image = Image.fromarray(scaled_array.astype(numpy.uint8))

        # expect
        self.assertEqual(get_image_content(image_array), _get_pil_image_data(expected_image))

    def test_get_image_content_from_3d_grayscale_array(self):
        # given
        image_array = numpy.array([[[1], [2]], [[3], [4]], [[5], [6]]])
        expected_array = numpy.array([[1, 2], [3, 4], [5, 6]]) * 255
        expected_image = Image.fromarray(expected_array.astype(numpy.uint8))

        # expect
        self.assertEqual(get_image_content(image_array), _get_pil_image_data(expected_image))

    def test_get_image_content_from_rgb_array(self):
        # given
        image_array = numpy.random.rand(200, 300, 3)
        scaled_array = image_array * 255
        expected_image = Image.fromarray(scaled_array.astype(numpy.uint8))

        # expect
        self.assertEqual(get_image_content(image_array), _get_pil_image_data(expected_image))

    def test_get_image_content_from_rgba_array(self):
        # given
        image_array = numpy.random.rand(200, 300, 4)
        scaled_array = image_array * 255
        expected_image = Image.fromarray(scaled_array.astype(numpy.uint8))

        # expect
        self.assertEqual(get_image_content(image_array), _get_pil_image_data(expected_image))

    def test_get_image_content_from_figure(self):
        # given
        pyplot.plot([1, 2, 3, 4])
        pyplot.ylabel("some interesting numbers")
        figure = pyplot.gcf()
        figure.canvas.draw()

        # expect
        self.assertEqual(get_image_content(figure), _get_figure_as_image(figure))

    @unittest.skipIf(IS_WINDOWS, "Installing Torch on Windows takes too long")
    @unittest.skipIf(
        IS_MACOS and sys.version_info.major == 3 and sys.version_info.minor == 10,
        "No torch for 3.10 on Mac",
    )
    def test_get_image_content_from_torch_tensor(self):
        import torch

        # given
        image_tensor = torch.rand(200, 300, 3)
        expected_array = image_tensor.numpy() * 255
        expected_image = Image.fromarray(expected_array.astype(numpy.uint8))

        # expect
        self.assertEqual(get_image_content(image_tensor), _get_pil_image_data(expected_image))

    def test_get_image_content_from_tensorflow_tensor(self):
        import tensorflow as tf

        # given
        image_tensor = tf.random.uniform(shape=[200, 300, 3])
        expected_array = image_tensor.numpy() * 255
        expected_image = Image.fromarray(expected_array.astype(numpy.uint8))

        # expect
        self.assertEqual(get_image_content(image_tensor), _get_pil_image_data(expected_image))
