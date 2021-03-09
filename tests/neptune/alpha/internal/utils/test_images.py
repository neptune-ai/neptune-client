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

# pylint: disable=protected-access
import io
import os
import sys
import unittest
from typing import Optional
from uuid import uuid4

import matplotlib
from matplotlib import pyplot
from matplotlib.figure import Figure

from PIL import Image
import numpy

from neptune.alpha.internal.utils.images import get_image_content

matplotlib.use('agg')


class TestImage(unittest.TestCase):

    TEST_DIR = "/tmp/neptune/{}".format(uuid4())

    def setUp(self):
        if not os.path.exists(self.TEST_DIR):
            os.makedirs(self.TEST_DIR)

    def test_get_image_content_from_pil_image(self):
        # given
        image_array = self._random_image_array()
        expected_image = Image.fromarray(image_array.astype(numpy.uint8))

        # expect
        self.assertEqual(get_image_content(expected_image), self._encode_pil_image(expected_image))

    def test_get_image_content_from_2d_grayscale_array(self):
        # given
        image_array = self._random_image_array(d=None)
        scaled_array = image_array * 255
        expected_image = Image.fromarray(scaled_array.astype(numpy.uint8))

        # expect
        self.assertEqual(get_image_content(image_array), self._encode_pil_image(expected_image))

    def test_get_image_content_from_3d_grayscale_array(self):
        # given
        image_array = numpy.array([
            [[1], [2]],
            [[3], [4]],
            [[5], [6]]
        ])
        expected_array = numpy.array([
            [1, 2],
            [3, 4],
            [5, 6]
        ]) * 255
        expected_image = Image.fromarray(expected_array.astype(numpy.uint8))

        # expect
        self.assertEqual(get_image_content(image_array), self._encode_pil_image(expected_image))

    def test_get_image_content_from_rgb_array(self):
        # given
        image_array = self._random_image_array()
        scaled_array = image_array * 255
        expected_image = Image.fromarray(scaled_array.astype(numpy.uint8))

        # expect
        self.assertEqual(get_image_content(image_array), self._encode_pil_image(expected_image))

    def test_get_image_content_from_rgba_array(self):
        # given
        image_array = self._random_image_array(d=4)
        scaled_array = image_array * 255
        expected_image = Image.fromarray(scaled_array.astype(numpy.uint8))

        # expect
        self.assertEqual(get_image_content(image_array), self._encode_pil_image(expected_image))

    def test_get_image_content_from_figure(self):
        # given
        pyplot.plot([1, 2, 3, 4])
        pyplot.ylabel('some interesting numbers')
        figure = pyplot.gcf()

        # expect
        self.assertEqual(get_image_content(figure), self._encode_figure(figure))

    @unittest.skipIf(sys.version_info < (3, 6),
                     reason="Installing Torch on Windows takes too long and 3.5 is not supported")
    def test_get_image_content_from_torch_tensor(self):
        import torch  # pylint: disable=C0415
        # given
        image_tensor = torch.rand(200, 300, 3)  # pylint: disable=no-member
        expected_array = image_tensor.numpy() * 255
        expected_image = Image.fromarray(expected_array.astype(numpy.uint8))

        # expect
        self.assertEqual(get_image_content(image_tensor), self._encode_pil_image(expected_image))

    @unittest.skipIf(sys.version_info < (3, 6), reason="Tensorflow isn't built for older Pythons")
    def test_get_image_content_from_tensorflow_tensor(self):
        import tensorflow as tf  # pylint: disable=C0415
        # given
        image_tensor = tf.random.uniform(shape=[200, 300, 3])
        expected_array = image_tensor.numpy() * 255
        expected_image = Image.fromarray(expected_array.astype(numpy.uint8))

        # expect
        self.assertEqual(get_image_content(image_tensor), self._encode_pil_image(expected_image))

    @staticmethod
    def _encode_pil_image(image: Image) -> bytes:
        with io.BytesIO() as image_buffer:
            image.save(image_buffer, format='PNG')
            return image_buffer.getvalue()

    @staticmethod
    def _encode_figure(figure: Figure) -> bytes:
        with io.BytesIO() as image_buffer:
            figure.savefig(image_buffer, format='PNG', bbox_inches="tight")
            return image_buffer.getvalue()

    @staticmethod
    def _random_image_array(w=20, h=30, d: Optional[int] = 3):
        if d:
            return numpy.random.rand(w, h, d) * 255
        else:
            return numpy.random.rand(w, h) * 255
