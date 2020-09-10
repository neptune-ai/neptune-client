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
import base64
import io
import os
import unittest
from typing import Optional
from uuid import uuid4

import matplotlib
from matplotlib import pyplot
from matplotlib.figure import Figure

from PIL import Image
import numpy

from neptune.internal.utils.images import get_image_content

matplotlib.use('agg')


class TestImage(unittest.TestCase):

    TEST_DIR = "/tmp/neptune/{}".format(uuid4())

    def setUp(self):
        if not os.path.exists(self.TEST_DIR):
            os.makedirs(self.TEST_DIR)

    def test_get_image_content_from_string(self):
        # given
        filename = "{}/image.png".format(self.TEST_DIR)
        image_array = self._random_image_array()
        expected_image = Image.fromarray(image_array.astype(numpy.uint8))
        expected_image.save(filename)

        # expect
        self.assertEqual(get_image_content(filename), self._encode_pil_image(expected_image))

    def test_get_image_content_from_pil_image(self):
        # given
        image_array = self._random_image_array()
        expected_image = Image.fromarray(image_array.astype(numpy.uint8))

        # expect
        self.assertEqual(get_image_content(expected_image), self._encode_pil_image(expected_image))

    def test_get_image_content_from_2d_grayscale_array(self):
        # given
        image_array = self._random_image_array(d=None)
        expected_image = Image.fromarray(image_array.astype(numpy.uint8))

        # expect
        self.assertEqual(get_image_content(image_array), self._encode_pil_image(expected_image))

    def test_get_image_content_from_3d_grayscale_array(self):
        # given
        image_array = numpy.array([
            [[1], [2]],
            [[3], [4]],
            [[5], [6]]
        ])
        expected_image = Image.fromarray(numpy.array([
            [1, 2],
            [3, 4],
            [5, 6]
        ]).astype(numpy.uint8))

        # expect
        self.assertEqual(get_image_content(image_array), self._encode_pil_image(expected_image))

    def test_get_image_content_from_rgb_array(self):
        # given
        image_array = self._random_image_array()
        expected_image = Image.fromarray(image_array.astype(numpy.uint8))

        # expect
        self.assertEqual(get_image_content(image_array), self._encode_pil_image(expected_image))

    def test_get_image_content_from_rgba_array(self):
        # given
        image_array = self._random_image_array(d=4)
        expected_image = Image.fromarray(image_array.astype(numpy.uint8))

        # expect
        self.assertEqual(get_image_content(image_array), self._encode_pil_image(expected_image))

    def test_get_image_content_from_figure(self):
        # given
        pyplot.plot([1, 2, 3, 4])
        pyplot.ylabel('some interesting numbers')
        figure = pyplot.gcf()

        # expect
        self.assertEqual(get_image_content(figure), self._encode_figure(figure))

    @staticmethod
    def _encode_pil_image(image: Image) -> str:
        with io.BytesIO() as image_buffer:
            image.save(image_buffer, format='PNG')
            return base64.b64encode(image_buffer.getvalue()).decode('utf-8')

    @staticmethod
    def _encode_figure(figure: Figure) -> str:
        with io.BytesIO() as image_buffer:
            figure.savefig(image_buffer, format='PNG')
            return base64.b64encode(image_buffer.getvalue()).decode('utf-8')

    @staticmethod
    def _random_image_array(w=20, h=30, d: Optional[int] = 3):
        if d:
            return numpy.random.rand(w, h, d) * 255
        else:
            return numpy.random.rand(w, h) * 255
