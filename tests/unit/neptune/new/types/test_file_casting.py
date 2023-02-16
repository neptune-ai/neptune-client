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
from datetime import datetime

import numpy
from bokeh.plotting import figure
from PIL import Image

from neptune.types import (
    Datetime,
    File,
    Float,
    FloatSeries,
    Integer,
    String,
)
from neptune.types.namespace import Namespace
from neptune.types.type_casting import cast_value
from tests.unit.neptune.new.attributes.test_attribute_base import TestAttributeBase


class TestTypeCasting(TestAttributeBase):
    def test_cast_neptune_values(self):
        data = [
            Float(1),
            String("txt"),
            Namespace({"a": 1}),
            FloatSeries([1, 2, 3], min=5),
        ]
        for value in data:
            with self.subTest(msg=value):
                self.assertEqual(value, cast_value(value))

    def test_cats_simple_values(self):
        now = datetime.now().replace(microsecond=0)

        data = [
            (1, Integer(1)),
            (0.44, Float(0.44)),
            ("1", String("1")),
            (".44", String(".44")),
            ("txt", String("txt")),
            ({"a": 1}, Namespace({"a": 1})),
            (now, Datetime(now)),
        ]
        for simple_value, expected_value in data:
            with self.subTest(msg=simple_value):
                self.assertEqual(expected_value, cast_value(simple_value))

    def test_cats_file_values(self):
        image_array = numpy.random.rand(10, 10) * 255
        image = Image.fromarray(image_array.astype(numpy.uint8))
        bokeh_figure = figure(width=400, height=400)
        bokeh_figure.circle(size=20, color="navy", alpha=0.5)

        data = [image, bokeh_figure]
        for value in data:
            with self.subTest(msg=value):
                self.assertIsInstance(cast_value(value), File)
