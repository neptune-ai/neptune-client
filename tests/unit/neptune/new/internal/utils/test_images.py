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
import contextlib
import io
import os
import sys
import unittest
from functools import partial
from typing import Optional
from uuid import uuid4

import altair as alt
import matplotlib
import numpy
import pandas
import plotly.express as px
import seaborn as sns
from bokeh.plotting import figure
from matplotlib import pyplot
from matplotlib.figure import Figure
from PIL import Image
from vega_datasets import data

from neptune.common.utils import (
    IS_MACOS,
    IS_WINDOWS,
)
from neptune.internal.utils.images import (
    _scale_array,
    get_html_content,
    get_image_content,
)
from tests.unit.neptune.new.utils.logging import format_log

matplotlib.use("agg")


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
        image_array = numpy.array([[[1], [0]], [[-3], [4]], [[5], [6]]])
        expected_array = numpy.array([[1, 0], [-3, 4], [5, 6]])
        expected_image = Image.fromarray(expected_array.astype(numpy.uint8))

        # when

        # expect
        self.assertEqual(get_image_content(image_array), self._encode_pil_image(expected_image))

    def test_get_image_content_from_rgb_array(self):
        # given
        image_array = self._random_image_array()
        scaled_array = image_array * 255
        expected_image = Image.fromarray(scaled_array.astype(numpy.uint8))

        # expect
        self.assertEqual(get_image_content(image_array), self._encode_pil_image(expected_image))

        # and make sure that original image's size was preserved
        self.assertFalse((image_array * 255 - scaled_array).any())

    def test_get_image_content_from_rgba_array(self):
        # given
        image_array = self._random_image_array(d=4)
        scaled_array = image_array * 255
        expected_image = Image.fromarray(scaled_array.astype(numpy.uint8))

        # expect
        self.assertEqual(get_image_content(image_array), self._encode_pil_image(expected_image))

        # and make sure that original image's size was preserved
        self.assertFalse((image_array * 255 - scaled_array).any())

    def test_get_image_content_from_figure(self):
        # given
        pyplot.plot([1, 2, 3, 4])
        pyplot.ylabel("some interesting numbers")
        fig = pyplot.gcf()

        # expect
        self.assertEqual(get_image_content(fig), self._encode_figure(fig))

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
        self.assertEqual(get_image_content(image_tensor), self._encode_pil_image(expected_image))

        # and make sure that original image's size was preserved
        self.assertFalse((image_tensor.numpy() * 255 - expected_array).any())

    def test_get_image_content_from_tensorflow_tensor(self):
        import tensorflow as tf

        # given
        image_tensor = tf.random.uniform(shape=[200, 300, 3])
        expected_array = image_tensor.numpy() * 255
        expected_image = Image.fromarray(expected_array.astype(numpy.uint8))

        # expect
        self.assertEqual(get_image_content(image_tensor), self._encode_pil_image(expected_image))

    def test_get_image_content_from_seaborn_figure(self):
        # given
        grid = sns.relplot(numpy.random.randn(6, 4))

        # then
        self.assertEqual(get_image_content(grid), self._encode_figure(grid))

    def test_get_html_from_matplotlib_figure(self):
        # given
        fig = pyplot.figure()
        x = [
            21,
            22,
            23,
            4,
            5,
            6,
            77,
            8,
            9,
            10,
            31,
            32,
            33,
            34,
            35,
            36,
            37,
            18,
            49,
            50,
            100,
        ]
        pyplot.hist(x, bins=5)

        # when
        result = get_html_content(fig)

        # then
        self.assertTrue(result.startswith('<html>\n<head><meta charset="utf-8" />'))

    def test_get_html_from_plotly(self):
        # given
        df = px.data.tips()
        fig = px.histogram(
            df,
            x="total_bill",
            y="tip",
            color="sex",
            marginal="rug",
            hover_data=df.columns,
        )

        # when
        result = get_html_content(fig)

        # then
        self.assertTrue(result.startswith('<html>\n<head><meta charset="utf-8" />'))

    def test_get_html_from_altair(self):
        # given
        source = data.cars()

        chart = (
            alt.Chart(source)
            .mark_circle(size=60)
            .encode(
                x="Horsepower",
                y="Miles_per_Gallon",
                color="Origin",
                tooltip=["Name", "Origin", "Horsepower", "Miles_per_Gallon"],
            )
            .interactive()
        )

        # when
        result = get_html_content(chart)

        # then
        self.assertTrue(result.startswith("<!DOCTYPE html>\n<html>\n<head>\n  <style>"))

    def test_get_html_from_bokeh(self):
        # given
        p = figure(width=400, height=400)
        p.circle(size=20, color="navy", alpha=0.5)

        # when
        result = get_html_content(p)

        # then
        self.assertTrue(result.lstrip().startswith('<!DOCTYPE html>\n<html lang="en">'))

    def test_get_html_from_pandas(self):
        # given
        table = pandas.DataFrame(
            numpy.random.randn(6, 4),
            index=pandas.date_range("20130101", periods=6),
            columns=list("ABCD"),
        )

        # when
        result = get_html_content(table)

        # then
        self.assertTrue(
            result.startswith('<table border="1" class="dataframe">\n  <thead>\n    <tr style="text-align: right;">')
        )

    def test_get_html_from_seaborn(self):
        # given
        grid = sns.relplot(numpy.random.randn(6, 4))

        # when
        result = get_html_content(grid)

        # then
        self.assertTrue(result.startswith('<html>\n<head><meta charset="utf-8" /></head>'))

    @staticmethod
    def _encode_pil_image(image: Image) -> bytes:
        with io.BytesIO() as image_buffer:
            image.save(image_buffer, format="PNG")
            return image_buffer.getvalue()

    @staticmethod
    def _encode_figure(fig: Figure) -> bytes:
        with io.BytesIO() as image_buffer:
            fig.savefig(image_buffer, format="PNG", bbox_inches="tight")
            return image_buffer.getvalue()

    @staticmethod
    def _random_image_array(w=20, h=30, d: Optional[int] = 3):
        if d:
            return numpy.random.rand(w, h, d)
        else:
            return numpy.random.rand(w, h)


def test_scale_array_when_array_already_scaled():
    # given
    arr = numpy.array([[123, 32], [255, 0]])

    # when
    result = _scale_array(arr)

    # then
    assert numpy.all(arr == result)


def test_scale_array_when_array_not_scaled():
    # given
    arr = numpy.array([[0.3, 0], [0.5, 1]])

    # when
    result = _scale_array(arr)
    expected = numpy.array([[76.5, 0.0], [127.5, 255.0]])

    # then
    assert numpy.all(expected == result)


def test_scale_array_incorrect_range():
    # given
    arr = numpy.array([[-12, 7], [300, 0]])

    # when
    _log = partial(format_log, "WARNING")

    stdout = io.StringIO()
    with contextlib.redirect_stdout(stdout):
        result = _scale_array(arr)

    # then
    assert numpy.all(arr == result)  # returned original array

    assert stdout.getvalue() == _log(
        "Image data is in range [-12, 300]. To be interpreted as colors "
        "correctly values in the array need to be in the [0, 255] or [0.0, 1.0] range.\n",
    )
