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
import contextlib
import io
import os
import sys
import unittest
from typing import Optional
from unittest import mock
from uuid import uuid4

import matplotlib
import pandas
from matplotlib import pyplot
from matplotlib.figure import Figure

import plotly.express as px
import altair as alt
from vega_datasets import data
from bokeh.plotting import figure

from PIL import Image
import numpy

from neptune.new.internal.utils.images import get_image_content, get_html_content

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
            [[1], [0]],
            [[-3], [4]],
            [[5], [6]]
        ])
        expected_array = numpy.array([
            [1, 0],
            [-3, 4],
            [5, 6]
        ]) * 255
        expected_image = Image.fromarray(expected_array.astype(numpy.uint8))

        # expect
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            self.assertEqual(get_image_content(image_array), self._encode_pil_image(expected_image))
        self.assertEqual(
            stderr.getvalue(),
            "The smallest value in the array is -3 and the largest value in the array is 6."
            " To be interpreted as colors correctly values in the array need to be in the [0, 1] range.\n"
        )

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
        fig = pyplot.gcf()

        # expect
        self.assertEqual(get_image_content(fig), self._encode_figure(fig))

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

    def test_get_html_from_matplotlib_figure(self):
        # given
        fig = pyplot.figure()
        x = [21, 22, 23, 4, 5, 6, 77, 8, 9, 10, 31, 32, 33, 34, 35, 36, 37, 18, 49, 50, 100]
        pyplot.hist(x, bins=5)

        # when
        result = get_html_content(fig)

        # then
        self.assertTrue(result.startswith('<html>\n<head><meta charset="utf-8" />'))

    def test_get_html_from_plotly(self):
        # given
        df = px.data.tips()
        fig = px.histogram(df, x="total_bill", y="tip", color="sex", marginal="rug",
                           hover_data=df.columns)

        # when
        result = get_html_content(fig)

        # then
        self.assertTrue(result.startswith('<html>\n<head><meta charset="utf-8" />'))

    def test_get_html_from_altair(self):
        # given
        source = data.cars()

        chart = alt.Chart(source).mark_circle(size=60).encode(
            x='Horsepower',
            y='Miles_per_Gallon',
            color='Origin',
            tooltip=['Name', 'Origin', 'Horsepower', 'Miles_per_Gallon']
        ).interactive()

        # when
        result = get_html_content(chart)

        # then
        self.assertTrue(result.startswith('<!DOCTYPE html>\n<html>\n<head>\n  <style>'))

    def test_get_html_from_bokeh(self):
        # given
        p = figure(plot_width=400, plot_height=400)
        p.circle(size=20, color="navy", alpha=0.5)

        # when
        result = get_html_content(p)

        # then
        self.assertTrue(result.startswith('\n\n\n\n<!DOCTYPE html>\n<html lang="en">'))

    def test_get_html_from_pandas(self):
        # given
        table = pandas.DataFrame(
            numpy.random.randn(6, 4),
            index=pandas.date_range("20130101", periods=6),
            columns=list("ABCD"))

        # when
        result = get_html_content(table)

        # then
        self.assertTrue(result.startswith(
            '<table border="1" class="dataframe">\n  <thead>\n    <tr style="text-align: right;">'))

    def test_get_oversize_html_from_pandas(self):
        # given
        table = mock.Mock(spec=pandas.DataFrame)
        table.to_html.return_value = 40_000_000 * 'a'

        # when
        with self.assertLogs() as caplog:
            result = get_html_content(table)

        # then
        self.assertIsNone(result)
        self.assertEqual(
            caplog.output, [
                'WARNING:neptune.new.internal.utils.limits:You are attempting to create an in-memory file that'
                ' is 38.1MB large. Neptune supports logging in-memory file objects smaller than 32MB. '
                'Resize or increase compression of this object'
            ]
        )

    @staticmethod
    def _encode_pil_image(image: Image) -> bytes:
        with io.BytesIO() as image_buffer:
            image.save(image_buffer, format='PNG')
            return image_buffer.getvalue()

    @staticmethod
    def _encode_figure(fig: Figure) -> bytes:
        with io.BytesIO() as image_buffer:
            fig.savefig(image_buffer, format='PNG', bbox_inches="tight")
            return image_buffer.getvalue()

    @staticmethod
    def _random_image_array(w=20, h=30, d: Optional[int] = 3):
        if d:
            return numpy.random.rand(w, h, d)
        else:
            return numpy.random.rand(w, h)
