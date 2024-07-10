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
import altair as alt
import matplotlib.pyplot as plt
import numpy as np
import plotly.express as px
import seaborn as sns
from bokeh.plotting import figure
from PIL import Image
from vega_datasets import data


def generate_pil_image():
    data = np.random.randint(low=0, high=256, size=32 * 32 * 3, dtype=np.uint8)
    data = data.reshape(32, 32, 3)
    pil_image = Image.fromarray(data, "RGB")
    return pil_image


def generate_matplotlib_figure():
    rect = 0.1, 0.1, 0.8, 0.8
    fig = plt.figure()
    fig.add_axes(rect, label="label1")
    fig.add_axes(rect, label="label2")
    fig.add_axes(rect, frameon=False, facecolor="g")
    fig.add_axes(rect, polar=True)
    return fig


def generate_altair_chart():
    source = data.cars()

    brush = alt.selection(type="interval")

    points = (
        alt.Chart(source)
        .mark_point()
        .encode(
            x="Horsepower:Q",
            y="Miles_per_Gallon:Q",
            color=alt.condition(brush, "Origin:N", alt.value("lightgray")),
        )
        .add_selection(brush)
    )

    # TODO: return chart once problem with altair and JSONSchema is solved
    # https://github.com/altair-viz/altair/issues/2705
    # bars = (
    #   alt.Chart(source).mark_bar().encode(y="Origin:N", color="Origin:N", x="count(Origin):Q").transform_filter(brush)
    # )
    # chart = points & bars
    # return chart
    return points


def generate_brokeh_figure():
    N = 500
    x = np.linspace(0, 10, N)
    y = np.linspace(0, 10, N)
    xx, yy = np.meshgrid(x, y)
    d = np.sin(xx) * np.cos(yy)

    bokeh_figure = figure(tooltips=[("x", "$x"), ("y", "$y"), ("value", "@image")])
    bokeh_figure.x_range.range_padding = bokeh_figure.y_range.range_padding = 0

    # must give a vector of image data for image parameter
    bokeh_figure.image(image=[d], x=0, y=0, dw=10, dh=10, palette="Spectral11", level="image")
    bokeh_figure.grid.grid_line_width = 0.5

    return bokeh_figure


def generate_plotly_figure():
    df = px.data.tips()
    plotly_fig = px.histogram(df, x="total_bill", y="tip", color="sex", marginal="rug", hover_data=df.columns)

    return plotly_fig


def generate_seaborn_figure():
    sample_size = 30
    x = np.random.rand(sample_size) * 2 * np.pi
    data = {
        "x": x,
        "y": np.sin(x),
        "c": np.random.randint(0, 2, sample_size),
        "arch": x > np.pi,
    }
    seaborn_fig = sns.relplot(data=data, x="x", y="y", hue="c", col="arch")
    return seaborn_fig
