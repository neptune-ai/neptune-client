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
from bokeh.models import LogColorMapper
from bokeh.palettes import Viridis6 as palette
from bokeh.plotting import figure
from bokeh.sampledata.unemployment import data as unemployment
from bokeh.sampledata.us_counties import data as counties
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
            x="Horsepower:Q", y="Miles_per_Gallon:Q", color=alt.condition(brush, "Origin:N", alt.value("lightgray"))
        )
        .add_selection(brush)
    )

    bars = (
        alt.Chart(source).mark_bar().encode(y="Origin:N", color="Origin:N", x="count(Origin):Q").transform_filter(brush)
    )

    chart = points & bars

    return chart


def generate_brokeh_figure():
    palette2 = tuple(reversed(palette))

    cnts = {code: county for code, county in counties.items() if county["state"] == "tx"}

    county_xs = [county["lons"] for county in cnts.values()]
    county_ys = [county["lats"] for county in cnts.values()]

    county_names = [county["name"] for county in cnts.values()]
    county_rates = [unemployment[county_id] for county_id in cnts]
    color_mapper = LogColorMapper(palette=palette2)

    chart_data = dict(
        x=county_xs,
        y=county_ys,
        name=county_names,
        rate=county_rates,
    )

    TOOLS = "pan,wheel_zoom,reset,hover,save"

    p = figure(
        title="Texas Unemployment, 2009",
        tools=TOOLS,
        x_axis_location=None,
        y_axis_location=None,
        tooltips=[("Name", "@name"), ("Unemployment rate", "@rate%"), ("(Long, Lat)", "($x, $y)")],
    )
    p.grid.grid_line_color = None
    p.hover.point_policy = "follow_mouse"

    p.patches(
        "x",
        "y",
        source=chart_data,
        fill_color={"field": "rate", "transform": color_mapper},
        fill_alpha=0.7,
        line_color="white",
        line_width=0.5,
    )

    return p


def generate_plotly_figure():
    df = px.data.tips()
    plotly_fig = px.histogram(df, x="total_bill", y="tip", color="sex", marginal="rug", hover_data=df.columns)

    return plotly_fig
