#
# Copyright (c) 2021, Neptune Labs Sp. z o.o.
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
__all__ = ["with_check_if_file_appears", "tmp_context", "a_project_name", "Environment"]

import io
import os
import random
import tempfile
from datetime import datetime
from collections import namedtuple
from contextlib import contextmanager

import numpy
from PIL import Image
from PIL.PngImagePlugin import PngImageFile


def _remove_file_if_exists(filepath):
    try:
        os.remove(filepath)
    except OSError:
        pass


# init kwargs which significantly reduce operations noise
DISABLE_SYSLOG_KWARGS = {
    "capture_stdout": False,
    "capture_stderr": False,
    "capture_hardware_metrics": False,
}


@contextmanager
def with_check_if_file_appears(filepath):
    """Checks if file will be present when leaving the block.
    File is removed if exists when entering the block."""
    _remove_file_if_exists(filepath)

    try:
        yield
    finally:
        assert os.path.exists(filepath)
        _remove_file_if_exists(filepath)


@contextmanager
def preserve_cwd(path):
    cwd = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(cwd)


@contextmanager
def tmp_context():
    with tempfile.TemporaryDirectory() as tmp:
        with preserve_cwd(tmp):
            yield tmp


def generate_image(*, size: int) -> Image:
    random_numbers = numpy.random.rand(size, size, 3) * 255
    return Image.fromarray(random_numbers.astype("uint8")).convert("RGB")


def image_to_png(*, image: Image) -> PngImageFile:
    png_buf = io.BytesIO()
    image.save(png_buf, format="png")
    png_buf.seek(0)
    return PngImageFile(png_buf)


def a_project_name(project_slug: str):
    project_name = f"e2e-{datetime.now().strftime('%Y%m%d-%H%M')}-{project_slug}"
    project_key = "".join(
        random.choices(population=project_slug.replace("-", ""), k=10)
    ).upper()

    return project_name, project_key


Environment = namedtuple(
    "Environment",
    ["workspace", "project", "user_token", "admin_token", "admin", "user"],
)
