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
__all__ = [
    "with_check_if_file_appears",
    "tmp_context",
    "a_project_name",
    "a_key",
    "Environment",
    "initialize_container",
    "reinitialize_container",
]

import io
import os
import random
import string
import tempfile
from contextlib import contextmanager
from datetime import datetime

import numpy
from attr import dataclass
from PIL import Image
from PIL.PngImagePlugin import PngImageFile

import neptune.new as neptune
from e2e_tests.exceptions import MissingEnvironmentVariable


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


def a_key():
    return "".join(random.choices(string.ascii_uppercase, k=10))


def a_project_name(project_slug: str):
    project_name = f"e2e-{datetime.now().strftime('%Y%m%d-%H%M')}-{project_slug}"

    return project_name


class RawEnvironment:
    """Load environment variables required to run e2e tests"""

    def __init__(self):
        env = os.environ
        try:
            # Target workspace name
            self.workspace_name = env["WORKSPACE_NAME"]
            # Admin user
            self.admin_username = env["ADMIN_USERNAME"]
            # Admin user API token
            self.admin_neptune_api_token = env["ADMIN_NEPTUNE_API_TOKEN"]
            # Member user
            self.user_username = env["USER_USERNAME"]
            # SA name
            self.service_account_name = env["SERVICE_ACCOUNT_NAME"]
            # Member user or SA API token
            self.neptune_api_token = env["NEPTUNE_API_TOKEN"]
        except KeyError as e:
            raise MissingEnvironmentVariable(missing_variable=e.args[0]) from e


@dataclass
class Environment:
    workspace: str
    project: str
    user_token: str  # token of `user` or `service_account`
    admin_token: str
    admin: str
    user: str
    service_account: str


def initialize_container(container_type, project, **extra_args):
    if container_type == "project":
        return neptune.init_project(name=project, **extra_args)

    if container_type == "run":
        return neptune.init_run(project=project, **extra_args)

    if container_type == "model":
        return neptune.init_model(key=a_key(), project=project, **extra_args)

    if container_type == "model_version":
        model = neptune.init_model(key=a_key(), project=project, **extra_args)
        model_sys_id = model["sys/id"].fetch()
        model.stop()

        return neptune.init_model_version(model=model_sys_id, project=project, **extra_args)

    raise NotImplementedError(container_type)


def reinitialize_container(sys_id: str, container_type: str, project: str):
    if container_type == "project":
        # exactly same as initialize_container(project), for convenience
        return neptune.init_project(name=project)

    if container_type == "run":
        return neptune.init_run(with_id=sys_id, project=project)

    if container_type == "model":
        return neptune.init_model(with_id=sys_id, project=project)

    if container_type == "model_version":
        return neptune.init_model_version(with_id=sys_id, project=project)

    raise NotImplementedError()
