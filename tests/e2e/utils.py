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
    "modified_environ",
    "catch_time",
    "SIZE_1KB",
    "SIZE_1MB",
]

import io
import os
import random
import string
import tempfile
from contextlib import contextmanager
from datetime import datetime
from math import sqrt
from time import perf_counter

import numpy
from attr import dataclass
from PIL import Image
from PIL.PngImagePlugin import PngImageFile

import neptune
from neptune.internal.container_type import ContainerType
from tests.e2e.exceptions import MissingEnvironmentVariable


def _remove_file_if_exists(filepath):
    try:
        os.remove(filepath)
    except OSError:
        pass


SIZE_1MB = 2**20
SIZE_1KB = 2**10

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
    """generate image of size in bytes"""
    width = int(sqrt(size / 3))  # 3 bytes per one pixel in square image
    random_numbers = numpy.random.rand(width, width, 3) * 255
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
    if isinstance(container_type, ContainerType):
        container_type = container_type.value

    if container_type == "project":
        return neptune.init_project(project=project, **extra_args)

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


def reinitialize_container(sys_id: str, container_type: str, project: str, **kwargs):
    if container_type == "project":
        # exactly same as initialize_container(project), for convenience
        return neptune.init_project(project=project, **kwargs)

    if container_type == "run":
        return neptune.init_run(with_id=sys_id, project=project, **kwargs)

    if container_type == "model":
        return neptune.init_model(with_id=sys_id, project=project, **kwargs)

    if container_type == "model_version":
        return neptune.init_model_version(with_id=sys_id, project=project, **kwargs)

    raise NotImplementedError()


# from https://stackoverflow.com/a/62956469
@contextmanager
def catch_time() -> float:
    start = perf_counter()
    yield lambda: perf_counter() - start


# from https://stackoverflow.com/a/34333710
@contextmanager
def modified_environ(*remove, **update):
    """
    Temporarily updates the ``os.environ`` dictionary in-place.

    The ``os.environ`` dictionary is updated in-place so that the modification
    is sure to work in all situations.

    :param remove: Environment variables to remove.
    :param update: Dictionary of environment variables and values to add/update.
    """
    env = os.environ
    update = update or {}
    remove = remove or []

    # List of environment variables being updated or removed.
    stomped = (set(update.keys()) | set(remove)) & set(env.keys())
    # Environment variables and values to restore on exit.
    update_after = {k: env[k] for k in stomped}
    # Environment variables and values to remove on exit.
    remove_after = frozenset(k for k in update if k not in env)

    try:
        env.update(update)
        for k in remove:
            env.pop(k, None)
        yield
    finally:
        env.update(update_after)
        for k in remove_after:
            env.pop(k)
