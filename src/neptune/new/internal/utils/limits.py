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
import logging
import warnings

_logger = logging.getLogger(__name__)


_CUSTOM_RUN_ID_LENGTH = 36
_IMAGE_SIZE_LIMIT_MB = 15
_IN_MEMORY_SIZE_LIMIT_MB = 32
_STREAM_SIZE_LIMIT_MB = 32
_LOGGED_IMAGE_SIZE_LIMIT_MB = 15

BYTES_IN_MB = 1024 * 1024

STREAM_SIZE_LIMIT_BYTES = _STREAM_SIZE_LIMIT_MB * BYTES_IN_MB


def custom_run_id_exceeds_length(custom_run_id):
    if custom_run_id and len(custom_run_id) > _CUSTOM_RUN_ID_LENGTH:
        _logger.warning(
            "Given custom_run_id exceeds %s" " characters and it will be ignored.",
            _CUSTOM_RUN_ID_LENGTH,
        )
        return True
    return False


def image_size_exceeds_limit(content_size):
    if content_size > _IMAGE_SIZE_LIMIT_MB * BYTES_IN_MB:
        _logger.warning(
            "You are attempting to create an image that is %.2fMB large. "
            "Neptune supports logging images smaller than %dMB. "
            "Resize or increase compression of this image",
            content_size / BYTES_IN_MB,
            _IMAGE_SIZE_LIMIT_MB,
        )
        return True
    return False


def image_size_exceeds_limit_for_logging(content_size):
    if content_size > _LOGGED_IMAGE_SIZE_LIMIT_MB * BYTES_IN_MB:
        warnings.warn(
            f"You are attempting to log an image that is {content_size / BYTES_IN_MB:.2f}MB large. "
            f"Neptune supports logging images smaller than {_LOGGED_IMAGE_SIZE_LIMIT_MB}MB. "
            "Resize or increase compression of this image.",
            category=UserWarning,
        )
        return True
    return False


def file_size_exceeds_limit(content_size):
    if content_size > _IN_MEMORY_SIZE_LIMIT_MB * BYTES_IN_MB:
        _logger.warning(
            "You are attempting to create an in-memory file that is %.1fMB large. "
            "Neptune supports logging in-memory file objects smaller than %dMB. "
            "Resize or increase compression of this object",
            content_size / BYTES_IN_MB,
            _IN_MEMORY_SIZE_LIMIT_MB,
        )
        return True
    return False


def stream_size_exceeds_limit(content_size):
    if content_size > _STREAM_SIZE_LIMIT_MB * BYTES_IN_MB:
        _logger.warning(
            "Your stream is larger than %dMB. " "Neptune supports saving files from streams smaller than %dMB.",
            _STREAM_SIZE_LIMIT_MB,
            _STREAM_SIZE_LIMIT_MB,
        )
        return True
    return False
