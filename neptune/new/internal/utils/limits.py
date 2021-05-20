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
import logging

_logger = logging.getLogger(__name__)


_IMAGE_SIZE_LIMIT_MB = 15
_IN_MEMORY_SIZE_LIMIT_MB = 32
_STREAM_SIZE_LIMIT_MB = 32

BYTES_IN_MB = 1024 * 1024

STREAM_SIZE_LIMIT_BYTES = _STREAM_SIZE_LIMIT_MB * BYTES_IN_MB


def image_size_exceeds_limit(content_size):
    if content_size > _IMAGE_SIZE_LIMIT_MB * BYTES_IN_MB:
        _logger.warning('You are attempting to create an image that is %.2fMB large. '
                        'Neptune supports logging images smaller than %dMB. '
                        'Resize or increase compression of this image',
                        content_size / BYTES_IN_MB,
                        _IMAGE_SIZE_LIMIT_MB)
        return True
    return False


def file_size_exceeds_limit(content_size):
    if content_size > _IN_MEMORY_SIZE_LIMIT_MB * BYTES_IN_MB:
        _logger.warning('You are attempting to create an in-memory file that is %.1fMB large. '
                        'Neptune supports logging in-memory file objects smaller than %dMB. '
                        'Resize or increase compression of this object',
                        content_size / BYTES_IN_MB,
                        _IN_MEMORY_SIZE_LIMIT_MB)
        return True
    return False


def stream_size_exceeds_limit(content_size):
    if content_size > _STREAM_SIZE_LIMIT_MB * BYTES_IN_MB:
        _logger.warning('Your stream is larger than %dMB. '
                        'Neptune supports saving files from streams smaller than %dMB.',
                        _STREAM_SIZE_LIMIT_MB,
                        _STREAM_SIZE_LIMIT_MB)
        return True
    return False
