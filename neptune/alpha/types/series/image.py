#
# Copyright (c) 2020, Neptune Labs Sp. z o.o.
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
from neptune.alpha.internal.utils import base64_encode

from neptune.alpha.internal.utils.images import get_image_content


class Image:

    def __init__(self, value=None, content: str = None):
        if content is not None:
            self.content = content
        elif value is not None:
            content_bytes = get_image_content(value)
            self.content = base64_encode(content_bytes) if content_bytes is not None else None
        else:
            raise ValueError("Parameter 'value' unfilled.")
