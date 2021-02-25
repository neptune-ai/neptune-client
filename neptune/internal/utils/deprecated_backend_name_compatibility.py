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

from neptune.exceptions import NeptuneException

_logger = logging.getLogger(__name__)


def get_api_client_value(api_client, backend):
    if api_client is not None and backend is not None:
        raise NeptuneException(f"`api_client` and `backend` can't be both passed as arguments")
    if backend is not None:
        _logger.warning(f'`backend` attribute is deprecated and will be removed in future.'
                        f' Use `api_client` instead.')
        return backend
    return api_client


class DeprecatedBackendMixin:
    recommended_class = None

    def __init__(self, *args, **kwargs):
        assert self.recommended_class is not None
        _logger.warning(f"`{type(self).__name__}` is deprecated."
                        f" Use `{self.recommended_class.__name__}` instead.")

        super().__init__(*args, **kwargs)
