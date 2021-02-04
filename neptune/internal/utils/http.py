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
import logging
from functools import wraps
from http.client import NOT_FOUND, UNPROCESSABLE_ENTITY  # pylint:disable=no-name-in-module

from requests.exceptions import HTTPError

from neptune.api_exceptions import ExperimentNotFound, StorageLimitReached
from neptune.exceptions import NeptuneException

_logger = logging.getLogger(__name__)


def extract_response_field(response, field_name):
    if response is None:
        return None

    try:
        response_json = response.json()
        if isinstance(response_json, dict):
            return response_json.get(field_name)
        else:
            _logger.debug('HTTP response is not a dict: %s', str(response_json))
            return None
    except ValueError as e:
        _logger.debug('Failed to parse HTTP response: %s', e)
        return None


def handle_quota_limits(f):
    """Wrapper for functions which may request for non existing experiment or cause quota limit breach

    Limitations:
    Decorated function must be called with experiment argument like this fun(..., experiment=<experiment>, ...)"""

    @wraps(f)
    def handler(*args, **kwargs):
        experiment = kwargs.get('experiment')
        if experiment is None:
            raise NeptuneException('This function must be called with experiment passed by name,'
                                   ' like this fun(..., experiment=<experiment>, ...)')
        try:
            return f(*args, **kwargs)
        except HTTPError as e:
            if e.response.status_code == NOT_FOUND:
                # pylint: disable=protected-access
                raise ExperimentNotFound(
                    experiment_short_id=experiment.id, project_qualified_name=experiment._project.full_id)
            if (e.response.status_code == UNPROCESSABLE_ENTITY and
                    extract_response_field(e.response, 'title').startswith('Storage limit reached in organization: ')):
                raise StorageLimitReached()
            raise

    return handler
