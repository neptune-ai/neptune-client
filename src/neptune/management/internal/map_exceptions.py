#
# Copyright (c) 2023, Neptune Labs Sp. z o.o.
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
__all__ = ["map_exceptions"]

from functools import wraps

from neptune.api.exceptions import ActiveProjectsLimitReached as APIActiveProjectsLimitReached
from neptune.api.exceptions import IncorrectIdentifier as APIIncorrectIdentifier
from neptune.api.exceptions import ObjectNotFound as APIObjectNotFound
from neptune.api.exceptions import ProjectKeyCollision as APIProjectKeyCollision
from neptune.api.exceptions import ProjectKeyInvalid as APIProjectKeyInvalid
from neptune.api.exceptions import ProjectNameCollision as APIProjectNameCollision
from neptune.api.exceptions import ProjectNameInvalid as APIProjectNameInvalid
from neptune.api.exceptions import ProjectPrivacyRestricted as APIProjectPrivacyRestricted
from neptune.api.exceptions import ProjectsLimitReached as APIProjectsLimitReached
from neptune.management.exceptions import (
    ActiveProjectsLimitReachedException,
    IncorrectIdentifierException,
    ObjectNotFound,
    ProjectKeyCollision,
    ProjectKeyInvalid,
    ProjectNameCollision,
    ProjectNameInvalid,
    ProjectPrivacyRestrictedException,
    ProjectsLimitReached,
)


def map_exceptions(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)

        except APIProjectsLimitReached as e:
            raise ProjectsLimitReached() from e

        except APIProjectKeyCollision as e:
            raise ProjectKeyCollision(key=e.key) from e

        except APIProjectNameCollision as e:
            raise ProjectNameCollision(key=e.key) from e

        except APIProjectKeyInvalid as e:
            raise ProjectKeyInvalid(key=e.key, reason=e.reason) from e

        except APIProjectNameInvalid as e:
            raise ProjectNameInvalid(name=e.name) from e

        except APIIncorrectIdentifier as e:
            raise IncorrectIdentifierException(identifier=e.identifier) from e

        except APIObjectNotFound as e:
            raise ObjectNotFound() from e

        except APIProjectPrivacyRestricted as e:
            raise ProjectPrivacyRestrictedException(requested=e.requested, allowed=e.allowed) from e

        except APIActiveProjectsLimitReached as e:
            raise ActiveProjectsLimitReachedException(current_quota=e.current_quota) from e

    return wrapper
