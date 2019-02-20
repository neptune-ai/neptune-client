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


class ConnectionLost(Exception):
    def __init__(self):
        super(ConnectionLost, self).__init__('Connection lost. Please try again.')


class ServerError(Exception):
    def __init__(self):
        super(ServerError, self).__init__('Server error. Please try again later.')


class Unauthorized(Exception):
    def __init__(self):
        super(Unauthorized, self).__init__('You have no permissions to access this resource.')


class InvalidApiKey(Exception):
    def __init__(self):
        super(InvalidApiKey, self).__init__('The provided API key is invalid.')
