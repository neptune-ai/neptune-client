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
# pylint: disable=unused-argument  # TODO: remove this after actually implementing
import typing


def get_project_list(api_token=None) -> typing.List[str]:
    ...


def create_project(name, key, workspace=None, visibility=None, description=None, api_token=None) -> str:
    ...


def delete_project(name, workspace=None, api_token=None):
    ...


def add_project_member(name, username, role, workspace=None, api_token=None):
    ...


def get_project_member_list(name, workspace=None, api_token=None) -> typing.Dict[str, str]:
    ...


def remove_project_member(name, username, workspace=None, api_token=None):
    ...


def get_workspace_member_list(name, api_token=None) -> typing.Dict[str, str]:
    ...
