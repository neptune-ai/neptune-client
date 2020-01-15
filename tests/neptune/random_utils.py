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
import datetime
import random
import string
import uuid

import mock

from neptune.projects import Project


def a_string():
    char_set = string.ascii_letters
    return ''.join(random.sample(char_set * 10, 10))


def a_uuid_string():
    return str(uuid.uuid4())


def a_string_list(length=2):
    return [a_string() for _ in range(0, length)]


def a_timestamp():
    return datetime.datetime.now()


def sort_df_by_columns(df):
    df = df.reindex(sorted(df.columns), axis=1)
    return df


def an_experiment_id():
    prefix = ''.join(random.choice(string.ascii_uppercase) for _ in range(3))
    number = random.randint(0, 100)
    return "{}-{}".format(prefix, number)


def a_project_qualified_name():
    return "{}/{}".format(a_string(), a_string())


def a_project():
    return Project(
        backend=mock.MagicMock(),
        internal_id=a_uuid_string(),
        name=a_string(),
        namespace=a_string()
    )
