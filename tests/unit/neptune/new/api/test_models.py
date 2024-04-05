#
# Copyright (c) 2024, Neptune Labs Sp. z o.o.
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
from mock import Mock

from neptune.api.models import (
    BoolField,
    FloatField,
    IntField,
    StringField,
)


def test__float_field__from_dict():
    # given
    data = {"attributeType": "float", "attributeName": "some/float", "value": 18.5}

    # when
    result = FloatField.from_dict(data)

    # then
    assert result.path == "some/float"
    assert result.value == 18.5


def test__float_field__from_model():
    # given
    model = Mock(attributeType="float", attributeName="some/float", value=18.5)

    # when
    result = FloatField.from_model(model)

    # then
    assert result.path == "some/float"
    assert result.value == 18.5


def test__int_field__from_dict():
    # given
    data = {"attributeType": "int", "attributeName": "some/int", "value": 18}

    # when
    result = IntField.from_dict(data)

    # then
    assert result.path == "some/int"
    assert result.value == 18


def test__int_field__from_model():
    # given
    model = Mock(attributeType="int", attributeName="some/int", value=18)

    # when
    result = IntField.from_model(model)

    # then
    assert result.path == "some/int"
    assert result.value == 18


def test__string_field__from_dict():
    # given
    data = {"attributeType": "string", "attributeName": "some/string", "value": "hello"}

    # when
    result = StringField.from_dict(data)

    # then
    assert result.path == "some/string"
    assert result.value == "hello"


def test__string_field__from_model():
    # given
    model = Mock(attributeType="string", attributeName="some/string", value="hello")

    # when
    result = StringField.from_model(model)

    # then
    assert result.path == "some/string"
    assert result.value == "hello"


def test__string_field__from_dict__empty():
    # given
    data = {"attributeType": "string", "attributeName": "some/string", "value": ""}

    # when
    result = StringField.from_dict(data)

    # then
    assert result.path == "some/string"
    assert result.value == ""


def test__string_field__from_model__empty():
    # given
    model = Mock(attributeType="string", attributeName="some/string", value="")

    # when
    result = StringField.from_model(model)

    # then
    assert result.path == "some/string"
    assert result.value == ""


def test__bool_field__from_dict():
    # given
    data = {"attributeType": "bool", "attributeName": "some/bool", "value": True}

    # when
    result = BoolField.from_dict(data)

    # then
    assert result.path == "some/bool"
    assert result.value is True


def test__bool_field__from_model():
    # given
    model = Mock(attributeType="bool", attributeName="some/bool", value=True)

    # when
    result = BoolField.from_model(model)

    # then
    assert result.path == "some/bool"
    assert result.value is True
