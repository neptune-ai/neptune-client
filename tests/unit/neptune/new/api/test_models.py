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
import datetime

import pytest
from mock import Mock

from neptune.api.models import (
    ArtifactField,
    BoolField,
    DateTimeField,
    Field,
    FieldDefinition,
    FieldType,
    FileEntry,
    FileField,
    FileSetField,
    FloatField,
    FloatSeriesField,
    GitRefField,
    ImageSeriesField,
    IntField,
    LeaderboardEntriesSearchResult,
    LeaderboardEntry,
    NotebookRefField,
    ObjectStateField,
    StringField,
    StringSeriesField,
    StringSetField,
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


def test__datetime_field__from_dict():
    # given
    data = {"attributeType": "datetime", "attributeName": "some/datetime", "value": "2024-01-01T00:12:34.567890Z"}

    # when
    result = DateTimeField.from_dict(data)

    # then
    assert result.path == "some/datetime"
    assert result.value == datetime.datetime(2024, 1, 1, 0, 12, 34, 567890)


def test__datetime_field__from_model():
    # given
    model = Mock(attributeType="datetime", attributeName="some/datetime", value="2024-01-01T00:12:34.567890Z")

    # when
    result = DateTimeField.from_model(model)

    # then
    assert result.path == "some/datetime"
    assert result.value == datetime.datetime(2024, 1, 1, 0, 12, 34, 567890)


def test__float_series_field__from_dict():
    # given
    data = {
        "attributeType": "floatSeries",
        "attributeName": "some/floatSeries",
        "last": 19.5,
    }

    # when
    result = FloatSeriesField.from_dict(data)

    # then
    assert result.path == "some/floatSeries"
    assert result.last == 19.5


def test__float_series_field__from_dict__no_last():
    # given
    data = {
        "attributeType": "floatSeries",
        "attributeName": "some/floatSeries",
    }

    # when
    result = FloatSeriesField.from_dict(data)

    # then
    assert result.path == "some/floatSeries"
    assert result.last is None


def test__float_series_field__from_model():
    # given
    model = Mock(
        attributeType="floatSeries",
        attributeName="some/floatSeries",
        last=19.5,
    )

    # when
    result = FloatSeriesField.from_model(model)

    # then
    assert result.path == "some/floatSeries"
    assert result.last == 19.5


def test__float_series_field__from_model__no_last():
    # given
    model = Mock(
        attributeType="floatSeries",
        attributeName="some/floatSeries",
        last=None,
    )

    # when
    result = FloatSeriesField.from_model(model)

    # then
    assert result.path == "some/floatSeries"
    assert result.last is None


def test__string_series_field__from_dict():
    # given
    data = {
        "attributeType": "stringSeries",
        "attributeName": "some/stringSeries",
        "last": "hello",
    }

    # when
    result = StringSeriesField.from_dict(data)

    # then
    assert result.path == "some/stringSeries"
    assert result.last == "hello"


def test__string_series_field__from_dict__no_last():
    # given
    data = {
        "attributeType": "stringSeries",
        "attributeName": "some/stringSeries",
    }

    # when
    result = StringSeriesField.from_dict(data)

    # then
    assert result.path == "some/stringSeries"
    assert result.last is None


def test__string_series_field__from_model():
    # given
    model = Mock(
        attributeType="stringSeries",
        attributeName="some/stringSeries",
        last="hello",
    )

    # when
    result = StringSeriesField.from_model(model)

    # then
    assert result.path == "some/stringSeries"
    assert result.last == "hello"


def test__string_series_field__from_model__no_last():
    # given
    model = Mock(
        attributeType="stringSeries",
        attributeName="some/stringSeries",
        last=None,
    )

    # when
    result = StringSeriesField.from_model(model)

    # then
    assert result.path == "some/stringSeries"
    assert result.last is None


def test__image_series_field__from_dict():
    # given
    data = {
        "attributeType": "imageSeries",
        "attributeName": "some/imageSeries",
        "lastStep": 15.0,
    }

    # when
    result = ImageSeriesField.from_dict(data)

    # then
    assert result.path == "some/imageSeries"
    assert result.last_step == 15.0


def test__image_series_field__from_dict__no_last_step():
    # given
    data = {
        "attributeType": "imageSeries",
        "attributeName": "some/imageSeries",
    }

    # when
    result = ImageSeriesField.from_dict(data)

    # then
    assert result.path == "some/imageSeries"
    assert result.last_step is None


def test__image_series_field__from_model():
    # given
    model = Mock(
        attributeType="imageSeries",
        attributeName="some/imageSeries",
        lastStep=15.0,
    )

    # when
    result = ImageSeriesField.from_model(model)

    # then
    assert result.path == "some/imageSeries"
    assert result.last_step == 15.0


def test__image_series_field__from_model__no_last_step():
    # given
    model = Mock(
        attributeType="imageSeries",
        attributeName="some/imageSeries",
        lastStep=None,
    )

    # when
    result = ImageSeriesField.from_model(model)

    # then
    assert result.path == "some/imageSeries"
    assert result.last_step is None


def test__string_set_field__from_dict():
    # given
    data = {
        "attributeType": "stringSet",
        "attributeName": "some/stringSet",
        "values": ["hello", "world"],
    }

    # when
    result = StringSetField.from_dict(data)

    # then
    assert result.path == "some/stringSet"
    assert result.values == {"hello", "world"}


def test__string_set_field__from_dict__empty():
    # given
    data = {
        "attributeType": "stringSet",
        "attributeName": "some/stringSet",
        "values": [],
    }

    # when
    result = StringSetField.from_dict(data)

    # then
    assert result.path == "some/stringSet"
    assert result.values == set()


def test__string_set_field__from_model():
    # given
    model = Mock(
        attributeType="stringSet",
        attributeName="some/stringSet",
        values=["hello", "world"],
    )

    # when
    result = StringSetField.from_model(model)

    # then
    assert result.path == "some/stringSet"
    assert result.values == {"hello", "world"}


def test__string_set_field__from_model__empty():
    # given
    model = Mock(
        attributeType="stringSet",
        attributeName="some/stringSet",
        values=[],
    )

    # when
    result = StringSetField.from_model(model)

    # then
    assert result.path == "some/stringSet"
    assert result.values == set()


def test__file_field__from_dict():
    # given
    data = {
        "attributeType": "file",
        "attributeName": "some/file",
        "name": "file.txt",
        "size": 1024,
        "ext": "txt",
    }

    # when
    result = FileField.from_dict(data)

    # then
    assert result.path == "some/file"
    assert result.name == "file.txt"
    assert result.size == 1024
    assert result.ext == "txt"


def test__file_field__from_model():
    # given
    model = Mock(
        attributeType="file",
        attributeName="some/file",
        size=1024,
        ext="txt",
    )
    model.name = "file.txt"

    # when
    result = FileField.from_model(model)

    # then
    assert result.path == "some/file"
    assert result.name == "file.txt"
    assert result.size == 1024
    assert result.ext == "txt"


@pytest.mark.parametrize("state,expected", [("running", "Active"), ("idle", "Inactive")])
def test__object_state_field__from_dict(state, expected):
    # given
    data = {"attributeType": "experimentState", "attributeName": "sys/state", "value": state}

    # when
    result = ObjectStateField.from_dict(data)

    # then
    assert result.path == "sys/state"
    assert result.value == expected


@pytest.mark.parametrize("state,expected", [("running", "Active"), ("idle", "Inactive")])
def test__object_state_field__from_model(state, expected):
    # given
    model = Mock(attributeType="experimentState", attributeName="sys/state", value=state)

    # when
    result = ObjectStateField.from_model(model)

    # then
    assert result.path == "sys/state"
    assert result.value == expected


def test__file_set_field__from_dict():
    # given
    data = {
        "attributeType": "fileSet",
        "attributeName": "some/fileSet",
        "size": 3072,
    }

    # when
    result = FileSetField.from_dict(data)

    # then
    assert result.path == "some/fileSet"
    assert result.size == 3072


def test__file_set_field__from_model():
    # given
    model = Mock(
        attributeType="fileSet",
        attributeName="some/fileSet",
        size=3072,
    )

    # when
    result = FileSetField.from_model(model)

    # then
    assert result.path == "some/fileSet"
    assert result.size == 3072


def test__notebook_ref_field__from_dict():
    # given
    data = {
        "attributeType": "notebookRef",
        "attributeName": "some/notebookRef",
        "notebookName": "Data Processing.ipynb",
    }

    # when
    result = NotebookRefField.from_dict(data)

    # then
    assert result.path == "some/notebookRef"
    assert result.notebook_name == "Data Processing.ipynb"


def test__notebook_ref_field__from_dict__no_notebook_name():
    # given
    data = {
        "attributeType": "notebookRef",
        "attributeName": "some/notebookRef",
    }

    # when
    result = NotebookRefField.from_dict(data)

    # then
    assert result.path == "some/notebookRef"
    assert result.notebook_name is None


def test__notebook_ref_field__from_model():
    # given
    model = Mock(
        attributeType="notebookRef",
        attributeName="some/notebookRef",
        notebookName="Data Processing.ipynb",
    )

    # when
    result = NotebookRefField.from_model(model)

    # then
    assert result.path == "some/notebookRef"
    assert result.notebook_name == "Data Processing.ipynb"


def test__notebook_ref_field__from_model__no_notebook_name():
    # given
    model = Mock(
        attributeType="notebookRef",
        attributeName="some/notebookRef",
        notebookName=None,
    )

    # when
    result = NotebookRefField.from_model(model)

    # then
    assert result.path == "some/notebookRef"
    assert result.notebook_name is None


def test__git_ref_field__from_dict():
    # given
    data = {
        "attributeType": "gitRef",
        "attributeName": "some/gitRef",
        "commit": {
            "commitId": "b2d7f8a",
        },
    }

    # when
    result = GitRefField.from_dict(data)

    # then
    assert result.path == "some/gitRef"
    assert result.commit.commit_id == "b2d7f8a"


def test__git_ref_field__from_dict__no_commit():
    # given
    data = {
        "attributeType": "gitRef",
        "attributeName": "some/gitRef",
    }

    # when
    result = GitRefField.from_dict(data)

    # then
    assert result.path == "some/gitRef"
    assert result.commit is None


def test__git_ref_field__from_model():
    # given
    model = Mock(
        attributeType="gitRef",
        attributeName="some/gitRef",
        commit=Mock(
            commitId="b2d7f8a",
        ),
    )

    # when
    result = GitRefField.from_model(model)

    # then
    assert result.path == "some/gitRef"
    assert result.commit.commit_id == "b2d7f8a"


def test__git_ref_field__from_model__no_commit():
    # given
    model = Mock(
        attributeType="gitRef",
        attributeName="some/gitRef",
        commit=None,
    )

    # when
    result = GitRefField.from_model(model)

    # then
    assert result.path == "some/gitRef"
    assert result.commit is None


def test__artifact_field__from_dict():
    # given
    data = {
        "attributeType": "artifact",
        "attributeName": "some/artifact",
        "hash": "f192cddb2b98c0b4c72bba22b68d2245",
    }

    # when
    result = ArtifactField.from_dict(data)

    # then
    assert result.path == "some/artifact"
    assert result.hash == "f192cddb2b98c0b4c72bba22b68d2245"


def test__artifact_field__from_model():
    # given
    model = Mock(
        attributeType="artifact",
        attributeName="some/artifact",
        hash="f192cddb2b98c0b4c72bba22b68d2245",
    )

    # when
    result = ArtifactField.from_model(model)

    # then
    assert result.path == "some/artifact"
    assert result.hash == "f192cddb2b98c0b4c72bba22b68d2245"


def test__field__from_dict__float():
    # given
    data = {
        "path": "some/float",
        "type": "float",
        "floatProperties": {"attributeType": "float", "attributeName": "some/float", "value": 18.5},
    }

    # when
    result = Field.from_dict(data)

    # then
    assert result.path == "some/float"
    assert isinstance(result, FloatField)
    assert result.value == 18.5


def test__field__from_model__float():
    # given
    model = Mock(
        path="some/float",
        type="float",
        floatProperties=Mock(attributeType="float", attributeName="some/float", value=18.5),
    )

    # when
    result = Field.from_model(model)

    # then
    assert result.path == "some/float"
    assert isinstance(result, FloatField)
    assert result.value == 18.5


def test__field__from_dict__int():
    # given
    data = {
        "path": "some/int",
        "type": "int",
        "intProperties": {"attributeType": "int", "attributeName": "some/int", "value": 18},
    }

    # when
    result = Field.from_dict(data)

    # then
    assert result.path == "some/int"
    assert isinstance(result, IntField)
    assert result.value == 18


def test__field__from_model__int():
    # given
    model = Mock(
        path="some/int", type="int", intProperties=Mock(attributeType="int", attributeName="some/int", value=18)
    )

    # when
    result = Field.from_model(model)

    # then
    assert result.path == "some/int"
    assert isinstance(result, IntField)
    assert result.value == 18


def test__field__from_dict__string():
    # given
    data = {
        "path": "some/string",
        "type": "string",
        "stringProperties": {"attributeType": "string", "attributeName": "some/string", "value": "hello"},
    }

    # when
    result = Field.from_dict(data)

    # then
    assert result.path == "some/string"
    assert isinstance(result, StringField)
    assert result.value == "hello"


def test__field__from_model__string():
    # given
    model = Mock(
        path="some/string",
        type="string",
        stringProperties=Mock(attributeType="string", attributeName="some/string", value="hello"),
    )

    # when
    result = Field.from_model(model)

    # then
    assert result.path == "some/string"
    assert isinstance(result, StringField)
    assert result.value == "hello"


def test__field__from_dict__bool():
    # given
    data = {
        "path": "some/bool",
        "type": "bool",
        "boolProperties": {"attributeType": "bool", "attributeName": "some/bool", "value": True},
    }

    # when
    result = Field.from_dict(data)

    # then
    assert result.path == "some/bool"
    assert isinstance(result, BoolField)
    assert result.value is True


def test__field__from_model__bool():
    # given
    model = Mock(
        path="some/bool", type="bool", boolProperties=Mock(attributeType="bool", attributeName="some/bool", value=True)
    )

    # when
    result = Field.from_model(model)

    # then
    assert result.path == "some/bool"
    assert isinstance(result, BoolField)
    assert result.value is True


def test__field__from_dict__datetime():
    # given
    data = {
        "path": "some/datetime",
        "type": "datetime",
        "datetimeProperties": {
            "attributeType": "datetime",
            "attributeName": "some/datetime",
            "value": "2024-01-01T00:12:34.567890Z",
        },
    }

    # when
    result = Field.from_dict(data)

    # then
    assert result.path == "some/datetime"
    assert isinstance(result, DateTimeField)
    assert result.value == datetime.datetime(2024, 1, 1, 0, 12, 34, 567890)


def test__field__from_model__datetime():
    # given
    model = Mock(
        path="some/datetime",
        type="datetime",
        datetimeProperties=Mock(
            attributeType="datetime", attributeName="some/datetime", value="2024-01-01T00:12:34.567890Z"
        ),
    )

    # when
    result = Field.from_model(model)

    # then
    assert result.path == "some/datetime"
    assert isinstance(result, DateTimeField)
    assert result.value == datetime.datetime(2024, 1, 1, 0, 12, 34, 567890)


def test__field__from_dict__float_series():
    # given
    data = {
        "path": "some/floatSeries",
        "type": "floatSeries",
        "floatSeriesProperties": {"attributeType": "floatSeries", "attributeName": "some/floatSeries", "last": 19.5},
    }

    # when
    result = Field.from_dict(data)

    # then
    assert result.path == "some/floatSeries"
    assert isinstance(result, FloatSeriesField)
    assert result.last == 19.5


def test__field__from_model__float_series():
    # given
    model = Mock(
        path="some/floatSeries",
        type="floatSeries",
        floatSeriesProperties=Mock(attributeType="floatSeries", attributeName="some/floatSeries", last=19.5),
    )

    # when
    result = Field.from_model(model)

    # then
    assert result.path == "some/floatSeries"
    assert isinstance(result, FloatSeriesField)
    assert result.last == 19.5


def test__field__from_dict__string_series():
    # given
    data = {
        "path": "some/stringSeries",
        "type": "stringSeries",
        "stringSeriesProperties": {
            "attributeType": "stringSeries",
            "attributeName": "some/stringSeries",
            "last": "hello",
        },
    }

    # when
    result = Field.from_dict(data)

    # then
    assert result.path == "some/stringSeries"
    assert isinstance(result, StringSeriesField)
    assert result.last == "hello"


def test__field__from_model__string_series():
    # given
    model = Mock(
        path="some/stringSeries",
        type="stringSeries",
        stringSeriesProperties=Mock(attributeType="stringSeries", attributeName="some/stringSeries", last="hello"),
    )

    # when
    result = Field.from_model(model)

    # then
    assert result.path == "some/stringSeries"
    assert isinstance(result, StringSeriesField)
    assert result.last == "hello"


def test__field__from_dict__image_series():
    # given
    data = {
        "path": "some/imageSeries",
        "type": "imageSeries",
        "imageSeriesProperties": {
            "attributeType": "imageSeries",
            "attributeName": "some/imageSeries",
            "lastStep": 15.0,
        },
    }

    # when
    result = Field.from_dict(data)

    # then
    assert result.path == "some/imageSeries"
    assert isinstance(result, ImageSeriesField)
    assert result.last_step == 15.0


def test__field__from_model__image_series():
    # given
    model = Mock(
        path="some/imageSeries",
        type="imageSeries",
        imageSeriesProperties=Mock(attributeType="imageSeries", attributeName="some/imageSeries", lastStep=15.0),
    )

    # when
    result = Field.from_model(model)

    # then
    assert result.path == "some/imageSeries"
    assert isinstance(result, ImageSeriesField)
    assert result.last_step == 15.0


def test__field__from_dict__string_set():
    # given
    data = {
        "path": "some/stringSet",
        "type": "stringSet",
        "stringSetProperties": {
            "attributeType": "stringSet",
            "attributeName": "some/stringSet",
            "values": ["hello", "world"],
        },
    }

    # when
    result = Field.from_dict(data)

    # then
    assert result.path == "some/stringSet"
    assert isinstance(result, StringSetField)
    assert result.values == {"hello", "world"}


def test__field__from_model__string_set():
    # given
    model = Mock(
        path="some/stringSet",
        type="stringSet",
        stringSetProperties=Mock(attributeType="stringSet", attributeName="some/stringSet", values=["hello", "world"]),
    )

    # when
    result = Field.from_model(model)

    # then
    assert result.path == "some/stringSet"
    assert isinstance(result, StringSetField)
    assert result.values == {"hello", "world"}


def test__field__from_dict__file():
    # given
    data = {
        "path": "some/file",
        "type": "file",
        "fileProperties": {
            "attributeType": "file",
            "attributeName": "some/file",
            "name": "file.txt",
            "size": 1024,
            "ext": "txt",
        },
    }

    # when
    result = Field.from_dict(data)

    # then
    assert result.path == "some/file"
    assert isinstance(result, FileField)
    assert result.name == "file.txt"
    assert result.size == 1024
    assert result.ext == "txt"


def test__field__from_model__file():
    # given
    model = Mock(
        path="some/file",
        type="file",
        fileProperties=Mock(attributeType="file", attributeName="some/file", size=1024, ext="txt"),
    )
    model.fileProperties.name = "file.txt"

    # when
    result = Field.from_model(model)

    # then
    assert result.path == "some/file"
    assert isinstance(result, FileField)
    assert result.name == "file.txt"
    assert result.size == 1024
    assert result.ext == "txt"


def test__field__from_dict__object_state():
    # given
    data = {
        "path": "sys/state",
        "type": "experimentState",
        "experimentStateProperties": {
            "attributeType": "experimentState",
            "attributeName": "sys/state",
            "value": "running",
        },
    }

    # when
    result = Field.from_dict(data)

    # then
    assert result.path == "sys/state"
    assert isinstance(result, ObjectStateField)
    assert result.value == "Active"


def test__field__from_model__object_state():
    # given
    model = Mock(
        path="sys/state",
        type="experimentState",
        experimentStateProperties=Mock(attributeType="experimentState", attributeName="sys/state", value="running"),
    )

    # when
    result = Field.from_model(model)

    # then
    assert result.path == "sys/state"
    assert isinstance(result, ObjectStateField)
    assert result.value == "Active"


def test__field__from_dict__file_set():
    # given
    data = {
        "path": "some/fileSet",
        "type": "fileSet",
        "fileSetProperties": {"attributeType": "fileSet", "attributeName": "some/fileSet", "size": 3072},
    }

    # when
    result = Field.from_dict(data)

    # then
    assert result.path == "some/fileSet"
    assert isinstance(result, FileSetField)
    assert result.size == 3072


def test__field__from_model__file_set():
    # given
    model = Mock(
        path="some/fileSet",
        type="fileSet",
        fileSetProperties=Mock(attributeType="fileSet", attributeName="some/fileSet", size=3072),
    )

    # when
    result = Field.from_model(model)

    # then
    assert result.path == "some/fileSet"
    assert isinstance(result, FileSetField)
    assert result.size == 3072


def test__field__from_dict__notebook_ref():
    # given
    data = {
        "path": "some/notebookRef",
        "type": "notebookRef",
        "notebookRefProperties": {
            "attributeType": "notebookRef",
            "attributeName": "some/notebookRef",
            "notebookName": "Data Processing.ipynb",
        },
    }

    # when
    result = Field.from_dict(data)

    # then
    assert result.path == "some/notebookRef"
    assert isinstance(result, NotebookRefField)
    assert result.notebook_name == "Data Processing.ipynb"


def test__field__from_model__notebook_ref():
    # given
    model = Mock(
        path="some/notebookRef",
        type="notebookRef",
        notebookRefProperties=Mock(
            attributeType="notebookRef", attributeName="some/notebookRef", notebookName="Data Processing.ipynb"
        ),
    )

    # when
    result = Field.from_model(model)

    # then
    assert result.path == "some/notebookRef"
    assert isinstance(result, NotebookRefField)
    assert result.notebook_name == "Data Processing.ipynb"


def test__field__from_dict__git_ref():
    # given
    data = {
        "path": "some/gitRef",
        "type": "gitRef",
        "gitRefProperties": {
            "attributeType": "gitRef",
            "attributeName": "some/gitRef",
            "commit": {"commitId": "b2d7f8a"},
        },
    }

    # when
    result = Field.from_dict(data)

    # then
    assert result.path == "some/gitRef"
    assert isinstance(result, GitRefField)
    assert result.commit.commit_id == "b2d7f8a"


def test__field__from_model__git_ref():
    # given
    model = Mock(
        path="some/gitRef",
        type="gitRef",
        gitRefProperties=Mock(attributeType="gitRef", attributeName="some/gitRef", commit=Mock(commitId="b2d7f8a")),
    )

    # when
    result = Field.from_model(model)

    # then
    assert result.path == "some/gitRef"
    assert isinstance(result, GitRefField)
    assert result.commit.commit_id == "b2d7f8a"


def test__field__from_dict__artifact():
    # given
    data = {
        "path": "some/artifact",
        "type": "artifact",
        "artifactProperties": {
            "attributeType": "artifact",
            "attributeName": "some/artifact",
            "hash": "f192cddb2b98c0b4c72bba22b68d2245",
        },
    }

    # when
    result = Field.from_dict(data)

    # then
    assert result.path == "some/artifact"
    assert isinstance(result, ArtifactField)
    assert result.hash == "f192cddb2b98c0b4c72bba22b68d2245"


def test__field__from_model__artifact():
    # given
    model = Mock(
        path="some/artifact",
        type="artifact",
        artifactProperties=Mock(
            attributeType="artifact", attributeName="some/artifact", hash="f192cddb2b98c0b4c72bba22b68d2245"
        ),
    )

    # when
    result = Field.from_model(model)

    # then
    assert result.path == "some/artifact"
    assert isinstance(result, ArtifactField)
    assert result.hash == "f192cddb2b98c0b4c72bba22b68d2245"


def test__field_definition__from_dict():
    # given
    data = {
        "name": "some/float",
        "type": "float",
    }

    # when
    result = FieldDefinition.from_dict(data)

    # then
    assert result.path == "some/float"
    assert result.type == FieldType.FLOAT


def test__field_definition__from_model():
    # given
    model = Mock(
        type="float",
    )
    model.name = "some/float"

    # when
    result = FieldDefinition.from_model(model)

    # then
    assert result.path == "some/float"
    assert result.type == FieldType.FLOAT


def test__leaderboard_entry__from_dict():
    # given
    data = {
        "experimentId": "some-id",
        "attributes": [
            {
                "path": "some/float",
                "type": "float",
                "floatProperties": {"attributeType": "float", "attributeName": "some/float", "value": 18.5},
            },
            {
                "path": "some/int",
                "type": "int",
                "intProperties": {"attributeType": "int", "attributeName": "some/int", "value": 18},
            },
            {
                "path": "some/string",
                "type": "string",
                "stringProperties": {"attributeType": "string", "attributeName": "some/string", "value": "hello"},
            },
        ],
    }

    # when
    result = LeaderboardEntry.from_dict(data)

    # then
    assert result.object_id == "some-id"
    assert len(result.fields) == 3

    float_field = result.fields[0]
    assert isinstance(float_field, FloatField)
    assert float_field.path == "some/float"
    assert float_field.value == 18.5

    int_field = result.fields[1]
    assert isinstance(int_field, IntField)
    assert int_field.path == "some/int"

    string_field = result.fields[2]
    assert isinstance(string_field, StringField)
    assert string_field.path == "some/string"


def test__leaderboard_entry__from_model():
    # given
    model = Mock(
        experimentId="some-id",
        attributes=[
            Mock(
                path="some/float",
                type="float",
                floatProperties=Mock(attributeType="float", attributeName="some/float", value=18.5),
            ),
            Mock(
                path="some/int", type="int", intProperties=Mock(attributeType="int", attributeName="some/int", value=18)
            ),
            Mock(
                path="some/string",
                type="string",
                stringProperties=Mock(attributeType="string", attributeName="some/string", value="hello"),
            ),
        ],
    )

    # when
    result = LeaderboardEntry.from_model(model)

    # then
    assert result.object_id == "some-id"
    assert len(result.fields) == 3

    float_field = result.fields[0]
    assert isinstance(float_field, FloatField)
    assert float_field.path == "some/float"
    assert float_field.value == 18.5

    int_field = result.fields[1]
    assert isinstance(int_field, IntField)
    assert int_field.path == "some/int"

    string_field = result.fields[2]
    assert isinstance(string_field, StringField)
    assert string_field.path == "some/string"


def test__leaderboard_entries_search_result__from_dict():
    # given
    data = {
        "matchingItemCount": 2,
        "entries": [
            {
                "experimentId": "some-id-1",
                "attributes": [
                    {
                        "path": "some/float",
                        "type": "float",
                        "floatProperties": {"attributeType": "float", "attributeName": "some/float", "value": 18.5},
                    },
                ],
            },
            {
                "experimentId": "some-id-2",
                "attributes": [
                    {
                        "path": "some/int",
                        "type": "int",
                        "intProperties": {"attributeType": "int", "attributeName": "some/int", "value": 18},
                    },
                ],
            },
        ],
    }

    # when
    result = LeaderboardEntriesSearchResult.from_dict(data)

    # then
    assert result.matching_item_count == 2
    assert len(result.entries) == 2

    entry_1 = result.entries[0]
    assert entry_1.object_id == "some-id-1"
    assert len(entry_1.fields) == 1
    assert isinstance(entry_1.fields[0], FloatField)

    entry_2 = result.entries[1]
    assert entry_2.object_id == "some-id-2"
    assert len(entry_2.fields) == 1
    assert isinstance(entry_2.fields[0], IntField)


def test__leaderboard_entries_search_result__from_model():
    # given
    model = Mock(
        matchingItemCount=2,
        entries=[
            Mock(
                experimentId="some-id-1",
                attributes=[
                    Mock(
                        path="some/float",
                        type="float",
                        floatProperties=Mock(attributeType="float", attributeName="some/float", value=18.5),
                    ),
                ],
            ),
            Mock(
                experimentId="some-id-2",
                attributes=[
                    Mock(
                        path="some/int",
                        type="int",
                        intProperties=Mock(attributeType="int", attributeName="some/int", value=18),
                    ),
                ],
            ),
        ],
    )

    # when
    result = LeaderboardEntriesSearchResult.from_model(model)

    # then
    assert result.matching_item_count == 2
    assert len(result.entries) == 2

    entry_1 = result.entries[0]
    assert entry_1.object_id == "some-id-1"
    assert len(entry_1.fields) == 1
    assert isinstance(entry_1.fields[0], FloatField)

    entry_2 = result.entries[1]
    assert entry_2.object_id == "some-id-2"
    assert len(entry_2.fields) == 1
    assert isinstance(entry_2.fields[0], IntField)


@pytest.mark.parametrize("field_type", list(FieldType))
def test__all_field_types__have_class(field_type):
    # when
    field_class = Field.by_type(field_type)

    # then
    assert field_class is not None
    assert field_class.type == field_type


def test__file_entry__from_model():
    # given
    now = datetime.datetime.now()

    # and
    model = Mock(
        size=100,
        mtime=now,
        fileType="file",
    )
    model.name = "mock_name"

    entry = FileEntry.from_dto(model)

    assert entry.name == "mock_name"
    assert entry.size == 100
    assert entry.mtime == now
    assert entry.file_type == "file"
