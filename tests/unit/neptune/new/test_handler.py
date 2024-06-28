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
import argparse
import os
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import (
    datetime,
    timedelta,
)
from unittest.mock import patch

import pytest

from neptune import (
    ANONYMOUS_API_TOKEN,
    init_run,
)
from neptune.attributes.atoms.boolean import Boolean
from neptune.attributes.atoms.datetime import Datetime
from neptune.attributes.atoms.float import Float
from neptune.attributes.atoms.integer import Integer
from neptune.attributes.atoms.string import String
from neptune.attributes.sets.string_set import StringSet
from neptune.envs import (
    API_TOKEN_ENV_NAME,
    PROJECT_ENV_NAME,
)
from neptune.exceptions import (
    NeptuneUnsupportedFunctionalityException,
    NeptuneUserApiInputException,
)
from neptune.internal.warnings import (
    NeptuneUnsupportedType,
    warned_once,
)
from neptune.objects.neptune_object import NeptuneObject
from neptune.types.atoms.datetime import Datetime as DatetimeVal
from neptune.types.atoms.float import Float as FloatVal
from neptune.types.atoms.string import String as StringVal
from neptune.types.namespace import Namespace as NamespaceVal
from neptune.types.series.float_series import FloatSeries as FloatSeriesVal
from neptune.types.series.string_series import StringSeries as StringSeriesVal
from neptune.types.sets.string_set import StringSet as StringSetVal

PIL = pytest.importorskip("PIL")


class Obj:
    pass


@contextmanager
def assert_unsupported_warning():
    warned_once.clear()
    with pytest.warns(NeptuneUnsupportedType):
        yield


@contextmanager
def assert_logged_warning(capsys: pytest.CaptureFixture, msg: str = ""):
    _ = capsys.readouterr()
    yield
    captured = capsys.readouterr()
    assert msg in captured.out


@pytest.mark.skip(reason="Backend not implemented")
@patch.object(
    NeptuneObject,
    "_async_create_run",
    lambda self: self._backend._create_container(self._custom_id, self.container_type, self._project_id),
)
class TestBaseAssign:
    @classmethod
    def setUpClass(cls) -> None:
        os.environ[PROJECT_ENV_NAME] = "organization/project"
        os.environ[API_TOKEN_ENV_NAME] = ANONYMOUS_API_TOKEN

    def test_assign_operator(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            now = datetime.now()
            exp["some/num/val"] = 5.0
            exp["some/str/val"] = "some text"
            exp["some/datetime/val"] = now

            exp.wait()

            assert exp["some/num/val"].fetch() == 5.0
            assert exp["some/str/val"].fetch() == "some text"
            assert exp["some/datetime/val"].fetch() == now.replace(microsecond=1000 * int(now.microsecond / 1000))
            assert isinstance(exp.get_structure()["some"]["num"]["val"], Float)
            assert isinstance(exp.get_structure()["some"]["str"]["val"], String)
            assert isinstance(exp.get_structure()["some"]["datetime"]["val"], Datetime)

    def test_assign(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            now = datetime.now()
            exp["some/num/val"].assign(5.0)
            exp["some/int/val"].assign(50)
            exp["some/str/val"].assign("some text", wait=True)
            exp["some/bool/val"].assign(True)
            exp["some/datetime/val"].assign(now)
            assert exp["some/num/val"].fetch() == 5.0
            assert exp["some/int/val"].fetch() == 50
            assert exp["some/str/val"].fetch() == "some text"
            assert exp["some/bool/val"].fetch()  # == True
            assert exp["some/datetime/val"].fetch() == now.replace(microsecond=1000 * int(now.microsecond / 1000))
            assert isinstance(exp.get_structure()["some"]["num"]["val"], Float)
            assert isinstance(exp.get_structure()["some"]["int"]["val"], Integer)
            assert isinstance(exp.get_structure()["some"]["str"]["val"], String)
            assert isinstance(exp.get_structure()["some"]["bool"]["val"], Boolean)
            assert isinstance(exp.get_structure()["some"]["datetime"]["val"], Datetime)

            now = now + timedelta(seconds=3, microseconds=500000)
            exp["some/num/val"].assign(FloatVal(15))
            exp["some/str/val"].assign(StringVal("other text"), wait=False)
            exp["some/datetime/val"].assign(DatetimeVal(now), wait=True)
            assert exp["some/num/val"].fetch() == 15
            assert exp["some/str/val"].fetch() == "other text"
            assert exp["some/datetime/val"].fetch() == now.replace(microsecond=1000 * int(now.microsecond / 1000))
            assert isinstance(exp.get_structure()["some"]["num"]["val"], Float)
            assert isinstance(exp.get_structure()["some"]["str"]["val"], String)
            assert isinstance(exp.get_structure()["some"]["datetime"]["val"], Datetime)

    def test_lookup(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            ns = exp["some/ns"]
            ns["val"] = 5
            exp.wait()
            assert exp["some/ns/val"].fetch() == 5

            ns = exp["other/ns"]
            exp["other/ns/some/value"] = 3
            exp.wait()
            assert ns["some/value"].fetch() == 3

    def test_stringify_path(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            exp[None] = 5
            exp[0] = 5
            exp[5] = 5

            exp["ns"][None] = 7
            exp["ns"][0] = 7
            exp["ns"][5] = 7

            exp.wait()

            assert exp[None].fetch() == 5
            assert exp[0].fetch() == 5
            assert exp[5].fetch() == 5

            assert exp.exists("ns/None")
            assert exp.exists("ns/0")
            assert exp.exists("ns/5")

            assert exp["ns"][None].fetch() == 7
            assert exp["ns"][0].fetch() == 7
            assert exp["ns"][5].fetch() == 7


@patch.object(
    NeptuneObject,
    "_async_create_run",
    lambda self: self._backend._create_container(self._custom_id, self.container_type, self._project_id),
)
class TestSeries:
    @classmethod
    def setUpClass(cls) -> None:
        os.environ[PROJECT_ENV_NAME] = "organization/project"
        os.environ[API_TOKEN_ENV_NAME] = ANONYMOUS_API_TOKEN

    @pytest.mark.xfail(reason="fetch_last disabled", strict=True, raises=NeptuneUnsupportedFunctionalityException)
    def test_assign_series(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            exp["some/num/val"].assign(FloatSeriesVal([1, 2, 0, 10]))
            exp["some/str/val"].assign(StringSeriesVal(["text1", "text2"]), wait=True)
            assert exp["some"]["num"]["val"].fetch_last() == 10
            assert exp["some"]["str"]["val"].fetch_last() == "text2"

            exp["some/num/val"].assign(FloatSeriesVal([122, 543, 2, 5]))
            exp["some/str/val"].assign(StringSeriesVal(["other 1", "other 2", "other 3"]), wait=True)
            assert exp["some"]["num"]["val"].fetch_last() == 5
            assert exp["some"]["str"]["val"].fetch_last() == "other 3"

    @pytest.mark.xfail(reason="Fetch last disabled", strict=True, raises=NeptuneUnsupportedFunctionalityException)
    def test_log(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            exp["some/num/val"].log(5)
            exp["some/str/val"].log("some text")
            assert exp["some"]["num"]["val"].fetch_last() == 5
            assert exp["some"]["str"]["val"].fetch_last() == "some text"

    @pytest.mark.xfail(reason="fetch_last disabled", strict=True, raises=NeptuneUnsupportedFunctionalityException)
    def test_log_dict(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            dict_value = str({"key-a": "value-a", "key-b": "value-b"})
            exp["some/num/val"].log(dict_value)
            assert exp["some"]["num"]["val"].fetch_last() == str(dict_value)

    @pytest.mark.xfail(reason="fetch_last disabled", strict=True, raises=NeptuneUnsupportedFunctionalityException)
    def test_append(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            exp["some/num/val"].append(5)
            exp["some/str/val"].append("some text")
            assert exp["some"]["num"]["val"].fetch_last() == 5
            assert exp["some"]["str"]["val"].fetch_last() == "some text"

    @pytest.mark.xfail(reason="fetch_last disabled", strict=True, raises=NeptuneUnsupportedFunctionalityException)
    def test_append_dict(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            dict_value = {"key-a": "value-a", "key-b": "value-b"}
            exp["some/num/val"].append(dict_value)
            assert exp["some"]["num"]["val"]["key-a"].fetch_last() == "value-a"
            assert exp["some"]["num"]["val"]["key-b"].fetch_last() == "value-b"

    @pytest.mark.xfail(reason="fetch_last disabled", strict=True, raises=NeptuneUnsupportedFunctionalityException)
    def test_append_complex_input(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            exp["train/dictOfDicts"].append(
                {
                    "key-a": {"aa": 11, "ab": 22},
                    "key-b": {"ba": 33, "bb": 44},
                }
            )
            assert exp["train"]["dictOfDicts"]["key-a"]["aa"].fetch_last() == 11
            assert exp["train"]["dictOfDicts"]["key-a"]["ab"].fetch_last() == 22
            assert exp["train"]["dictOfDicts"]["key-b"]["ba"].fetch_last() == 33
            assert exp["train"]["dictOfDicts"]["key-b"]["bb"].fetch_last() == 44

    @pytest.mark.xfail(reason="File logging disabled", strict=True, raises=NeptuneUnsupportedFunctionalityException)
    def test_log_many_values(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            exp["some/num/val"].log([5, 10, 15])
            exp["some/str/val"].log(["some text", "other"])
            assert exp["some"]["num"]["val"].fetch_last() == 15
            assert exp["some"]["str"]["val"].fetch_last() == "other"

    def test_append_many_values_cause_error(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            with assert_unsupported_warning():
                exp["some/empty-list/val"].append([])

            with assert_unsupported_warning():
                exp["some/tuple/val"].append(())

            with assert_unsupported_warning():
                exp["some/list/val"].append([5, 10, 15])

            with assert_unsupported_warning():
                exp["some/str-tuple/val"].append(("some text", "other"))

            with assert_unsupported_warning():
                exp["some/dict-list/val"].append({"key-a": [1, 2]})

            with assert_unsupported_warning():
                exp["some/custom-obj/val"].append(Obj())

            with assert_unsupported_warning():
                exp["some/list-custom-obj/val"].append([Obj(), Obj()])

    @pytest.mark.xfail(reason="fetch_last disabled", strict=True, raises=NeptuneUnsupportedFunctionalityException)
    def test_extend(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            exp["some/num/val"].extend([5, 7])
            exp["some/str/val"].extend(["some", "text"])
            assert exp["some"]["num"]["val"].fetch_last() == 7
            assert exp["some"]["str"]["val"].fetch_last() == "text"

    @pytest.mark.xfail(reason="fetch_last disabled", strict=True, raises=NeptuneUnsupportedFunctionalityException)
    def test_extend_dict(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            dict_value = {"key-a": ["value-a", "value-aa"], "key-b": ["value-b", "value-bb"], "key-c": ["ccc"]}
            exp["some/num/val"].extend(dict_value)
            assert exp["some"]["num"]["val"]["key-a"].fetch_last() == "value-aa"
            assert exp["some"]["num"]["val"]["key-b"].fetch_last() == "value-bb"
            assert exp["some"]["num"]["val"]["key-c"].fetch_last() == "ccc"

    @pytest.mark.xfail(reason="fetch_last disabled", strict=True, raises=NeptuneUnsupportedFunctionalityException)
    def test_extend_nested(self):
        """We expect that we are able to log arbitrary tre structure"""
        with init_run(mode="debug", flush_period=0.5) as exp:
            exp["train/simple_dict"].extend({"list1": [1, 2, 3], "list2": [10, 20, 30]})
            exp["train/simple_dict"].extend(
                {
                    "list1": [4, 5, 6],
                }
            )
            assert exp["train"]["simple_dict"]["list1"].fetch_last() == 6
            assert list(exp["train"]["simple_dict"]["list1"].fetch_values().value) == [1, 2, 3, 4, 5, 6]
            assert exp["train"]["simple_dict"]["list2"].fetch_last() == 30
            assert list(exp["train"]["simple_dict"]["list2"].fetch_values().value) == [10, 20, 30]

            exp["train/different-depths"].extend(
                {"lvl1": {"lvl1.1": [1, 2, 3], "lvl1.2": {"lvl1.2.1": [1]}}, "lvl2": [10, 20]}
            )
            exp["train/different-depths/lvl1"].extend({"lvl1.2": {"lvl1.2.1": [2, 3]}})
            assert exp["train"]["different-depths"]["lvl1"]["lvl1.1"].fetch_last() == 3
            assert list(exp["train"]["different-depths"]["lvl1"]["lvl1.1"].fetch_values().value) == [1, 2, 3]
            assert exp["train"]["different-depths"]["lvl1"]["lvl1.2"]["lvl1.2.1"].fetch_last() == 3
            assert list(exp["train"]["different-depths"]["lvl1"]["lvl1.2"]["lvl1.2.1"].fetch_values().value) == [
                1,
                2,
                3,
            ]
            assert exp["train"]["different-depths"]["lvl2"].fetch_last() == 20
            assert list(exp["train"]["different-depths"]["lvl2"].fetch_values().value) == [10, 20]

    def test_extend_nested_with_wrong_parameters(self):
        """We expect that we are able to log arbitrary tre structure"""
        with init_run(mode="debug", flush_period=0.5) as exp:
            with pytest.raises(NeptuneUserApiInputException):
                # wrong number of steps
                exp["train/simple_dict"].extend(values={"list1": [1, 2, 3], "list2": [10, 20, 30]}, steps=[0, 1])

            with pytest.raises(NeptuneUserApiInputException):
                # wrong number of timestamps
                exp["train/simple_dict"].extend(
                    values={"list1": [1, 2, 3], "list2": [10, 20, 30]}, timestamps=[time.time()] * 2
                )

    @pytest.mark.xfail(reason="Fetch last disabled", strict=True, raises=NeptuneUnsupportedFunctionalityException)
    def test_log_value_errors(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            with pytest.raises(ValueError):
                exp["x"].log([])
            with pytest.raises(ValueError):
                exp["x"].log([5, "str"])
            with pytest.raises(ValueError):
                exp["x"].log([5, 10], step=10)

            exp["some/num/val"].log([5], step=1)
            exp["some/num/val"].log([])
            with pytest.raises(ValueError):
                exp["some/num/val"].log("str")

            exp["some/str/val"].log(["str"], step=1)
            exp["some/str/val"].log([])

            assert exp["some"]["num"]["val"].fetch_last() == 5
            assert exp["some"]["str"]["val"].fetch_last() == "str"


@pytest.mark.skip(reason="Backend not implemented")
@patch.object(
    NeptuneObject,
    "_async_create_run",
    lambda self: self._backend._create_container(self._custom_id, self.container_type, self._project_id),
)
class TestSet:
    @classmethod
    def setUpClass(cls) -> None:
        os.environ[PROJECT_ENV_NAME] = "organization/project"
        os.environ[API_TOKEN_ENV_NAME] = ANONYMOUS_API_TOKEN

    @pytest.mark.xfail(reason="fetch_last disabled", strict=True, raises=NeptuneUnsupportedFunctionalityException)
    def test_append_errors(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            exp["some/num/val"].append(5, step=1)
            with pytest.raises(ValueError):
                exp["some/num/val"].append("str")

            exp["some/str/val"].append("str", step=1)

            assert exp["some"]["num"]["val"].fetch_last() == 5
            assert exp["some"]["str"]["val"].fetch_last() == "str"

    def test_extend_value_errors(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            with pytest.raises(NeptuneUserApiInputException):
                exp["x"].extend(10, step=10)
            with pytest.raises(ValueError):
                exp["x"].extend([5, "str"])

    def test_assign_set(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            exp["some/str/val"].assign(StringSetVal(["tag1", "tag2"]), wait=True)
            assert exp["some/str/val"].fetch() == {"tag1", "tag2"}
            assert isinstance(exp.get_structure()["some"]["str"]["val"], StringSet)

            exp["some/str/val"].assign(StringSetVal(["other_1", "other_2", "other_3"]), wait=True)
            assert exp["some/str/val"].fetch() == {"other_1", "other_2", "other_3"}
            assert isinstance(exp.get_structure()["some"]["str"]["val"], StringSet)

    def test_add(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            exp["some/str/val"].add(["some text", "something else"], wait=True)
            assert exp["some/str/val"].fetch() == {"some text", "something else"}

            exp["some/str/val"].add("one more", wait=True)
            assert exp["some/str/val"].fetch() == {"some text", "something else", "one more"}

            assert isinstance(exp.get_structure()["some"]["str"]["val"], StringSet)


@pytest.mark.skip(reason="Backend not implemented")
@patch.object(
    NeptuneObject,
    "_async_create_run",
    lambda self: self._backend._create_container(self._custom_id, self.container_type, self._project_id),
)
class TestNamespace:
    @classmethod
    def setUpClass(cls) -> None:
        os.environ[PROJECT_ENV_NAME] = "organization/project"
        os.environ[API_TOKEN_ENV_NAME] = ANONYMOUS_API_TOKEN

    @pytest.mark.xfail(reason="fetch_last disabled", strict=True, raises=NeptuneUnsupportedFunctionalityException)
    def test_assign_dict(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            exp["params"] = {
                "x": 5,
                "metadata": {"name": "Trol", "age": 376},
                "toys": StringSeriesVal(["cudgel", "hat"]),
                "nested": {"nested": {"deep_secret": FloatSeriesVal([13, 15])}},
            }
            assert exp["params/x"].fetch() == 5
            assert exp["params/metadata/name"].fetch() == "Trol"
            assert exp["params/metadata/age"].fetch() == 376
            assert exp["params/toys"].fetch_last() == "hat"
            assert exp["params/nested/nested/deep_secret"].fetch_last() == 15

    def test_assign_empty_dict(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            exp["params"] = {}
            exp["params"] = {"foo": 5}
            assert exp["params/foo"].fetch() == 5

    def test_argparse_namespace(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            exp["params"] = argparse.Namespace(
                foo="bar", baz=42, nested=argparse.Namespace(nested_attr=str([1, 2, 3]), num=55)
            )
            assert exp["params/foo"].fetch() == "bar"
            assert exp["params/baz"].fetch() == 42
            assert exp["params/nested/nested_attr"].fetch() == "[1, 2, 3]"
            assert exp["params/nested/num"].fetch() == 55

    def test_assign_namespace(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            exp["some/namespace"].assign(
                NamespaceVal(
                    {
                        "sub-namespace/val1": 1.0,
                        "sub-namespace/val2": StringSetVal(["tag1", "tag2"]),
                    }
                )
            )
            assert exp["some/namespace/sub-namespace/val1"].fetch() == 1.0
            assert exp["some/namespace/sub-namespace/val2"].fetch() == {"tag1", "tag2"}
            assert isinstance(exp.get_structure()["some"]["namespace"]["sub-namespace"]["val1"], Float)
            assert isinstance(exp.get_structure()["some"]["namespace"]["sub-namespace"]["val2"], StringSet)

            exp["some"].assign(NamespaceVal({"namespace/sub-namespace/val1": 2.0}))
            assert exp["some/namespace/sub-namespace/val1"].fetch() == 2.0
            assert exp["some/namespace/sub-namespace/val2"].fetch() == {"tag1", "tag2"}
            assert isinstance(exp.get_structure()["some"]["namespace"]["sub-namespace"]["val1"], Float)
            assert isinstance(exp.get_structure()["some"]["namespace"]["sub-namespace"]["val2"], StringSet)

            with pytest.raises(TypeError):
                exp["some"].assign(NamespaceVal({"namespace/sub-namespace/val1": {"tagA", "tagB"}}))

    def test_fetch_dict(self):
        now = datetime.now()

        with init_run(mode="debug", flush_period=0.5) as exp:
            exp["params/int"] = 1
            exp["params/float"] = 3.14
            exp["params/bool"] = True
            exp["params/datetime"] = now
            exp["params/sub-namespace/int"] = 42
            exp["params/sub-namespace/string"] = "Some text"

            # attributes to be ignored
            exp["params/sub-namespace/string_series"].log("Some text #1")
            exp["params/sub-namespace/int_series"].log(100)

            params_dict = exp["params"].fetch()
            assert params_dict == {
                "int": 1,
                "float": 3.14,
                "bool": True,
                "datetime": now.replace(microsecond=1000 * int(now.microsecond / 1000)),
                "sub-namespace": {
                    "int": 42,
                    "string": "Some text",
                },
            }

    def test_fetch_dict_with_path(self):
        now = datetime.now()

        with init_run(mode="debug", flush_period=0.5) as exp:
            exp["params/int"] = 1
            exp["params/float"] = 3.14
            exp["params/bool"] = True
            exp["params/datetime"] = now
            exp["params/sub-namespace/int"] = 42
            exp["params/sub-namespace/string"] = "Some text"

            params_dict = exp["params/sub-namespace"].fetch()
            assert params_dict == {"int": 42, "string": "Some text"}

    def test_assign_drops_dict_entry_with_empty_key(self, capsys):
        with init_run(mode="debug", flush_period=0.5) as exp:
            with assert_logged_warning(capsys, '"" can\'t be used in Namespaces and dicts stored in Neptune'):
                exp["some/namespace"] = {"": 1.1, "x": "Some text"}
                params_dict = exp["some/namespace"].fetch()
                assert params_dict == {"x": "Some text"}
            with assert_logged_warning(capsys, '"///" can\'t be used in Namespaces and dicts stored in Neptune'):
                exp["other/namespace"] = {"///": 1.1, "x": "Some text"}
                params_dict = exp["other/namespace"].fetch()
                assert params_dict == {"x": "Some text"}
            with assert_logged_warning(capsys, "can't be used in Namespaces and dicts stored in Neptune"):
                exp["other/namespace"] = {"": 2, "//": [1, 2], "///": 1.1, "x": "Some text"}
                params_dict = exp["other/namespace"].fetch()
                assert params_dict == {"x": "Some text"}


@patch.object(
    NeptuneObject,
    "_async_create_run",
    lambda self: self._backend._create_container(self._custom_id, self.container_type, self._project_id),
)
class TestDelete:
    @classmethod
    def setUpClass(cls) -> None:
        os.environ[PROJECT_ENV_NAME] = "organization/project"
        os.environ[API_TOKEN_ENV_NAME] = ANONYMOUS_API_TOKEN

    @pytest.mark.xfail(reason="Field deletion disabled", raises=NeptuneUnsupportedFunctionalityException, strict=True)
    def test_pop(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            exp["some/num/val"].assign(3, wait=True)
            assert "some" in exp.get_structure()
            ns = exp["some"]
            ns.pop("num/val", wait=True)
            assert "some" not in exp.get_structure()

    @pytest.mark.xfail(reason="Field deletion disabled", raises=NeptuneUnsupportedFunctionalityException, strict=True)
    def test_pop_self(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            exp["x"].assign(3, wait=True)
            assert "x" in exp.get_structure()
            exp["x"].pop(wait=True)
            assert "x" not in exp.get_structure()

    @pytest.mark.xfail(reason="Field deletion disabled", raises=NeptuneUnsupportedFunctionalityException, strict=True)
    def test_del(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            exp["some/num/val"].assign(3)
            assert "some" in exp.get_structure()
            ns = exp["some"]
            del ns["num/val"]
            assert "some" not in exp.get_structure()


@pytest.mark.skip(reason="Backend not implemented")
@patch.object(
    NeptuneObject,
    "_async_create_run",
    lambda self: self._backend._create_container(self._custom_id, self.container_type, self._project_id),
)
class TestOtherBehaviour:
    @classmethod
    def setUpClass(cls) -> None:
        os.environ[PROJECT_ENV_NAME] = "organization/project"
        os.environ[API_TOKEN_ENV_NAME] = ANONYMOUS_API_TOKEN

    def test_assign_distinct_types(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            exp["some/str/val"].assign(FloatVal(1.0), wait=True)
            assert exp["some/str/val"].fetch() == 1.0
            assert isinstance(exp.get_structure()["some"]["str"]["val"], Float)

            with pytest.raises(TypeError):
                exp["some/str/val"].assign(StringSetVal(["other_1", "other_2", "other_3"]), wait=True)

    def test_attribute_error(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            with pytest.raises(AttributeError):
                exp["var"].something()

    @pytest.mark.xfail(reason="fetch_last disabled", strict=True, raises=NeptuneUnsupportedFunctionalityException)
    def test_float_like_types(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            exp.define("attr1", self.FloatLike(5))
            assert exp["attr1"].fetch() == 5
            exp["attr1"] = "234"
            assert exp["attr1"].fetch() == 234
            with pytest.raises(ValueError):
                exp["attr1"] = "234a"

            exp["attr2"].assign(self.FloatLike(34))
            assert exp["attr2"].fetch() == 34
            exp["attr2"].assign("555")
            assert exp["attr2"].fetch() == 555
            with pytest.raises(ValueError):
                exp["attr2"].assign("string")

            exp["attr3"].log(self.FloatLike(34))
            assert exp["attr3"].fetch_last() == 34
            exp["attr3"].log(["345", self.FloatLike(34), 4, 13.0])
            assert exp["attr3"].fetch_last() == 13
            with pytest.raises(ValueError):
                exp["attr3"].log([4, "234a"])

    @pytest.mark.xfail(reason="fetch_last disabled", strict=True, raises=NeptuneUnsupportedFunctionalityException)
    def test_append_float_like_types(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            exp["attr"].append(self.FloatLike(34))
            assert exp["attr"].fetch_last() == 34
            exp["attr"].append("345")
            exp["attr"].append(self.FloatLike(34))
            exp["attr"].append(4)
            exp["attr"].append(13.0)
            assert exp["attr"].fetch_last() == 13
            with pytest.raises(ValueError):
                exp["attr"].append(4)
                exp["attr"].append("234a")

    @pytest.mark.xfail(reason="fetch_last disabled", strict=True, raises=NeptuneUnsupportedFunctionalityException)
    def test_extend_float_like_types(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            exp["attr"].extend([self.FloatLike(34)])
            assert exp["attr"].fetch_last() == 34
            exp["attr"].extend(["345", self.FloatLike(34), 4, 13.0])
            assert exp["attr"].fetch_last() == 13
            with pytest.raises(ValueError):
                exp["attr"].extend([4, "234a"])

    @pytest.mark.xfail(reason="fetch_last disabled", strict=True, raises=NeptuneUnsupportedFunctionalityException)
    def test_assign_dict(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            exp["params"] = {
                "x": 5,
                "metadata": {"name": "Trol", "age": 376},
                "toys": StringSeriesVal(["cudgel", "hat"]),
                "nested": {"nested": {"deep_secret": FloatSeriesVal([13, 15])}},
                0: {"some_data": 345},
                None: {"some_data": 345},
            }
            assert exp["params/x"].fetch() == 5
            assert exp["params/metadata/name"].fetch() == "Trol"
            assert exp["params/metadata/age"].fetch() == 376
            assert exp["params/toys"].fetch_last() == "hat"
            assert exp["params/nested/nested/deep_secret"].fetch_last() == 15
            assert exp["params/0/some_data"].fetch() == 345
            assert exp["params/None/some_data"].fetch() == 345

    def test_convertable_to_dict(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            exp["params"] = argparse.Namespace(
                foo="bar", baz=42, nested=argparse.Namespace(nested_attr=str([1, 2, 3]), num=55)
            )
            assert exp["params/foo"].fetch() == "bar"
            assert exp["params/baz"].fetch() == 42
            assert exp["params/nested/nested_attr"].fetch() == "[1, 2, 3]"
            assert exp["params/nested/num"].fetch() == 55

    def test_representation(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            exp["params/int"] = 1
            exp["params/float"] = 3.14
            exp["params/bool"] = True
            exp["params/datetime"] = datetime.now()
            exp["params/sub-namespace/int"] = 42
            exp["params/sub-namespace/string"] = "Some text"

            assert repr(exp["params"]) == '<Namespace field at "params">'
            assert repr(exp["params/int"]) == '<Integer field at "params/int">'
            assert repr(exp["params/float"]) == '<Float field at "params/float">'
            assert repr(exp["params/bool"]) == '<Boolean field at "params/bool">'
            assert repr(exp["params/datetime"]) == '<Datetime field at "params/datetime">'
            assert repr(exp["params/unassigned"]) == '<Unassigned field at "params/unassigned">'

            sub_namespace = exp["params/sub-namespace"]
            assert repr(sub_namespace["int"]) == '<Integer field at "params/sub-namespace/int">'
            assert repr(sub_namespace["string"]) == '<String field at "params/sub-namespace/string">'
            assert repr(sub_namespace["unassigned"]) == '<Unassigned field at "params/sub-namespace/unassigned">'

    @dataclass
    class FloatLike:
        value: float

        def __float__(self):
            return float(self.value)
