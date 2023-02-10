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
from contextlib import contextmanager

from pytest import (
    fixture,
    warns,
)

from neptune import init_run
from neptune.common.deprecation import NeptuneDeprecationWarning
from neptune.types import (
    Boolean,
    String,
)
from neptune.utils import stringify_unsupported


class Obj:
    def __init__(self, name: str = "A"):
        self._name = name

    def __str__(self):
        return f"Object(name={self._name})"


@contextmanager
def assert_deprecation_warning():
    yield warns(expected_warning=NeptuneDeprecationWarning)


@contextmanager
def assert_no_warnings():
    with warns(None) as record:
        yield

    assert len(record) == 0, f"Warning detected: {record[0].message}"


@fixture
def run():
    with init_run(mode="debug") as run:
        yield run


class TestStringifyUnsupported:
    def test_assign__custom_object(self, run):
        with assert_deprecation_warning():
            run["with_warning"] = Obj()

        with assert_no_warnings():
            run["no_warning"] = stringify_unsupported(Obj())

        assert run["with_warning"].fetch() == run["no_warning"].fetch()

    def test_assign__custom_object__reassign(self, run):
        with assert_deprecation_warning():
            run["with_warning"] = Obj()
            run["with_warning"] = Obj(name="b")

        with assert_no_warnings():
            run["no_warning"] = stringify_unsupported(Obj())
            run["no_warning"] = stringify_unsupported(Obj(name="b"))

        assert run["with_warning"].fetch() == run["no_warning"].fetch()

    def test_assign__float(self, run):
        with assert_deprecation_warning():
            run["with_warning"] = 4.0

        with assert_no_warnings():
            run["no_warning"] = stringify_unsupported(4.0)

        assert run["with_warning"].fetch() == run["no_warning"].fetch()

    def test_assign__array(self, run):
        values = [Obj(), Obj(), Obj()]

        with assert_deprecation_warning():
            run["with_warning"] = values

        with assert_no_warnings():
            run["no_warning"] = stringify_unsupported(values)

        assert run["with_warning"].fetch() == run["no_warning"].fetch()

    def test_assign__array_inside_dict(self, run):
        values = [Obj(), Obj(), Obj()]

        with assert_deprecation_warning():
            run["with_warning"] = {"array": values}

        with assert_no_warnings():
            run["no_warning"] = stringify_unsupported({"array": values})

        assert run["with_warning"]["array"].fetch() == run["no_warning"]["array"].fetch()

    def test_assign__float__reassign(self, run):
        with assert_deprecation_warning():
            run["with_warning"] = 4.0
            run["with_warning"] = 5.3

        with assert_no_warnings():
            run["no_warning"] = stringify_unsupported(4.0)
            run["no_warning"] = stringify_unsupported(5.3)

        assert run["with_warning"].fetch() == run["no_warning"].fetch()

    def test_assign__string(self, run):
        with assert_deprecation_warning():
            run["with_warning"] = String("Nothing to be worry about")

        with assert_no_warnings():
            run["no_warning"] = String(stringify_unsupported("Nothing to be worry about"))

        assert run["with_warning"].fetch() == run["no_warning"].fetch()

    def test_assign__string__reassign(self, run):
        with assert_deprecation_warning():
            run["with_warning"] = String("Nothing to be worry about")
            run["with_warning"] = String("... or maybe")

        with assert_no_warnings():
            run["no_warning"] = String(stringify_unsupported("Nothing to be worry about"))
            run["no_warning"] = String(stringify_unsupported("... or maybe"))

        assert run["with_warning"].fetch() == run["no_warning"].fetch()

    def test_assign__string__custom_object(self, run):
        with assert_deprecation_warning():
            run["with_warning"] = String(Obj())

        with assert_no_warnings():
            run["no_warning"] = String(stringify_unsupported(Obj()))

        assert run["with_warning"].fetch() == run["no_warning"].fetch()

    def test_assign__string__custom_object__reassign(self, run):
        with assert_deprecation_warning():
            run["with_warning"] = String(Obj())
            run["with_warning"] = String(Obj(name="B"))

        with assert_no_warnings():
            run["no_warning"] = String(stringify_unsupported(Obj()))
            run["no_warning"] = String(stringify_unsupported(Obj(name="B")))

        assert run["with_warning"].fetch() == run["no_warning"].fetch()

    def test_assign__string__float(self, run):
        with assert_deprecation_warning():
            run["with_warning"] = String(4.0)

        with assert_no_warnings():
            run["no_warning"] = String(stringify_unsupported(4.0))

        assert run["with_warning"].fetch() == run["no_warning"].fetch()

    def test_assign__string__float__reassign(self, run):
        with assert_deprecation_warning():
            run["with_warning"] = String(4.0)
            run["with_warning"] = String(5.3)

        with assert_no_warnings():
            run["no_warning"] = String(stringify_unsupported(4.0))
            run["no_warning"] = String(stringify_unsupported(5.3))

        assert run["with_warning"].fetch() == run["no_warning"].fetch()

    def test_assign__tuple(self, run):
        values = (Obj(), Obj(), Obj())

        with assert_deprecation_warning():
            run["with_warning"] = values

        with assert_no_warnings():
            run["no_warning"] = stringify_unsupported(values)

        assert run["with_warning"].fetch() == run["no_warning"].fetch()

    def test_assign__tuple_inside_dict(self, run):
        values = (Obj(), Obj(), Obj())

        with assert_deprecation_warning():
            run["with_warning"] = {"tuple": values}

        with assert_no_warnings():
            run["no_warning"] = stringify_unsupported({"tuple": values})

        assert run["with_warning"].fetch() == run["no_warning"].fetch()

    def test_assign__dict(self, run):
        with assert_deprecation_warning():
            run["with_warning"] = {"a": Obj(), "b": "Test", "c": 25, "d": 1997, "e": {"f": Boolean(True)}}

        with assert_no_warnings():
            run["no_warning"] = stringify_unsupported(
                {"a": Obj(), "b": "Test", "c": 25, "d": 1997, "e": {"f": Boolean(True)}}
            )

        assert run["with_warning"].fetch() == run["no_warning"].fetch()

    def test_assign__dict__reassign(self, run):
        with assert_deprecation_warning():
            run["with_warning"] = {"a": Obj(), "b": "Test", "c": 25, "d": 1997, "e": {"f": Boolean(True)}}
            run["with_warning"] = {"a": Obj(name="B"), "d": 12, "e": {"f": Boolean(False)}}

        with assert_no_warnings():
            run["no_warning"] = stringify_unsupported(
                {"a": Obj(), "b": "Test", "c": 25, "d": 1997, "e": {"f": Boolean(True)}}
            )
            run["no_warning"] = stringify_unsupported({"a": Obj(name="B"), "d": 12, "e": {"f": Boolean(False)}})

        assert run["with_warning"].fetch() == run["no_warning"].fetch()

    def test_log__custom_object(self, run):
        with assert_deprecation_warning():
            run["with_warning"].log(Obj())

        with assert_no_warnings():
            run["no_warning"].log(stringify_unsupported(Obj()))

        assert run["with_warning"].fetch_values().equals(run["no_warning"].fetch_values())

    def test_log__list_of_custom_objects(self, run):
        with assert_deprecation_warning():
            run["with_warning"].log([Obj(), Obj(), Obj(), Obj(), Obj()])
            run["with_warning"].log(Obj())
            run["with_warning"].log([Obj(), Obj(), Obj(), Obj(), Obj()])

        with assert_no_warnings():
            run["no_warning"].log(stringify_unsupported([Obj(), Obj(), Obj(), Obj(), Obj()]))
            run["no_warning"].log(stringify_unsupported(Obj()))
            run["no_warning"].log(stringify_unsupported([Obj(), Obj(), Obj(), Obj(), Obj()]))

        assert run["with_warning"].fetch_values().equals(run["no_warning"].fetch_values())

    def test_log__float(self, run):
        with assert_deprecation_warning():
            run["with_warning"].log([1.0, 2.0, 3.0, 4.0, 5.0])

        with assert_no_warnings():
            run["no_warning"].log(stringify_unsupported([1.0, 2.0, 3.0, 4.0, 5.0]))

        assert run["with_warning"].fetch_values().equals(run["no_warning"].fetch_values())

    def test_extend__float(self, run):
        with assert_deprecation_warning():
            run["with_warning"].extend([1.0, 2.0, 3.0, 4.0, 5.0])

        with assert_no_warnings():
            run["no_warning"].extend(stringify_unsupported([1.0, 2.0, 3.0, 4.0, 5.0]))

        assert run["with_warning"].fetch_values().equals(run["no_warning"].fetch_values())

    def test_extend__dict(self, run):
        with assert_deprecation_warning():
            run["with_warning"].extend({"zz": [1.0, 2.0, 3.0, 4.0, 5.0], "bb": [Obj(), Obj(), Obj(), Obj(), Obj()]})

        with assert_no_warnings():
            run["no_warning"].extend(
                stringify_unsupported({"zz": [1.0, 2.0, 3.0, 4.0, 5.0], "bb": [Obj(), Obj(), Obj(), Obj(), Obj()]})
            )

        assert run["with_warning/zz"].fetch_values().equals(run["no_warning/zz"].fetch_values())
        assert run["with_warning/bb"].fetch_values().equals(run["no_warning/bb"].fetch_values())

    def test_append__float(self, run):
        with assert_deprecation_warning():
            run["with_warning"].append(1.0)
            run["with_warning"].append(2.0)

        with assert_no_warnings():
            run["no_warning"].append(stringify_unsupported(1.0))
            run["no_warning"].append(stringify_unsupported(2.0))

        assert run["with_warning"].fetch_values().equals(run["no_warning"].fetch_values())

    def test_append__custom_object(self, run):
        with assert_deprecation_warning():
            run["with_warning"].append(Obj())
            run["with_warning"].append(Obj())

        with assert_no_warnings():
            run["no_warning"].append(stringify_unsupported(Obj()))
            run["no_warning"].append(stringify_unsupported(Obj()))

        assert run["with_warning"].fetch_values().equals(run["no_warning"].fetch_values())

    def test_append__dict(self, run):
        with assert_deprecation_warning():
            run["with_warning"].append({"zz": 1.0})
            run["with_warning"].append({"zz": 2.0, "bb": 3.0})

        with assert_no_warnings():
            run["no_warning"].append(stringify_unsupported({"zz": 1.0}))
            run["no_warning"].append(stringify_unsupported({"zz": 2.0, "bb": 3.0}))

        assert run["with_warning/zz"].fetch_values().equals(run["no_warning/zz"].fetch_values())
        assert run["with_warning/bb"].fetch_values().equals(run["no_warning/bb"].fetch_values())
