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
    raises,
    warns,
)

from neptune import init_run
from neptune.common.warnings import (
    NeptuneUnsupportedType,
    warned_once,
)
from neptune.types import (
    Boolean,
    String,
    StringSeries,
)
from neptune.utils import stringify_unsupported


class Obj:
    def __init__(self, name: str = "A"):
        self._name = name

    def __repr__(self):
        return f"Object(name={self._name})"


@contextmanager
def assert_unsupported_warning():
    warned_once.clear()
    with warns(NeptuneUnsupportedType):
        yield


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
        with assert_unsupported_warning():
            run["unsupported"] = Obj()

        with assert_no_warnings():
            run["stringified"] = stringify_unsupported(Obj())

        with assert_no_warnings():
            run["regular"] = str(Obj())

        assert run["regular"].fetch() == run["stringified"].fetch()

    def test_assign__custom_object__reassign(self, run):
        with assert_unsupported_warning():
            run["unsupported"] = Obj()
            run["unsupported"] = Obj(name="b")

        with assert_no_warnings():
            run["stringified"] = stringify_unsupported(Obj())
            run["stringified"] = stringify_unsupported(Obj(name="b"))

        with assert_no_warnings():
            run["regular"] = str(Obj())
            run["regular"] = str(Obj(name="b"))

        assert run["regular"].fetch() == run["stringified"].fetch()

    def test_assign__float(self, run):
        with assert_no_warnings():
            run["stringified"] = stringify_unsupported(4.0)

        with assert_no_warnings():
            run["regular"] = 4.0

        assert run["regular"].fetch() == run["stringified"].fetch()

    def test_assign__string_series(self, run):
        with assert_no_warnings():
            run["stringified"] = StringSeries(stringify_unsupported([Obj(), Obj()]))

        with assert_no_warnings():
            run["regular"] = StringSeries(([str(Obj()), str(Obj())]))

        assert run["regular"].fetch() == run["stringified"].fetch()

    def test_assign__string_series__reassign(self, run):
        with assert_no_warnings():
            run["stringified"] = StringSeries(stringify_unsupported([Obj(), Obj()]))
            run["stringified"] = StringSeries(stringify_unsupported([Obj(), Obj(), Obj()]))

        with assert_no_warnings():
            run["regular"] = StringSeries(([str(Obj()), str(Obj())]))

        assert run["regular"].fetch() == run["stringified"].fetch()

    def test_assign__array(self, run):
        values = [Obj(), Obj(), Obj()]

        with assert_unsupported_warning():
            run["unsupported"] = values

        with assert_no_warnings():
            run["stringified"] = stringify_unsupported(values)

        with assert_no_warnings():
            run["regular"] = repr([Obj(), Obj(), Obj()])

        assert run["regular"].fetch() == run["stringified"].fetch()

    def test_assign__array_inside_dict(self, run):
        values = [Obj(), Obj(), Obj()]

        with assert_unsupported_warning():
            run["unsupported"] = {"array": values}

        with assert_no_warnings():
            run["stringified"] = stringify_unsupported({"array": values})

        with assert_no_warnings():
            run["regular"] = {"array": str([Obj(), Obj(), Obj()])}

        assert run["regular"]["array"].fetch() == run["stringified"]["array"].fetch()

    def test_assign__float__reassign(self, run):
        with assert_no_warnings():
            run["stringified"] = stringify_unsupported(4.0)
            run["stringified"] = stringify_unsupported(5.3)

        with assert_no_warnings():
            run["regular"] = 4.0
            run["regular"] = 5.3

        assert run["regular"].fetch() == run["stringified"].fetch()

    def test_assign__string(self, run):
        with assert_no_warnings():
            run["stringified"] = String(stringify_unsupported("Nothing to be worry about"))

        with assert_no_warnings():
            run["regular"] = String("Nothing to be worry about")

        assert run["regular"].fetch() == run["stringified"].fetch()

    def test_assign__string__reassign(self, run):
        with assert_no_warnings():
            run["stringified"] = String(stringify_unsupported("Nothing to be worry about"))
            run["stringified"] = String(stringify_unsupported("... or maybe"))

        with assert_no_warnings():
            run["regular"] = String("Nothing to be worry about")
            run["regular"] = String("... or maybe")

        assert run["regular"].fetch() == run["stringified"].fetch()

    def test_assign__string__custom_object(self, run):
        with raises(TypeError):
            run["unsupported"] = String(Obj())

        with assert_no_warnings():
            run["stringified"] = String(stringify_unsupported(Obj()))

        with assert_no_warnings():
            run["regular"] = String(str(Obj()))

        assert run["regular"].fetch() == run["stringified"].fetch()

    def test_assign__string__custom_object__reassign(self, run):
        with raises(TypeError):
            run["unsupported"] = String(Obj())
            run["unsupported"] = String(Obj(name="B"))

        with assert_no_warnings():
            run["stringified"] = String(stringify_unsupported(Obj()))
            run["stringified"] = String(stringify_unsupported(Obj(name="B")))

        with assert_no_warnings():
            run["regular"] = String(str(Obj()))
            run["regular"] = String(str(Obj(name="B")))

        assert run["regular"].fetch() == run["stringified"].fetch()

    def test_assign__string__float(self, run):
        with raises(TypeError):
            run["unsupported"] = String(4.0)

        with assert_no_warnings():
            run["stringified"] = String(stringify_unsupported(4.0))

        with assert_no_warnings():
            run["regular"] = str(4.0)

        assert run["regular"].fetch() == run["stringified"].fetch()

    def test_assign__string__float__reassign(self, run):
        with raises(TypeError):
            run["unsupported"] = String(4.0)
            run["unsupported"] = String(5.3)

        with assert_no_warnings():
            run["stringified"] = String(stringify_unsupported(4.0))
            run["stringified"] = String(stringify_unsupported(5.3))

        with assert_no_warnings():
            run["regular"] = String(str(4.0))
            run["regular"] = String(str(5.3))

        assert run["regular"].fetch() == run["stringified"].fetch()

    def test_assign__tuple(self, run):
        values = (Obj(), Obj(), Obj())

        with assert_unsupported_warning():
            run["unsupported"] = values

        with assert_no_warnings():
            run["stringified"] = stringify_unsupported(values)

        with assert_no_warnings():
            run["regular"] = str((Obj(), Obj(), Obj()))

        assert run["regular"].fetch() == run["stringified"].fetch()

    def test_assign__tuple_inside_dict(self, run):
        values = (Obj(), Obj(), Obj())

        with assert_unsupported_warning():
            run["unsupported"] = {"tuple": values}

        with assert_no_warnings():
            run["stringified"] = stringify_unsupported({"tuple": values})

        with assert_no_warnings():
            run["regular"] = {"tuple": str((Obj(), Obj(), Obj()))}

        assert run["regular"].fetch() == run["stringified"].fetch()

    def test_assign__dict(self, run):
        with assert_unsupported_warning():
            run["unsupported"] = {"a": Obj(), "b": "Test", "c": 25, "d": 1997, "e": {"f": Boolean(True)}}

        with assert_no_warnings():
            run["stringified"] = stringify_unsupported(
                {"a": Obj(), "b": "Test", "c": 25, "d": 1997, "e": {"f": Boolean(True)}}
            )

        with assert_no_warnings():
            run["regular"] = {"a": str(Obj()), "b": "Test", "c": 25, "d": 1997, "e": {"f": Boolean(True)}}

        assert run["regular"].fetch() == run["stringified"].fetch()

    def test_assign__dict__reassign(self, run):
        with assert_unsupported_warning():
            run["unsupported"] = {"a": Obj(), "b": "Test", "c": 25, "d": 1997, "e": {"f": Boolean(True)}}
            run["unsupported"] = {"a": Obj(name="B"), "d": 12, "e": {"f": Boolean(False)}}

        with assert_no_warnings():
            run["stringified"] = stringify_unsupported(
                {"a": Obj(), "b": "Test", "c": 25, "d": 1997, "e": {"f": Boolean(True)}}
            )
            run["stringified"] = stringify_unsupported({"a": Obj(name="B"), "d": 12, "e": {"f": Boolean(False)}})

        with assert_no_warnings():
            run["regular"] = {"a": str(Obj()), "b": "Test", "c": 25, "d": 1997, "e": {"f": Boolean(True)}}
            run["regular"] = {"a": str(Obj(name="B")), "d": 12, "e": {"f": Boolean(False)}}

        assert run["regular"].fetch() == run["stringified"].fetch()

    def test_assign__list(self, run):
        with assert_unsupported_error():
            run["unsupported"] = [Obj(), Obj(), Obj()]

        with assert_no_warnings():
            run["stringified"] = stringify_unsupported([Obj(), Obj(), Obj()])

        with assert_no_warnings():
            run["regular"] = str([Obj(), Obj(), Obj()])

        assert run["regular"].fetch() == run["stringified"].fetch()

    def test_assign__empty_list(self, run):
        with assert_unsupported_error():
            run["unsupported"] = []

        with assert_no_warnings():
            run["stringified"] = stringify_unsupported([])

        with assert_no_warnings():
            run["regular"] = str([])

        assert run["regular"].fetch() == run["stringified"].fetch()

    def test_assign__list__reassign(self, run):
        with assert_unsupported_error():
            run["unsupported"] = [Obj()]
            run["unsupported"] = [Obj(), Obj(), Obj()]

        with assert_no_warnings():
            run["stringified"] = stringify_unsupported([Obj()])
            run["stringified"] = stringify_unsupported([Obj(), Obj(), Obj()])

        with assert_no_warnings():
            run["regular"] = str([Obj()])
            run["regular"] = str([Obj(), Obj(), Obj()])

        assert run["regular"].fetch() == run["stringified"].fetch()

    def test_assign__empty_list__reassign(self, run):
        with assert_unsupported_error():
            run["unsupported"] = "string"
            run["unsupported"] = []

        with assert_no_warnings():
            run["stringified"] = stringify_unsupported([Obj(), Obj(), Obj()])
            run["stringified"] = stringify_unsupported([])

        with assert_no_warnings():
            run["regular"] = str([Obj(), Obj(), Obj()])
            run["regular"] = str([])

        assert run["regular"].fetch() == run["stringified"].fetch()

    def test_log__custom_object(self, run):
        with assert_unsupported_warning():
            run["unsupported"].log(Obj())

        with assert_no_warnings():
            run["stringified"].log(stringify_unsupported(Obj()))

        with assert_no_warnings():
            run["regular"].log(str(Obj()))

        assert run["regular"].fetch_values().equals(run["stringified"].fetch_values())

    def test_log__list_of_custom_objects(self, run):
        with assert_unsupported_warning():
            run["unsupported"].log([Obj(), Obj(), Obj(), Obj(), Obj()])
            run["unsupported"].log(Obj())
            run["unsupported"].log([Obj(), Obj(), Obj(), Obj(), Obj()])

        with assert_no_warnings():
            run["stringified"].log(stringify_unsupported([Obj(), Obj(), Obj(), Obj(), Obj()]))
            run["stringified"].log(stringify_unsupported(Obj()))
            run["stringified"].log(stringify_unsupported([Obj(), Obj(), Obj(), Obj(), Obj()]))

        with assert_no_warnings():
            run["regular"].log([str(Obj()), str(Obj()), str(Obj()), str(Obj()), str(Obj())])
            run["regular"].log(str(Obj()))
            run["regular"].log([str(Obj()), str(Obj()), str(Obj()), str(Obj()), str(Obj())])

        assert run["regular"].fetch_values().equals(run["stringified"].fetch_values())

    def test_log__float(self, run):
        with assert_no_warnings():
            run["stringified"].log(stringify_unsupported([1.0, 2.0, 3.0, 4.0, 5.0]))

        with assert_no_warnings():
            run["regular"].log([1.0, 2.0, 3.0, 4.0, 5.0])

        assert run["regular"].fetch_values().equals(run["stringified"].fetch_values())

    def test_extend__float(self, run):
        with assert_no_warnings():
            run["stringified"].extend(stringify_unsupported([1.0, 2.0, 3.0, 4.0, 5.0]))

        with assert_no_warnings():
            run["regular"].extend([1.0, 2.0, 3.0, 4.0, 5.0])

        assert run["regular"].fetch_values().equals(run["stringified"].fetch_values())

    def test_extend__dict(self, run):
        with assert_unsupported_warning():
            run["unsupported"].extend({"zz": [1.0, 2.0, 3.0, 4.0, 5.0], "bb": [Obj(), Obj(), Obj(), Obj(), Obj()]})

        with assert_no_warnings():
            run["stringified"].extend(
                stringify_unsupported({"zz": [1.0, 2.0, 3.0, 4.0, 5.0], "bb": [Obj(), Obj(), Obj(), Obj(), Obj()]})
            )

        with assert_no_warnings():
            run["regular"].extend(
                {"zz": [1.0, 2.0, 3.0, 4.0, 5.0], "bb": [str(Obj()), str(Obj()), str(Obj()), str(Obj()), str(Obj())]}
            )

        assert run["regular/zz"].fetch_values().equals(run["stringified/zz"].fetch_values())
        assert run["regular/bb"].fetch_values().equals(run["stringified/bb"].fetch_values())

    def test_append__float(self, run):
        with assert_no_warnings():
            run["stringified"].append(stringify_unsupported(1.0))
            run["stringified"].append(stringify_unsupported(2.0))

        with assert_no_warnings():
            run["regular"].append(1.0)
            run["regular"].append(2.0)

        assert run["regular"].fetch_values().equals(run["stringified"].fetch_values())

    def test_append__custom_object(self, run):
        with assert_unsupported_warning():
            run["unsupported"].append(Obj())
            run["unsupported"].append(Obj())

        with assert_no_warnings():
            run["stringified"].append(stringify_unsupported(Obj()))
            run["stringified"].append(stringify_unsupported(Obj()))

        with assert_no_warnings():
            run["regular"].append(str(Obj()))
            run["regular"].append(str(Obj()))

        assert run["regular"].fetch_values().equals(run["stringified"].fetch_values())

    def test_append__dict(self, run):
        with assert_no_warnings():
            run["stringified"].append(stringify_unsupported({"zz": 1.0}))
            run["stringified"].append(stringify_unsupported({"zz": 2.0, "bb": 3.0}))

        with assert_no_warnings():
            run["regular"].append({"zz": 1.0})
            run["regular"].append({"zz": 2.0, "bb": 3.0})

        assert run["regular/zz"].fetch_values().equals(run["stringified/zz"].fetch_values())
        assert run["regular/bb"].fetch_values().equals(run["stringified/bb"].fetch_values())
