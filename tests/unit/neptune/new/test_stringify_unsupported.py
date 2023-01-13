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

from neptune.common.deprecation import NeptuneDeprecationWarning
from neptune.new import init_run
from neptune.new.types import (
    Boolean,
    String,
)
from neptune.new.utils import stringify_unsupported


class Obj:
    def __str__(self):
        return "Object()"


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

    def test_assign__custom_object_direct_method(self, run):
        with assert_deprecation_warning():
            run["with_warning"].assign(Obj())

        with assert_no_warnings():
            run["no_warning"].assign(stringify_unsupported(Obj()))

        assert run["with_warning"].fetch() == run["no_warning"].fetch()

    def test_assign__float(self, run):
        with assert_deprecation_warning():
            run["with_warning"] = 4.0

        with assert_no_warnings():
            run["no_warning"] = stringify_unsupported(4.0)

        assert run["with_warning"].fetch() == run["no_warning"].fetch()

    def test_assign__float_direct_method(self, run):
        with assert_deprecation_warning():
            run["with_warning"].assign(5.3)

        with assert_no_warnings():
            run["no_warning"].assign(stringify_unsupported(5.3))

        assert run["with_warning"].fetch() == run["no_warning"].fetch()

    def test_assign__string_type_custom_object(self, run):
        with assert_deprecation_warning():
            run["with_warning"] = String(Obj())

        with assert_no_warnings():
            run["no_warning"] = String(stringify_unsupported(Obj()))

        assert run["with_warning"].fetch() == run["no_warning"].fetch()

    def test_assign__string_type_float(self, run):
        with assert_deprecation_warning():
            run["with_warning"] = String(4.0)

        with assert_no_warnings():
            run["no_warning"] = String(stringify_unsupported(4.0))

        assert run["with_warning"].fetch() == run["no_warning"].fetch()

    def test_assign__dict(self, run):
        with assert_deprecation_warning():
            run["with_warning"] = {"a": Obj(), "b": "Test", "c": 25, "d": 1997, "e": {"f": Boolean(True)}}

        with assert_no_warnings():
            run["no_warning"] = stringify_unsupported(
                {"a": Obj(), "b": "Test", "c": 25, "d": 1997, "e": {"f": Boolean(True)}}
            )

        assert run["with_warning"].fetch() == run["no_warning"].fetch()

    def test_log__custom_object_single(self, run):
        with assert_deprecation_warning():
            run["with_warning"].log(Obj())

        with assert_no_warnings():
            run["no_warning"].log(stringify_unsupported(Obj()))

        assert run["with_warning"].fetch_values().equals(run["no_warning"].fetch_values())

    def test_log__custom_object(self, run):
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

        assert run["with_warning"].fetch_values().equals(run["no_warning"].fetch_values())

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

        assert run["with_warning"].fetch_values().equals(run["no_warning"].fetch_values())
