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
import unittest
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import (
    datetime,
    timedelta,
)
from io import (
    BytesIO,
    StringIO,
)
from tempfile import TemporaryDirectory
from unittest.mock import patch

import PIL
from pytest import warns

from neptune import (
    ANONYMOUS_API_TOKEN,
    init_run,
)
from neptune.attributes.atoms.boolean import Boolean
from neptune.attributes.atoms.datetime import Datetime
from neptune.attributes.atoms.file import File
from neptune.attributes.atoms.float import Float
from neptune.attributes.atoms.integer import Integer
from neptune.attributes.atoms.string import String
from neptune.attributes.series import FileSeries
from neptune.attributes.sets.string_set import StringSet
from neptune.common.warnings import (
    NeptuneUnsupportedType,
    warned_once,
)
from neptune.envs import (
    API_TOKEN_ENV_NAME,
    PROJECT_ENV_NAME,
)
from neptune.exceptions import (
    FileNotFound,
    NeptuneUserApiInputException,
)
from neptune.types import File as FileVal
from neptune.types.atoms.artifact import Artifact
from neptune.types.atoms.datetime import Datetime as DatetimeVal
from neptune.types.atoms.float import Float as FloatVal
from neptune.types.atoms.string import String as StringVal
from neptune.types.namespace import Namespace as NamespaceVal
from neptune.types.series.file_series import FileSeries as FileSeriesVal
from neptune.types.series.float_series import FloatSeries as FloatSeriesVal
from neptune.types.series.string_series import StringSeries as StringSeriesVal
from neptune.types.sets.string_set import StringSet as StringSetVal
from tests.unit.neptune.new.utils.file_helpers import create_file


class Obj:
    pass


@contextmanager
def assert_unsupported_warning():
    warned_once.clear()
    with warns(NeptuneUnsupportedType):
        yield


class TestBaseAssign(unittest.TestCase):
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

            self.assertEqual(exp["some/num/val"].fetch(), 5.0)
            self.assertEqual(exp["some/str/val"].fetch(), "some text")
            self.assertEqual(
                exp["some/datetime/val"].fetch(),
                now.replace(microsecond=1000 * int(now.microsecond / 1000)),
            )
            self.assertIsInstance(exp.get_structure()["some"]["num"]["val"], Float)
            self.assertIsInstance(exp.get_structure()["some"]["str"]["val"], String)
            self.assertIsInstance(exp.get_structure()["some"]["datetime"]["val"], Datetime)

    def test_assign(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            now = datetime.now()
            exp["some/num/val"].assign(5.0)
            exp["some/int/val"].assign(50)
            exp["some/str/val"].assign("some text", wait=True)
            exp["some/bool/val"].assign(True)
            exp["some/datetime/val"].assign(now)
            self.assertEqual(exp["some/num/val"].fetch(), 5.0)
            self.assertEqual(exp["some/int/val"].fetch(), 50)
            self.assertEqual(exp["some/str/val"].fetch(), "some text")
            self.assertEqual(exp["some/bool/val"].fetch(), True)
            self.assertEqual(
                exp["some/datetime/val"].fetch(),
                now.replace(microsecond=1000 * int(now.microsecond / 1000)),
            )
            self.assertIsInstance(exp.get_structure()["some"]["num"]["val"], Float)
            self.assertIsInstance(exp.get_structure()["some"]["int"]["val"], Integer)
            self.assertIsInstance(exp.get_structure()["some"]["str"]["val"], String)
            self.assertIsInstance(exp.get_structure()["some"]["bool"]["val"], Boolean)
            self.assertIsInstance(exp.get_structure()["some"]["datetime"]["val"], Datetime)

            now = now + timedelta(seconds=3, microseconds=500000)
            exp["some/num/val"].assign(FloatVal(15))
            exp["some/str/val"].assign(StringVal("other text"), wait=False)
            exp["some/datetime/val"].assign(DatetimeVal(now), wait=True)
            self.assertEqual(exp["some/num/val"].fetch(), 15)
            self.assertEqual(exp["some/str/val"].fetch(), "other text")
            self.assertEqual(
                exp["some/datetime/val"].fetch(),
                now.replace(microsecond=1000 * int(now.microsecond / 1000)),
            )
            self.assertIsInstance(exp.get_structure()["some"]["num"]["val"], Float)
            self.assertIsInstance(exp.get_structure()["some"]["str"]["val"], String)
            self.assertIsInstance(exp.get_structure()["some"]["datetime"]["val"], Datetime)

    def test_lookup(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            ns = exp["some/ns"]
            ns["val"] = 5
            exp.wait()
            self.assertEqual(exp["some/ns/val"].fetch(), 5)

            ns = exp["other/ns"]
            exp["other/ns/some/value"] = 3
            exp.wait()
            self.assertEqual(ns["some/value"].fetch(), 3)


class TestUpload(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        os.environ[PROJECT_ENV_NAME] = "organization/project"
        os.environ[API_TOKEN_ENV_NAME] = ANONYMOUS_API_TOKEN

    def test_save_download_text_stream_to_given_destination(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            data = "Some test content of the stream"

            exp["some/num/attr_name"] = FileVal.from_stream(StringIO(data))
            self.assertIsInstance(exp.get_structure()["some"]["num"]["attr_name"], File)

            with create_file() as temp_filename:
                exp["some/num/attr_name"].download(temp_filename)
                with open(temp_filename, "rt") as file:
                    self.assertEqual(file.read(), data)

    def test_save_download_binary_stream_to_default_destination(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            data = b"Some test content of the stream"

            exp["some/num/attr_name"] = FileVal.from_stream(BytesIO(data))
            self.assertIsInstance(exp.get_structure()["some"]["num"]["attr_name"], File)

            with TemporaryDirectory() as temp_dir:
                with patch("neptune.internal.backends.neptune_backend_mock.os.path.abspath") as abspath_mock:
                    abspath_mock.side_effect = lambda path: os.path.normpath(temp_dir + "/" + path)
                    exp["some/num/attr_name"].download()
                with open(temp_dir + "/attr_name.bin", "rb") as file:
                    self.assertEqual(file.read(), data)

    @patch(
        "neptune.internal.utils.glob",
        new=lambda path, recursive=False: [path.replace("*", "file.txt")],
    )
    @patch("neptune.internal.backends.neptune_backend_mock.ZipFile.write")
    def test_save_files_download(self, zip_write_mock):
        with init_run(mode="debug", flush_period=0.5) as exp:
            exp["some/artifacts"].upload_files("path/to/file.txt")
            exp["some/artifacts"].upload_files("path/to/other/*")

            with TemporaryDirectory() as temp_dir:
                exp["some/artifacts"].download(temp_dir)
                exp["some/artifacts"].download(temp_dir)

            zip_write_mock.assert_any_call(os.path.abspath("path/to/file.txt"), "path/to/file.txt")
            zip_write_mock.assert_any_call(os.path.abspath("path/to/other/file.txt"), "path/to/other/file.txt")


class TestSeries(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        os.environ[PROJECT_ENV_NAME] = "organization/project"
        os.environ[API_TOKEN_ENV_NAME] = ANONYMOUS_API_TOKEN

    def test_assign_series(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            exp["some/num/val"].assign(FloatSeriesVal([1, 2, 0, 10]))
            exp["some/str/val"].assign(StringSeriesVal(["text1", "text2"]), wait=True)
            exp["some/img/val"].assign(FileSeriesVal([FileVal.as_image(PIL.Image.new("RGB", (10, 15), color="red"))]))
            self.assertEqual(exp["some"]["num"]["val"].fetch_last(), 10)
            self.assertEqual(exp["some"]["str"]["val"].fetch_last(), "text2")
            self.assertIsInstance(exp.get_structure()["some"]["img"]["val"], FileSeries)

            exp["some/num/val"].assign(FloatSeriesVal([122, 543, 2, 5]))
            exp["some/str/val"].assign(StringSeriesVal(["other 1", "other 2", "other 3"]), wait=True)
            self.assertEqual(exp["some"]["num"]["val"].fetch_last(), 5)
            self.assertEqual(exp["some"]["str"]["val"].fetch_last(), "other 3")

    def test_log(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            exp["some/num/val"].log(5)
            exp["some/str/val"].log("some text")
            exp["some/img/val"].log(FileVal.as_image(PIL.Image.new("RGB", (60, 30), color="red")))
            exp["some/img/val"].log(PIL.Image.new("RGB", (60, 30), color="red"))
            self.assertEqual(exp["some"]["num"]["val"].fetch_last(), 5)
            self.assertEqual(exp["some"]["str"]["val"].fetch_last(), "some text")
            self.assertIsInstance(exp.get_structure()["some"]["img"]["val"], FileSeries)

    def test_log_dict(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            dict_value = str({"key-a": "value-a", "key-b": "value-b"})
            exp["some/num/val"].log(dict_value)
            self.assertEqual(exp["some"]["num"]["val"].fetch_last(), str(dict_value))

    def test_append(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            exp["some/num/val"].append(5)
            exp["some/str/val"].append("some text")
            exp["some/img/val"].append(FileVal.as_image(PIL.Image.new("RGB", (60, 30), color="red")))
            exp["some/img/val"].append(PIL.Image.new("RGB", (60, 30), color="red"))
            self.assertEqual(exp["some"]["num"]["val"].fetch_last(), 5)
            self.assertEqual(exp["some"]["str"]["val"].fetch_last(), "some text")
            self.assertIsInstance(exp.get_structure()["some"]["img"]["val"], FileSeries)

    def test_append_dict(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            dict_value = {"key-a": "value-a", "key-b": "value-b"}
            exp["some/num/val"].append(dict_value)
            self.assertEqual(exp["some"]["num"]["val"]["key-a"].fetch_last(), "value-a")
            self.assertEqual(exp["some"]["num"]["val"]["key-b"].fetch_last(), "value-b")

    def test_append_complex_input(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            exp["train/dictOfDicts"].append(
                {
                    "key-a": {"aa": 11, "ab": 22},
                    "key-b": {"ba": 33, "bb": 44},
                }
            )
            self.assertEqual(exp["train"]["dictOfDicts"]["key-a"]["aa"].fetch_last(), 11)
            self.assertEqual(exp["train"]["dictOfDicts"]["key-a"]["ab"].fetch_last(), 22)
            self.assertEqual(exp["train"]["dictOfDicts"]["key-b"]["ba"].fetch_last(), 33)
            self.assertEqual(exp["train"]["dictOfDicts"]["key-b"]["bb"].fetch_last(), 44)

    def test_log_many_values(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            exp["some/num/val"].log([5, 10, 15])
            exp["some/str/val"].log(["some text", "other"])
            exp["some/img/val"].log(
                [
                    FileVal.as_image(PIL.Image.new("RGB", (60, 30), color="red")),
                    FileVal.as_image(PIL.Image.new("RGB", (20, 90), color="red")),
                ]
            )
            self.assertEqual(exp["some"]["num"]["val"].fetch_last(), 15)
            self.assertEqual(exp["some"]["str"]["val"].fetch_last(), "other")
            self.assertIsInstance(exp.get_structure()["some"]["img"]["val"], FileSeries)

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

    def test_extend(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            exp["some/num/val"].extend([5, 7])
            exp["some/str/val"].extend(["some", "text"])
            exp["some/img/val"].extend(
                [
                    FileVal.as_image(PIL.Image.new("RGB", (60, 30), color="red")),
                    FileVal.as_image(PIL.Image.new("RGB", (20, 90), color="blue")),
                ]
            )
            self.assertEqual(exp["some"]["num"]["val"].fetch_last(), 7)
            self.assertEqual(exp["some"]["str"]["val"].fetch_last(), "text")
            self.assertIsInstance(exp.get_structure()["some"]["img"]["val"], FileSeries)

    def test_extend_dict(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            dict_value = {"key-a": ["value-a", "value-aa"], "key-b": ["value-b", "value-bb"], "key-c": ["ccc"]}
            exp["some/num/val"].extend(dict_value)
            self.assertEqual(exp["some"]["num"]["val"]["key-a"].fetch_last(), "value-aa")
            self.assertEqual(exp["some"]["num"]["val"]["key-b"].fetch_last(), "value-bb")
            self.assertEqual(exp["some"]["num"]["val"]["key-c"].fetch_last(), "ccc")

    def test_extend_nested(self):
        """We expect that we are able to log arbitrary tre structure"""
        with init_run(mode="debug", flush_period=0.5) as exp:
            exp["train/simple_dict"].extend({"list1": [1, 2, 3], "list2": [10, 20, 30]})
            exp["train/simple_dict"].extend(
                {
                    "list1": [4, 5, 6],
                }
            )
            self.assertEqual(exp["train"]["simple_dict"]["list1"].fetch_last(), 6)
            self.assertEqual(list(exp["train"]["simple_dict"]["list1"].fetch_values().value), [1, 2, 3, 4, 5, 6])
            self.assertEqual(exp["train"]["simple_dict"]["list2"].fetch_last(), 30)
            self.assertEqual(list(exp["train"]["simple_dict"]["list2"].fetch_values().value), [10, 20, 30])

            exp["train/different-depths"].extend(
                {"lvl1": {"lvl1.1": [1, 2, 3], "lvl1.2": {"lvl1.2.1": [1]}}, "lvl2": [10, 20]}
            )
            exp["train/different-depths/lvl1"].extend({"lvl1.2": {"lvl1.2.1": [2, 3]}})
            self.assertEqual(exp["train"]["different-depths"]["lvl1"]["lvl1.1"].fetch_last(), 3)
            self.assertEqual(list(exp["train"]["different-depths"]["lvl1"]["lvl1.1"].fetch_values().value), [1, 2, 3])
            self.assertEqual(exp["train"]["different-depths"]["lvl1"]["lvl1.2"]["lvl1.2.1"].fetch_last(), 3)
            self.assertEqual(
                list(exp["train"]["different-depths"]["lvl1"]["lvl1.2"]["lvl1.2.1"].fetch_values().value), [1, 2, 3]
            )
            self.assertEqual(exp["train"]["different-depths"]["lvl2"].fetch_last(), 20)
            self.assertEqual(list(exp["train"]["different-depths"]["lvl2"].fetch_values().value), [10, 20])

    def test_extend_nested_with_wrong_parameters(self):
        """We expect that we are able to log arbitrary tre structure"""
        with init_run(mode="debug", flush_period=0.5) as exp:
            with self.assertRaises(NeptuneUserApiInputException):
                # wrong number of steps
                exp["train/simple_dict"].extend(values={"list1": [1, 2, 3], "list2": [10, 20, 30]}, steps=[0, 1])

            with self.assertRaises(NeptuneUserApiInputException):
                # wrong number of timestamps
                exp["train/simple_dict"].extend(
                    values={"list1": [1, 2, 3], "list2": [10, 20, 30]}, timestamps=[time.time()] * 2
                )

    def test_log_value_errors(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            img = FileVal.as_image(PIL.Image.new("RGB", (60, 30), color="red"))

            with self.assertRaises(ValueError):
                exp["x"].log([])
            with self.assertRaises(ValueError):
                exp["x"].log([5, "str"])
            with self.assertRaises(ValueError):
                exp["x"].log([5, 10], step=10)

            exp["some/num/val"].log([5], step=1)
            exp["some/num/val"].log([])
            with self.assertRaises(ValueError):
                exp["some/num/val"].log("str")
            with self.assertRaises(TypeError):
                exp["some/num/val"].log(img)

            exp["some/str/val"].log(["str"], step=1)
            exp["some/str/val"].log([])

            exp["some/img/val"].log([img], step=1)
            exp["some/img/val"].log([])
            with self.assertRaises(TypeError):
                exp["some/img/val"].log(5)
            with self.assertRaises(FileNotFound):
                exp["some/img/val"].log("path")

            self.assertEqual(exp["some"]["num"]["val"].fetch_last(), 5)
            self.assertEqual(exp["some"]["str"]["val"].fetch_last(), "str")
            self.assertIsInstance(exp.get_structure()["some"]["img"]["val"], FileSeries)


class TestSet(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        os.environ[PROJECT_ENV_NAME] = "organization/project"
        os.environ[API_TOKEN_ENV_NAME] = ANONYMOUS_API_TOKEN

    def test_append_errors(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            img = FileVal.as_image(PIL.Image.new("RGB", (60, 30), color="red"))

            exp["some/num/val"].append(5, step=1)
            with self.assertRaises(ValueError):
                exp["some/num/val"].append("str")
            with self.assertRaises(TypeError):
                exp["some/num/val"].append(img)

            exp["some/str/val"].append("str", step=1)
            exp["some/img/val"].append(img, step=1)
            with self.assertRaises(TypeError):
                exp["some/img/val"].append(5)
            with self.assertRaises(FileNotFound):
                exp["some/img/val"].append("path")

            self.assertEqual(exp["some"]["num"]["val"].fetch_last(), 5)
            self.assertEqual(exp["some"]["str"]["val"].fetch_last(), "str")
            self.assertIsInstance(exp.get_structure()["some"]["img"]["val"], FileSeries)

    def test_extend_value_errors(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            with self.assertRaises(NeptuneUserApiInputException):
                exp["x"].extend(10, step=10)
            with self.assertRaises(ValueError):
                exp["x"].extend([5, "str"])

    def test_assign_set(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            exp["some/str/val"].assign(StringSetVal(["tag1", "tag2"]), wait=True)
            self.assertEqual(exp["some/str/val"].fetch(), {"tag1", "tag2"})
            self.assertIsInstance(exp.get_structure()["some"]["str"]["val"], StringSet)

            exp["some/str/val"].assign(StringSetVal(["other_1", "other_2", "other_3"]), wait=True)
            self.assertEqual(exp["some/str/val"].fetch(), {"other_1", "other_2", "other_3"})
            self.assertIsInstance(exp.get_structure()["some"]["str"]["val"], StringSet)

    def test_add(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            exp["some/str/val"].add(["some text", "something else"], wait=True)
            self.assertEqual(exp["some/str/val"].fetch(), {"some text", "something else"})

            exp["some/str/val"].add("one more", wait=True)
            self.assertEqual(exp["some/str/val"].fetch(), {"some text", "something else", "one more"})

            self.assertIsInstance(exp.get_structure()["some"]["str"]["val"], StringSet)


class TestNamespace(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        os.environ[PROJECT_ENV_NAME] = "organization/project"
        os.environ[API_TOKEN_ENV_NAME] = ANONYMOUS_API_TOKEN

    def test_assign_dict(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            exp["params"] = {
                "x": 5,
                "metadata": {"name": "Trol", "age": 376},
                "toys": StringSeriesVal(["cudgel", "hat"]),
                "nested": {"nested": {"deep_secret": FloatSeriesVal([13, 15])}},
            }
            self.assertEqual(exp["params/x"].fetch(), 5)
            self.assertEqual(exp["params/metadata/name"].fetch(), "Trol")
            self.assertEqual(exp["params/metadata/age"].fetch(), 376)
            self.assertEqual(exp["params/toys"].fetch_last(), "hat")
            self.assertEqual(exp["params/nested/nested/deep_secret"].fetch_last(), 15)

    def test_assign_empty_dict(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            exp["params"] = {}
            exp["params"] = {"foo": 5}
            self.assertEqual(exp["params/foo"].fetch(), 5)

    def test_argparse_namespace(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            exp["params"] = argparse.Namespace(
                foo="bar", baz=42, nested=argparse.Namespace(nested_attr=str([1, 2, 3]), num=55)
            )
            self.assertEqual(exp["params/foo"].fetch(), "bar")
            self.assertEqual(exp["params/baz"].fetch(), 42)
            self.assertEqual(exp["params/nested/nested_attr"].fetch(), "[1, 2, 3]")
            self.assertEqual(exp["params/nested/num"].fetch(), 55)

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
            self.assertEqual(exp["some/namespace/sub-namespace/val1"].fetch(), 1.0)
            self.assertEqual(exp["some/namespace/sub-namespace/val2"].fetch(), {"tag1", "tag2"})
            self.assertIsInstance(exp.get_structure()["some"]["namespace"]["sub-namespace"]["val1"], Float)
            self.assertIsInstance(exp.get_structure()["some"]["namespace"]["sub-namespace"]["val2"], StringSet)

            exp["some"].assign(NamespaceVal({"namespace/sub-namespace/val1": 2.0}))
            self.assertEqual(exp["some/namespace/sub-namespace/val1"].fetch(), 2.0)
            self.assertEqual(exp["some/namespace/sub-namespace/val2"].fetch(), {"tag1", "tag2"})
            self.assertIsInstance(exp.get_structure()["some"]["namespace"]["sub-namespace"]["val1"], Float)
            self.assertIsInstance(exp.get_structure()["some"]["namespace"]["sub-namespace"]["val2"], StringSet)

            with self.assertRaises(TypeError):
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
            exp["some/num/attr_name"] = FileVal.from_stream(BytesIO(b"Some stream"))

            params_dict = exp["params"].fetch()
            self.assertDictEqual(
                params_dict,
                {
                    "int": 1,
                    "float": 3.14,
                    "bool": True,
                    "datetime": now.replace(microsecond=1000 * int(now.microsecond / 1000)),
                    "sub-namespace": {
                        "int": 42,
                        "string": "Some text",
                    },
                },
            )

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
            self.assertDictEqual(
                params_dict,
                {
                    "int": 42,
                    "string": "Some text",
                },
            )


class TestDelete(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        os.environ[PROJECT_ENV_NAME] = "organization/project"
        os.environ[API_TOKEN_ENV_NAME] = ANONYMOUS_API_TOKEN

    def test_pop(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            exp["some/num/val"].assign(3, wait=True)
            self.assertIn("some", exp.get_structure())
            ns = exp["some"]
            ns.pop("num/val", wait=True)
            self.assertNotIn("some", exp.get_structure())

    def test_pop_self(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            exp["x"].assign(3, wait=True)
            self.assertIn("x", exp.get_structure())
            exp["x"].pop(wait=True)
            self.assertNotIn("x", exp.get_structure())

    def test_del(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            exp["some/num/val"].assign(3)
            self.assertIn("some", exp.get_structure())
            ns = exp["some"]
            del ns["num/val"]
            self.assertNotIn("some", exp.get_structure())


class TestArtifacts(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        os.environ[PROJECT_ENV_NAME] = "organization/project"
        os.environ[API_TOKEN_ENV_NAME] = ANONYMOUS_API_TOKEN

    def test_artifacts(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            exp["art1"].track_files("s3://path/to/tracking/file", destination="/some/destination")
            exp["art2"].track_files("s3://path/to/tracking/file2")
            self.assertEqual(
                exp["art1"].fetch(),
                Artifact(value="e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
            )
            self.assertEqual(
                exp["art2"].fetch(),
                Artifact(value="e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
            )


class TestOtherBehaviour(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        os.environ[PROJECT_ENV_NAME] = "organization/project"
        os.environ[API_TOKEN_ENV_NAME] = ANONYMOUS_API_TOKEN

    def test_assign_distinct_types(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            exp["some/str/val"].assign(FloatVal(1.0), wait=True)
            self.assertEqual(exp["some/str/val"].fetch(), 1.0)
            self.assertIsInstance(exp.get_structure()["some"]["str"]["val"], Float)

            with self.assertRaises(TypeError):
                exp["some/str/val"].assign(StringSetVal(["other_1", "other_2", "other_3"]), wait=True)

    def test_attribute_error(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            with self.assertRaises(AttributeError):
                exp["var"].something()

    def test_float_like_types(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            exp.define("attr1", self.FloatLike(5))
            self.assertEqual(exp["attr1"].fetch(), 5)
            exp["attr1"] = "234"
            self.assertEqual(exp["attr1"].fetch(), 234)
            with self.assertRaises(ValueError):
                exp["attr1"] = "234a"

            exp["attr2"].assign(self.FloatLike(34))
            self.assertEqual(exp["attr2"].fetch(), 34)
            exp["attr2"].assign("555")
            self.assertEqual(exp["attr2"].fetch(), 555)
            with self.assertRaises(ValueError):
                exp["attr2"].assign("string")

            exp["attr3"].log(self.FloatLike(34))
            self.assertEqual(exp["attr3"].fetch_last(), 34)
            exp["attr3"].log(["345", self.FloatLike(34), 4, 13.0])
            self.assertEqual(exp["attr3"].fetch_last(), 13)
            with self.assertRaises(ValueError):
                exp["attr3"].log([4, "234a"])

    def test_append_float_like_types(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            exp["attr"].append(self.FloatLike(34))
            self.assertEqual(exp["attr"].fetch_last(), 34)
            exp["attr"].append("345")
            exp["attr"].append(self.FloatLike(34))
            exp["attr"].append(4)
            exp["attr"].append(13.0)
            self.assertEqual(exp["attr"].fetch_last(), 13)
            with self.assertRaises(ValueError):
                exp["attr"].append(4)
                exp["attr"].append("234a")

    def test_extend_float_like_types(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            exp["attr"].extend([self.FloatLike(34)])
            self.assertEqual(exp["attr"].fetch_last(), 34)
            exp["attr"].extend(["345", self.FloatLike(34), 4, 13.0])
            self.assertEqual(exp["attr"].fetch_last(), 13)
            with self.assertRaises(ValueError):
                exp["attr"].extend([4, "234a"])

    def test_assign_dict(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            exp["params"] = {
                "x": 5,
                "metadata": {"name": "Trol", "age": 376},
                "toys": StringSeriesVal(["cudgel", "hat"]),
                "nested": {"nested": {"deep_secret": FloatSeriesVal([13, 15])}},
            }
            self.assertEqual(exp["params/x"].fetch(), 5)
            self.assertEqual(exp["params/metadata/name"].fetch(), "Trol")
            self.assertEqual(exp["params/metadata/age"].fetch(), 376)
            self.assertEqual(exp["params/toys"].fetch_last(), "hat")
            self.assertEqual(exp["params/nested/nested/deep_secret"].fetch_last(), 15)

    def test_convertable_to_dict(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            exp["params"] = argparse.Namespace(
                foo="bar", baz=42, nested=argparse.Namespace(nested_attr=str([1, 2, 3]), num=55)
            )
            self.assertEqual(exp["params/foo"].fetch(), "bar")
            self.assertEqual(exp["params/baz"].fetch(), 42)
            self.assertEqual(exp["params/nested/nested_attr"].fetch(), "[1, 2, 3]")
            self.assertEqual(exp["params/nested/num"].fetch(), 55)

    def test_representation(self):
        with init_run(mode="debug", flush_period=0.5) as exp:
            exp["params/int"] = 1
            exp["params/float"] = 3.14
            exp["params/bool"] = True
            exp["params/datetime"] = datetime.now()
            exp["params/sub-namespace/int"] = 42
            exp["params/sub-namespace/string"] = "Some text"

            self.assertEqual('<Namespace field at "params">', repr(exp["params"]))
            self.assertEqual('<Integer field at "params/int">', repr(exp["params/int"]))
            self.assertEqual('<Float field at "params/float">', repr(exp["params/float"]))
            self.assertEqual('<Boolean field at "params/bool">', repr(exp["params/bool"]))
            self.assertEqual('<Datetime field at "params/datetime">', repr(exp["params/datetime"]))
            self.assertEqual('<Unassigned field at "params/unassigned">', repr(exp["params/unassigned"]))

            sub_namespace = exp["params/sub-namespace"]
            self.assertEqual('<Integer field at "params/sub-namespace/int">', repr(sub_namespace["int"]))
            self.assertEqual(
                '<String field at "params/sub-namespace/string">',
                repr(sub_namespace["string"]),
            )
            self.assertEqual(
                '<Unassigned field at "params/sub-namespace/unassigned">',
                repr(sub_namespace["unassigned"]),
            )

    @dataclass
    class FloatLike:
        value: float

        def __float__(self):
            return float(self.value)
