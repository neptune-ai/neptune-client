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
import unittest
from unittest.mock import MagicMock

from neptune.new.attributes import create_attribute_from_type
from neptune.new.attributes.attribute import Attribute
from neptune.new.internal.backends.api_model import AttributeType


class TestAttributeUtils(unittest.TestCase):
    def test_attribute_type_to_atom(self):
        # Expect all AttributeTypes are reflected in `attribute_type_to_atom`...
        # ... and this reflection is class based on `Attribute`
        self.assertTrue(all(isinstance(create_attribute_from_type(attr_type, MagicMock(), ""), Attribute)
                            for attr_type in AttributeType))
