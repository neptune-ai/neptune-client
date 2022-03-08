#
# Copyright (c) 2022, Neptune Labs Sp. z o.o.
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

from neptune.new.internal.backends.nql import (
    NQLAggregator,
    NQLAttributeOperator,
    NQLAttributeType,
    NQLQueryAttribute,
    NQLQueryAggregate,
)


class TestNQL(unittest.TestCase):
    def test_attributes(self):
        self.assertEqual(
            str(
                NQLQueryAttribute(
                    name="sys/owner",
                    type=NQLAttributeType.STRING,
                    operator=NQLAttributeOperator.EQUALS,
                    value="user1",
                )
            ),
            '(`sys/owner`:string = "user1")',
        )
        self.assertEqual(
            str(
                NQLQueryAttribute(
                    name="sys/tags",
                    type=NQLAttributeType.STRING_SET,
                    operator=NQLAttributeOperator.CONTAINS,
                    value="tag1",
                )
            ),
            '(`sys/tags`:stringSet CONTAINS "tag1")',
        )
        self.assertEqual(
            str(
                NQLQueryAttribute(
                    name="sys/state",
                    type=NQLAttributeType.EXPERIMENT_STATE,
                    operator=NQLAttributeOperator.EQUALS,
                    value="running",
                )
            ),
            '(`sys/state`:experimentState = "running")',
        )

    def test_multiple_attribute_values(self):
        self.assertEqual(
            str(
                NQLQueryAggregate(
                    items=[
                        NQLQueryAttribute(
                            name="sys/owner",
                            type=NQLAttributeType.STRING,
                            operator=NQLAttributeOperator.EQUALS,
                            value=user,
                        )
                        for user in ["user1", "user2"]
                    ],
                    aggregator=NQLAggregator.OR,
                )
            ),
            '((`sys/owner`:string = "user1") OR (`sys/owner`:string = "user2"))',
        )

    def test_multiple_queries(self):
        self.assertEqual(
            str(
                NQLQueryAggregate(
                    items=[
                        NQLQueryAggregate(
                            items=[
                                NQLQueryAttribute(
                                    name="sys/owner",
                                    type=NQLAttributeType.STRING,
                                    operator=NQLAttributeOperator.EQUALS,
                                    value=user,
                                )
                                for user in ["user1", "user2"]
                            ],
                            aggregator=NQLAggregator.OR,
                        ),
                        NQLQueryAggregate(
                            items=[
                                NQLQueryAttribute(
                                    name="sys/tags",
                                    type=NQLAttributeType.STRING_SET,
                                    operator=NQLAttributeOperator.CONTAINS,
                                    value=tag,
                                )
                                for tag in ["tag1", "tag2"]
                            ],
                            aggregator=NQLAggregator.OR,
                        ),
                    ],
                    aggregator=NQLAggregator.AND,
                )
            ),
            '(((`sys/owner`:string = "user1") OR (`sys/owner`:string = "user2")) AND '
            '((`sys/tags`:stringSet CONTAINS "tag1") OR (`sys/tags`:stringSet CONTAINS "tag2")))',
        )
