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


class Metric(object):
    def __init__(self, name, description, resource_type, unit, min_value, max_value, gauges, internal_id=None):
        self.__internal_id = internal_id
        self.__name = name
        self.__description = description
        self.__resource_type = resource_type
        self.__unit = unit
        self.__min_value = min_value
        self.__max_value = max_value
        self.__gauges = gauges

    @property
    def internal_id(self):
        return self.__internal_id

    @internal_id.setter
    def internal_id(self, value):
        self.__internal_id = value

    @property
    def name(self):
        return self.__name

    @property
    def description(self):
        return self.__description

    @property
    def resource_type(self):
        return self.__resource_type

    @property
    def unit(self):
        return self.__unit

    @property
    def min_value(self):
        return self.__min_value

    @property
    def max_value(self):
        return self.__max_value

    @property
    def gauges(self):
        return self.__gauges

    def __repr__(self):
        return ('Metric(internal_id={}, name={}, description={}, resource_type={}, unit={}, min_value={}, '
                'max_value={}, gauges={})').format(self.internal_id, self.name, self.description, self.resource_type,
                                                   self.unit, self.min_value, self.max_value, self.gauges)

    def __eq__(self, other):
        return self.__class__ == other.__class__ and repr(self) == repr(other)


class MetricResourceType(object):
    CPU = u'CPU'
    RAM = u'MEMORY'
    GPU = u'GPU'
    GPU_RAM = u'GPU_MEMORY'
    OTHER = u'OTHER'
