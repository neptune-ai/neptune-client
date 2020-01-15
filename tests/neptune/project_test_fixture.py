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

from tests.neptune.api_objects_factory import an_experiment_leaderboard_entry_dto

some_exp_entry_dto = an_experiment_leaderboard_entry_dto()

some_exp_entry_row = {
    'id': some_exp_entry_dto.shortId,
    'name': some_exp_entry_dto.name,
    'created': some_exp_entry_dto.timeOfCreation,
    'finished': some_exp_entry_dto.timeOfCompletion,
    'owner': some_exp_entry_dto.owner,
    'notes': some_exp_entry_dto.description,
    'running_time': some_exp_entry_dto.runningTime,
    'size': some_exp_entry_dto.size,
    'tags': some_exp_entry_dto.tags
}
some_exp_entry_row.update({'property_' + p.key: p.value for p in some_exp_entry_dto.properties})
some_exp_entry_row.update({'parameter_' + p.name: p.value for p in some_exp_entry_dto.parameters})
some_exp_entry_row.update({'channel_' + c.channelName: c.y for c in some_exp_entry_dto.channelsLastValues})
