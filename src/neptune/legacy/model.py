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


class ChannelWithLastValue:
    def __init__(self, channel_with_value_dto):
        self.channel_with_value_dto = channel_with_value_dto

    @property
    def id(self):
        return self.channel_with_value_dto.channelId

    @property
    def name(self):
        return self.channel_with_value_dto.channelName

    @property
    def type(self):
        return self.channel_with_value_dto.channelType

    @property
    def x(self):
        return self.channel_with_value_dto.x

    @x.setter
    def x(self, x):
        self.channel_with_value_dto.x = x

    @property
    def trimmed_y(self):
        return self.y[:255] if self.type == "text" else self.y

    @property
    def y(self):
        return self.channel_with_value_dto.y

    @y.setter
    def y(self, y):
        self.channel_with_value_dto.y = y


class LeaderboardEntry(object):
    def __init__(self, project_leaderboard_entry_dto):
        self.project_leaderboard_entry_dto = project_leaderboard_entry_dto

    @property
    def id(self):
        return self.project_leaderboard_entry_dto.shortId

    @property
    def name(self):
        return self.project_leaderboard_entry_dto.name

    @property
    def state(self):
        return self.project_leaderboard_entry_dto.state

    @property
    def internal_id(self):
        return self.project_leaderboard_entry_dto.id

    @property
    def project_full_id(self):
        return "{org_name}/{project_name}".format(
            org_name=self.project_leaderboard_entry_dto.organizationName,
            project_name=self.project_leaderboard_entry_dto.projectName,
        )

    @property
    def system_properties(self):
        entry = self.project_leaderboard_entry_dto
        return {
            "id": entry.shortId,
            "name": entry.name,
            "created": entry.timeOfCreation,
            "finished": entry.timeOfCompletion,
            "running_time": entry.runningTime,
            "owner": entry.owner,
            "size": entry.size,
            "tags": entry.tags,
            "notes": entry.description,
        }

    @property
    def channels(self):
        return [ChannelWithLastValue(ch) for ch in self.project_leaderboard_entry_dto.channelsLastValues]

    def add_channel(self, channel):
        self.project_leaderboard_entry_dto.channelsLastValues.append(channel.channel_with_value_dto)

    @property
    def channels_dict_by_name(self):
        return dict((ch.name, ch) for ch in self.channels)

    @property
    def parameters(self):
        return dict((p.name, p.value) for p in self.project_leaderboard_entry_dto.parameters)

    @property
    def properties(self):
        return dict((p.key, p.value) for p in self.project_leaderboard_entry_dto.properties)

    @property
    def tags(self):
        return self.project_leaderboard_entry_dto.tags


class Point(object):
    def __init__(self, point_dto):
        self.point_dto = point_dto

    @property
    def x(self):
        return self.point_dto.x

    @property
    def numeric_y(self):
        return self.point_dto.y.numericValue


class Points(object):
    def __init__(self, point_dtos):
        self.point_dtos = point_dtos

    @property
    def xs(self):
        return [p.x for p in self.point_dtos]

    @property
    def numeric_ys(self):
        return [p.y.numericValue for p in self.point_dtos]
