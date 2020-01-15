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
from random import randint, uniform

from mock import MagicMock

from tests.neptune.random_utils import a_string, a_uuid_string, a_timestamp


def a_project():
    project = MagicMock()
    project.id = a_uuid_string()
    project.name = a_string()
    return project


def an_invited_project_member():
    invitation_info = MagicMock()
    invitation_info.id = a_uuid_string()
    invitation_info.email = a_string() + '@example.com'

    project_member = MagicMock()
    project_member.invitationInfo = invitation_info
    project_member.registeredMemberInfo = None
    project_member.role = 'member'

    return project_member


def a_registered_project_member(username=None):
    if username is None:
        username = a_string()

    registered_member_info = MagicMock()
    registered_member_info.avatarSource = 'default'
    registered_member_info.avatarUrl = ''
    registered_member_info.firstName = a_string()
    registered_member_info.lastName = a_string()
    registered_member_info.username = username

    project_member = MagicMock()
    project_member.invitationInfo = None
    project_member.registeredMemberInfo = registered_member_info
    project_member.role = 'manager'

    return project_member


def an_experiment_states(creating=None, waiting=None, initializing=None, running=None, cleaning=None, aborted=None,
                         crashed=None, failed=None, succeeded=None, not_responding=None):
    def random_state_count():
        return randint(1, 100)

    experiment_states = MagicMock()
    experiment_states.creating = creating or random_state_count()
    experiment_states.waiting = waiting or random_state_count()
    experiment_states.initializing = initializing or random_state_count()
    experiment_states.running = running or random_state_count()
    experiment_states.cleaning = cleaning or random_state_count()
    experiment_states.aborted = aborted or random_state_count()
    experiment_states.crashed = crashed or random_state_count()
    experiment_states.failed = failed or random_state_count()
    experiment_states.succeeded = succeeded or random_state_count()
    experiment_states.notResponding = not_responding or random_state_count()
    return experiment_states


def a_property():
    p = MagicMock()
    p.key = a_string()
    p.value = a_string()
    return p


def a_parameter():
    p = MagicMock()
    p.id = a_uuid_string()
    p.name = a_string()
    p.parameterType = 'double'
    p.value = str(uniform(-100, 100))
    return p


def a_channel_value():
    cv = MagicMock()
    cv.channelId = a_uuid_string()
    cv.channelName = a_string()
    cv.channelType = 'numeric'
    cv.x = uniform(1, 100)
    cv.y = str(uniform(1, 100))
    return cv


def an_experiment_leaderboard_entry_dto():
    entry = MagicMock()
    entry.entryType = 'experiment'
    entry.id = a_uuid_string()
    entry.shortId = a_string()
    entry.projectId = a_uuid_string()
    entry.state = 'succeeded'
    entry.experimentStates = an_experiment_states(succeeded=1)
    entry.responding = True
    entry.name = a_string()
    entry.organizationName = a_string()
    entry.projectName = a_string()
    entry.description = a_string()
    entry.timeOfCreation = a_timestamp()
    entry.timeOfCompletion = a_timestamp()
    entry.runningTime = randint(1, 1000)
    entry.owner = a_string()
    entry.size = randint(1, 1000)
    entry.tags = [a_string(), a_string()]
    entry.environment = a_string()
    entry.workerType = a_string()
    entry.hostname = a_string()
    entry.sourceSize = randint(1, 1000)
    entry.sourceMd5 = a_string()
    entry.commitId = a_string()
    entry.properties = [a_property(), a_property()]
    entry.parameters = [a_parameter(), a_parameter()]
    entry.channelsLastValues = [a_channel_value(), a_channel_value()]
    entry.trashed = False
    entry.deleted = False
    entry.isBestExperiment = False
    return entry
