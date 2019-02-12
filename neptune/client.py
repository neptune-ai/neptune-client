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

from io import StringIO

from bravado.client import SwaggerClient
from bravado.requests_client import RequestsClient
from bravado_core.formatter import SwaggerFormat

from neptune.model import LeaderboardEntry
from neptune.oauth import NeptuneAuthenticator


class Client(object):
    def __init__(self, api_address, api_token):
        self.api_address = api_address
        self.api_token = api_token

        http_client = RequestsClient()

        self.backend_swagger_client = SwaggerClient.from_url(
            '{}/api/backend/swagger.json'.format(self.api_address),
            config=dict(
                validate_swagger_spec=False,
                formats=[uuid_format]),
            http_client=http_client)

        self.leaderboard_swagger_client = SwaggerClient.from_url(
            '{}/api/leaderboard/swagger.json'.format(self.api_address),
            config=dict(
                validate_swagger_spec=False,
                validate_responses=False,  # TODO!!!
                formats=[uuid_format]),
            http_client=http_client)

        http_client.authenticator = NeptuneAuthenticator(
            self.backend_swagger_client.api.exchangeApiToken(X_Neptune_Api_Token=api_token).response().result)

    def get_project(self, organization_name, project_name):
        r = self.backend_swagger_client.api.getProjectByName(
            organizationName=organization_name,
            projectName=project_name
        ).response()

        return r.result

    def get_projects(self, namespace):
        r = self.backend_swagger_client.api.listProjectsInOrganization(
            organizationName=namespace
        ).response()
        return r.result.entries

    def get_project_members(self, project_identifier):
        r = self.backend_swagger_client.api.listProjectMembers(
            projectIdentifier=project_identifier
        ).response()

        return r.result

    def get_leaderboard_entries(self, namespace, project_name,
                                entry_types=None, ids=None, group_ids=None,
                                states=None, owners=None, tags=None,
                                min_running_time=None):
        if entry_types is None:
            entry_types = ['experiment', 'notebook']

        def get_portion(limit, offset):
            return self.leaderboard_swagger_client.api.getLeaderboard(
                projectIdentifier="{}/{}".format(namespace, project_name),
                entryType=entry_types,
                shortId=ids, groupShortId=group_ids, state=states, owner=owners, tags=tags,
                minRunningTimeSeconds=min_running_time,
                sortBy=['shortId'], sortFieldType=['native'], sortDirection=['ascending'],
                limit=limit, offset=offset
            ).response().result.entries

        return [LeaderboardEntry(e) for e in self._get_all_items(get_portion, step=100)]

    def get_channel_points_csv(self, experiment_internal_id, channel_internal_id):
        csv = StringIO()
        csv.write(
            self.backend_swagger_client.api.getChannelValuesCSV(
                experimentId=experiment_internal_id, channelId=channel_internal_id
            ).response().incoming_response.text
        )
        csv.seek(0)
        return csv

    def get_metrics_csv(self, experiment_internal_id):
        csv = StringIO()
        csv.write(
            self.backend_swagger_client.api.getSystemMetricsCSV(
                experimentId=experiment_internal_id
            ).response().incoming_response.text
        )
        csv.seek(0)
        return csv

    @staticmethod
    def _get_all_items(get_portion, step):
        items = []

        previous_items = None
        while previous_items is None or len(previous_items) >= step:
            previous_items = get_portion(limit=step, offset=len(items))
            items += previous_items

        return items


uuid_format = SwaggerFormat(
    format='uuid',
    to_python=lambda x: x,
    to_wire=lambda x: x,
    validate=lambda x: None,
    description=''
)
