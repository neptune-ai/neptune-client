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

"""Remember to set environment values:
* NEPTUNE_API_TOKEN
* NEPTUNE_PROJECT
"""
import sys
from datetime import datetime

from requests import HTTPError

import neptune
from alpha_integration_dev.common_client_code import ClientFeatures
from neptune.exceptions import (
    DeleteArtifactUnsupportedInAlphaException,
    DownloadArtifactUnsupportedException,
    DownloadArtifactsUnsupportedException,
    DownloadSourcesException,
    FileNotFound,
)
from neptune.internal.api_clients.hosted_api_clients.hosted_alpha_leaderboard_api_client \
    import HostedAlphaLeaderboardApiClient


def get_api_version(exp):
    backend = exp._backend
    if isinstance(backend, HostedAlphaLeaderboardApiClient):
        return 2
    return 1


class OldClientFeatures(ClientFeatures):
    def __init__(self):
        super().__init__()
        neptune.init()
        neptune.create_experiment(
            name='const project name',
            description='exp description',
            params=self.params,
            properties=self.properties,
            tags=['initial tag 1', 'initial tag 2'],
            abort_callback=None,
            run_monitoring_thread=False,
            hostname='hostname value',
            # notebook_id='test1',  # TODO: Error 500 when wrong value
            upload_source_files='alpha_integration_dev/*.py',
        )

        exp = neptune.get_experiment()

        self._api_version = get_api_version(exp)

        properties = exp.get_properties()
        assert properties['init_text_property'] == 'some text'
        assert properties['init_number property'] == '42'
        assert properties['init_list'] == '[1, 2, 3]'

        assert set(exp.get_tags()) == {'initial tag 1', 'initial tag 2'}

        # download sources
        if self._api_version == 1:
            # old domain

            with self.with_check_if_file_appears('old_client.py.zip'):
                exp.download_sources('alpha_integration_dev/old_client.py')
            with self.with_check_if_file_appears('alpha_integration_dev.zip'):
                exp.download_sources('alpha_integration_dev')

            with self.with_assert_raises(FileNotFound):
                exp.download_sources('non_existing')
        else:
            # new api

            with self.with_check_if_file_appears('files.zip'):
                exp.download_sources()
            with self.with_assert_raises(DownloadSourcesException):
                exp.download_sources('whatever')
            with self.with_check_if_file_appears('file_set_sources/files.zip'):
                exp.download_sources(destination_dir='file_set_sources')

    def modify_tags(self):
        neptune.append_tags('tag1')
        neptune.append_tag(['tag2_to_remove', 'tag3'])
        neptune.remove_tag('tag2_to_remove')
        neptune.remove_tag('tag4_remove_non_existing')

        exp = neptune.get_experiment()
        assert set(exp.get_tags()) == {'initial tag 1', 'initial tag 2', 'tag1', 'tag3'}

    def modify_properties(self):
        neptune.set_property('prop', 'some text')
        neptune.set_property('prop_number', 42)
        neptune.set_property('nested/prop', 42)
        neptune.set_property('prop_to_del', 42)
        neptune.set_property('prop_list', [1, 2, 3])
        with open(self.text_file_path, mode='r') as f:
            neptune.set_property('prop_IO', f)
        neptune.set_property('prop_datetime', datetime.now())
        neptune.remove_property('prop_to_del')

        exp = neptune.get_experiment()
        properties = exp.get_properties()
        assert properties['prop'] == 'some text'
        assert properties['prop_number'] == '42'
        assert properties['nested/prop'] == '42'
        assert properties['prop_list'] == '[1, 2, 3]'
        assert 'prop_to_del' not in properties
        assert properties['prop_IO'] == "<_io.TextIOWrapper name='alpha_integration_dev/data/text.txt'" \
                                        " mode='r' encoding='UTF-8'>"
        print(f'Properties: {properties}')

    def log_std(self):
        print('stdout text1')
        print('stdout text2')
        print('stderr text1', file=sys.stderr)
        print('stderr text2', file=sys.stderr)

    def log_series(self):
        # floats
        neptune.log_metric('m1', 1)
        neptune.log_metric('m1', 2)
        neptune.log_metric('m1', 3)
        neptune.log_metric('m1', 2)
        neptune.log_metric('nested/m1', 1)

        # texts
        neptune.log_text('m2', 'a')
        neptune.log_text('m2', 'b')
        neptune.log_text('m2', 'c')

        # images
        # `image_name` and `description` will be lost
        neptune.log_image('g_img', self.img_path, image_name='name', description='desc')
        neptune.log_image('g_img', self.img_path)

        # see what we've logged
        logs = neptune.get_experiment().get_logs()
        print(f'Logs: {logs}')

    def handle_files_and_images(self):
        # image
        # `image_name` and `description` will be lost (`send_image` the same as `log_image`)
        neptune.send_image('image', self.img_path, name='name', description='desc')

        # artifact with default dest
        neptune.send_artifact(self.text_file_path)
        exp = neptune.get_experiment()
        with self.with_check_if_file_appears('text.txt'):
            exp.download_artifact('text.txt')
        with self.with_check_if_file_appears('custom_dest/text.txt'):
            exp.download_artifact('text.txt', 'custom_dest')

        # artifact with custom dest
        neptune.send_artifact(self.text_file_path, destination='something.txt')
        exp = neptune.get_experiment()
        with self.with_check_if_file_appears('something.txt'):
            exp.download_artifact('something.txt')
        with self.with_check_if_file_appears('custom_dest/something.txt'):
            exp.download_artifact('something.txt', 'custom_dest')

        # destination dirs
        neptune.log_artifact(self.text_file_path, destination='dir/text file artifact')
        neptune.log_artifact(self.text_file_path, destination='dir/artifact_to_delete')

        # deleting
        neptune.delete_artifacts('dir/artifact_to_delete')

        # streams
        with open(self.text_file_path, mode='r') as f:
            neptune.send_artifact(f, destination='file stream.txt')

    def handle_directories(self):
        exp = neptune.get_experiment()

        # download_artifacts
        neptune.send_artifact(self.data_dir)
        if self._api_version == 1:
            with self.with_check_if_file_appears('output.zip'):
                exp.download_artifacts()
        else:
            with self.with_assert_raises(DownloadArtifactsUnsupportedException):
                exp.download_artifacts()

        # create some nested artifacts
        neptune.log_artifact(self.img_path, destination='main dir/sub dir/art1')
        neptune.log_artifact(self.img_path, destination='main dir/sub dir/art2')
        neptune.log_artifact(self.img_path, destination='main dir/sub dir/art3')

        # downloading artifact - download_artifact
        # non existing artifact
        if self._api_version == 1:
            with self.with_assert_raises(FileNotFound):
                exp.download_artifact('main dir/sub dir/art100')
        else:
            with self.with_assert_raises(DownloadArtifactUnsupportedException):
                exp.download_artifact('main dir/sub dir/art100')
        # artifact directories
        if self._api_version == 1:
            with self.with_assert_raises(HTTPError):
                exp.download_artifact('main dir/sub dir')
        else:
            with self.with_assert_raises(DownloadArtifactUnsupportedException):
                exp.download_artifact('main dir/sub dir')

        # deleting artifacts
        neptune.delete_artifacts('main dir/sub dir/art1')

        # delete non existing artifact
        if self._api_version == 1:
            neptune.delete_artifacts('main dir/sub dir/art100')
        else:
            with self.with_assert_raises(DeleteArtifactUnsupportedInAlphaException):
                neptune.delete_artifacts('main dir/sub dir/art100')

        # delete dir
        if self._api_version == 1:
            neptune.delete_artifacts('main dir/sub dir')
        else:
            with self.with_assert_raises(DeleteArtifactUnsupportedInAlphaException):
                neptune.delete_artifacts('main dir/sub dir')

    def finalize(self):
        pass


if __name__ == '__main__':
    OldClientFeatures().run()
