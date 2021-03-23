#
# Copyright (c) 2021, self.experiment Labs Sp. z o.o.
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
* self.experiment_API_TOKEN
* self.experiment_PROJECT
"""
import os
import sys
from datetime import datetime

from neptune import Session, envs
from common_client_code import ClientFeatures


class OldClientNonglobalFeatures(ClientFeatures):
    def __init__(self):
        super().__init__()
        self.session = Session()
        self.project = self.session.get_project(os.getenv(envs.PROJECT_ENV_NAME))
        self.experiment = self.project.create_experiment(
            name='const project name',
            params=self.params,
            tags=['initial tag 1', 'initial tag 2'],
        )

    def modify_tags(self):
        self.experiment.append_tags('tag1')
        self.experiment.append_tag(['tag2_to_remove', 'tag3'])
        # self.experiment.remove_tag('tag2_to_remove')  # TODO: NPT-9222
        # self.experiment.remove_tag('tag4_remove_non_existing')  # TODO: NPT-9222

        exp = self.experiment
        assert set(exp.get_tags()) == {'initial tag 1', 'initial tag 2', 'tag1', 'tag2_to_remove', 'tag3'}

    def modify_properties(self):
        self.experiment.set_property('prop', 'some text')
        self.experiment.set_property('prop_number', 42)
        self.experiment.set_property('nested/prop', 42)
        self.experiment.set_property('prop_to_del', 42)
        self.experiment.set_property('prop_list', [1, 2, 3])
        with open(self.text_file_path, mode='r') as f:
            self.experiment.set_property('prop_IO', f)
        self.experiment.set_property('prop_datetime', datetime.now())
        self.experiment.remove_property('prop_to_del')

        properties = self.experiment.get_properties()
        assert properties['prop'] == 'some text'
        assert properties['prop_number'] == '42'
        assert properties['nested/prop'] == '42'
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
        self.experiment.log_metric('m1', 1)
        self.experiment.log_metric('m1', 2)
        self.experiment.log_metric('m1', 3)
        self.experiment.log_metric('m1', 2)
        self.experiment.log_metric('nested/m1', 1)

        # texts
        self.experiment.log_text('m2', 'a')
        self.experiment.log_text('m2', 'b')
        self.experiment.log_text('m2', 'c')

        # images
        # `image_name` and `description` will be lost
        self.experiment.log_image('g_img', self.img_path, image_name='name', description='desc')
        self.experiment.log_image('g_img', self.img_path)

        # see what we've logged
        logs = self.experiment.get_logs()
        print(f'Logs: {logs}')

    def handle_files_and_images(self):
        # image
        # `image_name` and `description` will be lost (`send_image` the same as `log_image`)
        self.experiment.send_image('image', self.img_path, name='name', description='desc')

        # artifact
        # (`log_artifact` the same as `log_artifact`)
        self.experiment.send_artifact(self.text_file_path)
        self.experiment.log_artifact(self.text_file_path, destination='dir/text file artifact')
        with open(self.text_file_path, mode='r') as f:
            self.experiment.send_artifact(f, destination='file stream.txt')
        self.experiment.log_artifact(self.img_path, destination='dir to delete/art1')
        self.experiment.log_artifact(self.img_path, destination='dir to delete/art2')
        # self.experiment.delete_artifacts('dir to delete')  # doesn't work for alpha NPT-9250
        self.experiment.delete_artifacts('dir to delete/art1')

    def finalize(self):
        pass


if __name__ == '__main__':
    OldClientNonglobalFeatures().run()
