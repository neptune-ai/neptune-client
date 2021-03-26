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

import neptune
from alpha_integration_dev.common_client_code import ClientFeatures


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
        properties = exp.get_properties()
        assert properties['init_text_property'] == 'some text'
        assert properties['init_number property'] == '42'
        assert properties['init_list'] == '[1, 2, 3]'

        assert set(exp.get_tags()) == {'initial tag 1', 'initial tag 2'}

    def modify_tags(self):
        neptune.append_tags('tag1')
        neptune.append_tag(['tag2_to_remove', 'tag3'])
        # neptune.remove_tag('tag2_to_remove')  # TODO: NPT-9222
        # neptune.remove_tag('tag4_remove_non_existing')  # TODO: NPT-9222

        exp = neptune.get_experiment()
        assert set(exp.get_tags()) == {'initial tag 1', 'initial tag 2', 'tag1', 'tag2_to_remove', 'tag3'}

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

        neptune.log_artifact(self.text_file_path, destination='dir/text file artifact')
        with open(self.text_file_path, mode='r') as f:
            neptune.send_artifact(f, destination='file stream.txt')
        neptune.log_artifact(self.img_path, destination='dir to delete/art1')
        neptune.log_artifact(self.img_path, destination='dir to delete/art2')
        # neptune.delete_artifacts('dir to delete')  # doesn't work for alpha NPT-9250
        neptune.delete_artifacts('dir to delete/art1')

    def finalize(self):
        pass


if __name__ == '__main__':
    OldClientFeatures().run()
