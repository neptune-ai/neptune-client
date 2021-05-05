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
        # self.exp[SYSTEM_TAGS_ATTRIBUTE_PATH].pop('tag2_to_remove')
        # self.exp[SYSTEM_TAGS_ATTRIBUTE_PATH].pop('tag4_remove_non_existing')
* NEPTUNE_API_TOKEN
* NEPTUNE_PROJECT
"""
import sys
from datetime import datetime

from PIL import Image

import neptune.new as neptune
from alpha_integration_dev.common_client_code import ClientFeatures
from neptune.new.attributes.constants import (
    ARTIFACT_ATTRIBUTE_SPACE,
    LOG_ATTRIBUTE_SPACE,
    PROPERTIES_ATTRIBUTE_SPACE,
    SOURCE_CODE_FILES_ATTRIBUTE_PATH,
    SYSTEM_TAGS_ATTRIBUTE_PATH,
)
from neptune.new.exceptions import MissingFieldException
from neptune.new.types import File


class NewClientFeatures(ClientFeatures):
    def __init__(self):
        super().__init__()
        self.exp = neptune.init(
            source_files='alpha_integration_dev/*.py'
        )

        # download sources
        self.exp.sync()
        with self.with_check_if_file_appears('files.zip'):
            self.exp[SOURCE_CODE_FILES_ATTRIBUTE_PATH].download()

    def modify_tags(self):
        self.exp[SYSTEM_TAGS_ATTRIBUTE_PATH].add('tag1')
        self.exp[SYSTEM_TAGS_ATTRIBUTE_PATH].add(['tag2_to_remove', 'tag3'])
        self.exp[SYSTEM_TAGS_ATTRIBUTE_PATH].remove('tag2_to_remove')
        self.exp[SYSTEM_TAGS_ATTRIBUTE_PATH].remove('tag4_remove_non_existing')
        # del self.exp[SYSTEM_TAGS_ATTRIBUTE_PATH]  # TODO: NPT-9222

        self.exp.sync()
        assert self.exp[SYSTEM_TAGS_ATTRIBUTE_PATH].fetch() == {'tag1', 'tag3'}

    def modify_properties(self):
        self.exp[PROPERTIES_ATTRIBUTE_SPACE]['prop'] = 'some text'
        self.exp[PROPERTIES_ATTRIBUTE_SPACE]['prop_number'] = 42
        self.exp[PROPERTIES_ATTRIBUTE_SPACE]['nested/prop'] = 42
        self.exp[PROPERTIES_ATTRIBUTE_SPACE]['prop_to_del'] = 42
        self.exp[PROPERTIES_ATTRIBUTE_SPACE]['prop_list'] = [1, 2, 3]
        with open(self.text_file_path, mode='r') as f:
            self.exp[PROPERTIES_ATTRIBUTE_SPACE]['prop_IO'] = f
        self.exp[PROPERTIES_ATTRIBUTE_SPACE]['prop_datetime'] = datetime.now()
        self.exp.sync()
        del self.exp[PROPERTIES_ATTRIBUTE_SPACE]['prop_to_del']

        assert self.exp[PROPERTIES_ATTRIBUTE_SPACE]['prop'].fetch() == 'some text'
        assert self.exp[PROPERTIES_ATTRIBUTE_SPACE]['prop_number'].fetch() == 42
        assert self.exp[PROPERTIES_ATTRIBUTE_SPACE]['nested/prop'].fetch() == 42
        prop_to_del_absent = False
        try:
            self.exp[PROPERTIES_ATTRIBUTE_SPACE]['prop_to_del'].fetch()
        except MissingFieldException:
            prop_to_del_absent = True
        assert prop_to_del_absent

    def log_std(self):
        print('stdout text1')
        print('stdout text2')
        print('stderr text1', file=sys.stderr)
        print('stderr text2', file=sys.stderr)

    def log_series(self):
        # floats
        self.exp[LOG_ATTRIBUTE_SPACE]['m1'].log(1)
        self.exp[LOG_ATTRIBUTE_SPACE]['m1'].log(2)
        self.exp[LOG_ATTRIBUTE_SPACE]['m1'].log(3)
        self.exp[LOG_ATTRIBUTE_SPACE]['m1'].log(2)
        self.exp[LOG_ATTRIBUTE_SPACE]['nested']['m1'].log(1)

        # texts
        self.exp[LOG_ATTRIBUTE_SPACE]['m2'].log('a')
        self.exp[LOG_ATTRIBUTE_SPACE]['m2'].log('b')
        self.exp[LOG_ATTRIBUTE_SPACE]['m2'].log('c')

        # images
        im_frame = Image.open(self.img_path)
        g_img = File.as_image(im_frame)
        self.exp[LOG_ATTRIBUTE_SPACE]['g_img'].log(g_img)

    def handle_files_and_images(self):
        # image
        im_frame = Image.open(self.img_path)
        g_img = File.as_image(im_frame)
        self.exp[ARTIFACT_ATTRIBUTE_SPACE]['assigned image'] = g_img
        self.exp.wait()
        with self.with_check_if_file_appears('assigned image.png'):
            self.exp[ARTIFACT_ATTRIBUTE_SPACE]['assigned image'].download()
        with self.with_check_if_file_appears('custom_dest.png'):
            self.exp[ARTIFACT_ATTRIBUTE_SPACE]['assigned image'].download('custom_dest.png')

        self.exp[ARTIFACT_ATTRIBUTE_SPACE]['logged image'].log(g_img)

        with open(self.img_path, mode='r') as f:
            # self.exp[ARTIFACT_ATTRIBUTE_SPACE]['assigned image stream'] = f
            self.exp[ARTIFACT_ATTRIBUTE_SPACE]['logged image stream'].log(f)

        # artifact
        text_file = neptune.types.File(self.text_file_path)
        self.exp[ARTIFACT_ATTRIBUTE_SPACE]['assigned file'] = text_file
        # self.exp[ARTIFACT_ATTRIBUTE_SPACE]['logged file'].log(text_file)  # wrong type
        with open(self.text_file_path, mode='r') as f:
            self.exp[ARTIFACT_ATTRIBUTE_SPACE]['assigned file stream'] = f
            self.exp[ARTIFACT_ATTRIBUTE_SPACE]['logged file stream'].log(f)

    def handle_directories(self):
        pass

    def finalize(self):
        return


if __name__ == '__main__':
    NewClientFeatures().run()
