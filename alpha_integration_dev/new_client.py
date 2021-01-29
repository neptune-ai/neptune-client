"""Remember to set environment values:
        # self.exp[SYSTEM_TAGS_ATTRIBUTE_PATH].pop('tag2_to_remove')
        # self.exp[SYSTEM_TAGS_ATTRIBUTE_PATH].pop('tag4_remove_non_existing')
* NEPTUNE_API_TOKEN
* NEPTUNE_PROJECT
"""
import sys

from PIL import Image

import neptune.alpha as neptune
from alpha_integration_dev.common_client_code import ClientFeatures
from neptune.alpha.attributes.constants import SYSTEM_TAGS_ATTRIBUTE_PATH, LOG_ATTRIBUTE_SPACE


class NewClientFeatures(ClientFeatures):
    def __init__(self):
        super().__init__()
        self.exp = neptune.init()

    def modify_tags(self):
        """NPT-9213"""
        self.exp[SYSTEM_TAGS_ATTRIBUTE_PATH].add('tag1')
        self.exp[SYSTEM_TAGS_ATTRIBUTE_PATH].add(['tag2_to_remove', 'tag3'])
        # self.exp[SYSTEM_TAGS_ATTRIBUTE_PATH].pop('tag2_to_remove')
        # self.exp[SYSTEM_TAGS_ATTRIBUTE_PATH].pop('tag4_remove_non_existing')

    def log_std(self):
        print('stdout text1')
        print('stdout text2')
        print('stderr text1', file=sys.stderr)
        print('stderr text2', file=sys.stderr)

    def log_series(self):
        # floats
        self.exp[f'{LOG_ATTRIBUTE_SPACE}m1'].log(1)
        self.exp[f'{LOG_ATTRIBUTE_SPACE}m1'].log(2)
        self.exp[f'{LOG_ATTRIBUTE_SPACE}m1'].log(3)
        self.exp[f'{LOG_ATTRIBUTE_SPACE}m1'].log(2)
        self.exp[f'{LOG_ATTRIBUTE_SPACE}nested/m1'].log(1)

        # texts
        self.exp[f'{LOG_ATTRIBUTE_SPACE}m2'].log('a')
        self.exp[f'{LOG_ATTRIBUTE_SPACE}m2'].log('b')
        self.exp[f'{LOG_ATTRIBUTE_SPACE}m2'].log('c')

        # images
        im_frame = Image.open(self.img_path)
        g_img = neptune.types.Image(im_frame)
        self.exp[f'{LOG_ATTRIBUTE_SPACE}g_img'].log(g_img)

    def handle_files_and_images(self):
        """NPT-9207"""
        return

    def other(self):
        return

    def run(self):
        self.modify_tags()
        self.log_std()
        self.log_series()
        self.handle_files_and_images()

        self.other()


if __name__ == '__main__':
    NewClientFeatures().run()
