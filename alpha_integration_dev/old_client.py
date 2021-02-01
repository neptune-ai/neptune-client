"""Remember to set environment values:
* NEPTUNE_API_TOKEN
* NEPTUNE_PROJECT
"""
import sys

import neptune
from common_client_code import ClientFeatures


class OldClientFeatures(ClientFeatures):
    def __init__(self):
        super().__init__()
        neptune.init()
        neptune.create_experiment(
            name='const project name',
            params=self.params,
            tags=['initial tag 1', 'initial tag 2'],
        )

    def modify_tags(self):
        """NPT-9213"""
        neptune.append_tag('tag1')
        neptune.append_tag(['tag2_to_remove', 'tag3'])
        neptune.remove_tag('tag2_to_remove')
        neptune.remove_tag('tag4_remove_non_existing')

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
        # neptune.log_image('g_img', self.img_path, image_name='name', description='desc')
        # neptune.log_image('g_img', self.img_path)

    def handle_files_and_images(self):
        """NPT-9207"""
        neptune.send_image('single_img', self.img_path, name='name', description='desc')
        neptune.send_image('single_img', self.img_path, name='name', description='desc')
        neptune.log_image('g_img', self.img_path, image_name='name', description='desc')
        neptune.send_artifact(self.img_path, destination='single artifact')
        neptune.log_artifact(self.img_path, destination='logged artifact')
        neptune.log_artifact(self.img_path, destination='artifact to delete')
        # neptune.delete_artifacts('artifact to delete')

    def other(self):
        v = neptune.get_experiment().get_logs()
        print(v)

    def run(self):
        # self.modify_tags()
        self.log_std()
        self.log_series()
        # self.handle_files_and_images()

        self.other()


if __name__ == '__main__':
    OldClientFeatures().run()
