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

import os

from setuptools import find_packages, setup

import git_version


def version():
    try:
        with open('VERSION') as f:
            return f.readline().strip()
    except IOError:
        return '0.0.0'


def main():
    root_dir = os.path.dirname(__file__)

    with open(os.path.join(root_dir, 'requirements.txt')) as f:
        requirements = [r.strip() for r in f]
        setup(
            name='neptune-client',
            version=version(),
            url='https://github.com/neptune-ml/neptune-client',
            license='Apache License 2.0',
            author='neptune.ml',
            author_email='contact@neptune.ml',
            description='Neptune Client',
            long_description=__doc__,
            packages=find_packages(include=['neptune*', 'cli*']),
            platforms='any',
            install_requires=requirements,
            entry_points={
                'console_scripts': [
                    'neptune = cli.main:main',
                ],
            },
            cmdclass={
                'git_version': git_version.GitVersion,
            },
            classifiers=[
                # As from http://pypi.python.org/pypi?%3Aaction=list_classifiers
                # 'Development Status :: 1 - Planning',
                # 'Development Status :: 2 - Pre-Alpha',
                # 'Development Status :: 3 - Alpha',
                'Development Status :: 4 - Beta',
                # 'Development Status :: 5 - Production/Stable',
                # 'Development Status :: 6 - Mature',
                # 'Development Status :: 7 - Inactive',
                'Environment :: Console',
                'Intended Audience :: Developers',
                'License :: OSI Approved :: Apache Software License',
                'Operating System :: POSIX',
                'Operating System :: MacOS',
                'Operating System :: Unix',
                'Operating System :: Microsoft :: Windows',
                'Programming Language :: Python',
                'Programming Language :: Python :: 2',
                'Programming Language :: Python :: 3',
                'Topic :: Software Development :: Libraries :: Python Modules',
            ]
        )


if __name__ == "__main__":
    main()
