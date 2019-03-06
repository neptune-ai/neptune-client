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
            description='Neptune Client',
            author='neptune.ml',
            author_email='contact@neptune.ml',
            url='https://neptune.ml/',
            long_description='Neptune Client',
            license='Apache License 2.0',
            install_requires=requirements,
            packages=find_packages(include=['neptune*']),
            cmdclass={
                'git_version': git_version.GitVersion,
            }
        )


if __name__ == "__main__":
    main()
