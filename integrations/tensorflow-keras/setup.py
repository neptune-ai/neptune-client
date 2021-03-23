import os

from neptune_tensorflow_keras._version import get_versions
from setuptools import setup


def main():
    with open('README.md') as readme_file:
        readme = readme_file.read()

    extras = {
    }

    all_deps = []
    for group_name in extras:
        all_deps += extras[group_name]
    extras['all'] = all_deps

    base_libs = ['neptune-client>=0.5.1', 'tensorflow']

    version = None
    if os.path.exists('PKG-INFO'):
        with open('PKG-INFO', 'r') as f:
            lines = f.readlines()
        for line in lines:
            if line.startswith('Version:'):
                version = line[8:].strip()
    else:
        version = get_versions()["version"]

    setup(
        name='neptune-tensorflow-keras',
        version=version,
        description='Neptune.ai tensorflow-keras integration library',
        author='neptune.ai',
        support='contact@neptune.ai',
        author_email='contact@neptune.ai',
        url="https://github.com/neptune-ai/neptune-client",
        long_description=readme,
        long_description_content_type="text/markdown",
        license='MIT License',
        install_requires=base_libs,
        extras_require=extras,
        packages=['neptune_tensorflow_keras', 'neptune_tensorflow_keras.impl'],
        zip_safe=False
    )


if __name__ == "__main__":
    main()
