import os

from neptune_sklearn._version import get_versions
from setuptools import setup


def main():
    with open('README.md') as readme_file:
        readme = readme_file.read()

    extras = {}

    all_deps = []
    for group_name in extras:
        all_deps += extras[group_name]
    extras['all'] = all_deps

    base_libs = ['neptune-client>=0.5.1', 'scikit-learn>=0.24.1', 'yellowbrick>=1.3', 'scikit-plot>=0.3.7']

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
        name='neptune-sklearn',
        version=version,
        description='Neptune.ai scikit-learn integration library',
        author='neptune.ai',
        support='contact@neptune.ai',
        author_email='contact@neptune.ai',
        url="https://github.com/neptune-ai/neptune-client",
        long_description=readme,
        long_description_content_type="text/markdown",
        license='MIT License',
        install_requires=base_libs,
        extras_require=extras,
        packages=['neptune_sklearn', 'neptune_sklearn.impl'],
        zip_safe=False
    )


if __name__ == "__main__":
    main()
