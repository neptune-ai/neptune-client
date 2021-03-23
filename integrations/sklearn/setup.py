import versioneer
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

    setup(
        name='neptune-sklearn',
        version=versioneer.get_version(),
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
        packages=['neptune_sklearn'],
        zip_safe=False
    )


if __name__ == "__main__":
    main()
