import os

from setuptools import find_packages, setup

import versioneer


def main():
    root_dir = os.path.dirname(__file__)

    with open(os.path.join(root_dir, 'requirements.txt')) as f:
        requirements = [r.strip() for r in f]
        setup(
            name='neptune-client',
            version=versioneer.get_version(),
            description='Neptune Client',
            author='neptune.ai',
            author_email='contact@neptune.ai',
            url='https://neptune.ai/',
            long_description='Neptune Client',
            license='Apache License 2.0',
            install_requires=requirements,
            packages=find_packages(),
            cmdclass=versioneer.get_cmdclass(),
            entry_points={
                'console_scripts': [
                    'neptune = neptune_cli.main:main',
                ],
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
