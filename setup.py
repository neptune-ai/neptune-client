import os

from setuptools import find_packages, setup

import versioneer


def main():
    root_dir = os.path.dirname(__file__)

    with open(os.path.join(root_dir, 'requirements.txt')) as f:
        requirements = [r.strip() for r in f]
        setup(
            name='neptune-client',
            python_requires='>=3.6.0',
            version=versioneer.get_version(),
            description='Neptune Client',
            author='neptune.ai',
            author_email='contact@neptune.ai',
            url='https://neptune.ai/',
            project_urls={
                'Tracker': 'https://github.com/neptune-ai/neptune-client/issues',
                'Source': 'https://github.com/neptune-ai/neptune-client',
                'Documentation': 'https://docs.neptune.ai/',
            },
            long_description='Neptune Client',
            license='Apache License 2.0',
            install_requires=requirements,
            packages=find_packages(),
            cmdclass=versioneer.get_cmdclass(),
            extras_require={
              "fastai": ["neptune-fastai"],
              "lightgbm": ["neptune-lightgbm"],
              "optuna": ["neptune-optuna"],
              "pytorch-lightning": ["neptune-pytorch-lightning"],
              "sacred": ["neptune-sacred"],
              "sklearn": ["neptune-sklearn"],
              "tensorflow-keras": ["neptune-tensorflow-keras"],
              "xgboost": ["neptune-xgboost"],
            },
            entry_points={
                'console_scripts': [
                    'neptune = neptune_cli.main:main',
                ],
            },
            classifiers=[
                # As from http://pypi.python.org/pypi?%3Aaction=list_classifiers
                'Development Status :: 5 - Production/Stable',
                'Environment :: Console',
                'Intended Audience :: Developers',
                'Intended Audience :: Science/Research',
                'License :: OSI Approved :: Apache Software License',
                'Natural Language :: English',
                'Operating System :: MacOS',
                'Operating System :: Microsoft :: Windows',
                'Operating System :: POSIX',
                'Operating System :: Unix',
                'Programming Language :: Python :: 3',
                'Programming Language :: Python :: 3.6',
                'Programming Language :: Python :: 3.7',
                'Programming Language :: Python :: 3.8',
                'Programming Language :: Python :: 3.9',
                'Topic :: Software Development :: Libraries :: Python Modules',
                'Programming Language :: Python :: Implementation :: CPython',
                'Topic :: Scientific/Engineering :: Artificial Intelligence',
            ],
            keywords=['MLOps', 'ML Experiment Tracking', 'ML Model Registry', 'ML Model Store', 'ML Metadata Store'],
        )


if __name__ == "__main__":
    main()
