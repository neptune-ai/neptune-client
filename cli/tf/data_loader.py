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
from __future__ import print_function

import os
import sys

import click


class TensorflowDataLoader(object):

    def __init__(self, project, path):
        self._project = project
        self._path = path

    @staticmethod
    def requirements_installed():
        # pylint:disable=unused-variable
        try:
            import tensorflow
            return True
        except ImportError:
            return False

    def run(self):
        import tensorflow as tf
        runs = os.listdir(self._path)
        if all(os.path.isdir(os.path.join(self._path, run)) for run in runs):  # multiple runs
            for run_dir in runs:
                try:
                    self._load_single_run(os.path.join(self._path, run_dir), tf)
                except Exception as e:
                    print("Cannot load run from directory '{}'. ".format(run_dir) + e.message, file=sys.stderr)
        else:  # single run
            self._load_single_run(self._path, tf)

    def _load_single_run(self, path, tf):
        click.echo("Loading {}...".format(path))
        with self._project.create_experiment(name=path,
                                             upload_source_files=[],
                                             abort_callback=lambda *args: None,
                                             upload_stdout=False,
                                             upload_stderr=False,
                                             send_hardware_metrics=False,
                                             run_monitoring_thread=False,
                                             handle_uncaught_exceptions=True) as exp:
            for root, _, run_files in os.walk(path):
                for run_file in run_files:
                    self._load_single_file(exp, os.path.join(root, run_file), tf)
            click.echo("{} was saved as {}".format(path, exp.id))

    @staticmethod
    def _load_single_file(exp, path, tf):
        for record in tf.train.summary_iterator(path):
            if hasattr(record, 'summary'):
                summary = record.summary
                if hasattr(summary, 'value'):
                    values = summary.value
                    for value in values:
                        if hasattr(value, 'tag') and hasattr(value, 'simple_value'):
                            exp.send_metric(
                                channel_name=value.tag,
                                x=record.step,
                                y=value.simple_value
                            )
