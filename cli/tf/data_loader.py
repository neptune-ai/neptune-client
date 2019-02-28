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

import collections
import os
import re
import sys

import click

from neptune.internal.integration.tensorflow_integration import TensorflowIntegrator


class TensorflowDataLoader(object):
    _RECORD = collections.namedtuple('Record', 'summary step')

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
        for root, _, run_files in os.walk(self._path):
            for run_file in run_files:
                try:
                    self._load_single_run(os.path.join(root, run_file), tf)
                except Exception as e:
                    print("Cannot load run from file '{}'. ".format(run_file) + e.message, file=sys.stderr)

    def _load_single_run(self, path, tf):
        click.echo("Loading {}...".format(path))
        run_path = os.path.relpath(path, self._path)
        run_name = re.sub(r'[^0-9A-Za-z_\-]', '_', run_path)
        if not self._experiment_exists(run_name):
            with self._project.create_experiment(name=run_name,
                                                 properties={
                                                     'tf/run/name': run_name,
                                                     'tf/run/path': run_path
                                                 },
                                                 tags=[run_name],
                                                 upload_source_files=[],
                                                 abort_callback=lambda *args: None,
                                                 upload_stdout=False,
                                                 upload_stderr=False,
                                                 send_hardware_metrics=False,
                                                 run_monitoring_thread=False,
                                                 handle_uncaught_exceptions=True) as exp:
                tf_integrator = TensorflowIntegrator(lambda *args: exp)
                self._load_single_file(path, tf, tf_integrator)
                click.echo("{} was saved as {}".format(run_path, exp.id))
        else:
            click.echo("{} is already synced".format(run_path))

    def _experiment_exists(self, run_name):
        existing_experiments = self._project.get_experiments(tag=run_name)
        return any(exp.name == run_name and exp.state == 'succeeded' for exp in existing_experiments)

    @staticmethod
    def _load_single_file(path, tf, tf_integrator):
        for record in tf.train.summary_iterator(path):
            if record.graph_def != "":
                graph_def = tf.GraphDef()
                graph_def.ParseFromString(record.graph_def)
                tf_integrator.add_graph_def(graph_def, path)
            tf_integrator.add_summary(path, record.summary, record.step)
