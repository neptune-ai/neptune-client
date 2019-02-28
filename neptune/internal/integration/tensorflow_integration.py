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

import io
import os
import time
import uuid

from PIL import Image
from future.builtins import object, str

from neptune.exceptions import LibraryNotInstalled, NeptuneException

_integrated_with_tensorflow = False


def integrate_with_tensorflow(experiment_getter):
    global _integrated_with_tensorflow  # pylint:disable=global-statement

    if _integrated_with_tensorflow:
        return
    _integrate_with_tensorflow(experiment_getter)
    _integrated_with_tensorflow = True


class TensorflowIntegrator(object):

    def __init__(self, experiment_getter=None):

        self._experiment_holder = experiment_getter
        self._summary_writer_to_graph_id = {}

    def add_summary(self, log_dir, summary, global_step=None):

        from tensorflow.core.framework import summary_pb2  # pylint:disable=import-error,no-name-in-module
        import tensorflow as tf

        if isinstance(summary, bytes):
            summ = summary_pb2.Summary()
            summ.ParseFromString(summary)
            summary = summ

        x = self._calculate_x_value(global_step)

        for value in summary.value:

            try:
                field = value.WhichOneof('value')

                if field == 'simple_value':
                    self._send_numeric_value(log_dir, value.tag, x, value.simple_value)
                elif field == 'image':
                    self._send_image(log_dir, value.tag, x, value.image.encoded_image_string)
                elif field == 'tensor' and value.tensor.dtype == tf.string:
                    self._send_text(log_dir, value.tag, x, ", ".join(value.tensor.string_val))
            except NeptuneException:
                pass

    def add_graph_def(self, graph_def, log_dir):
        writer = self.get_writer_name(log_dir)
        if writer in list(self._summary_writer_to_graph_id.keys()):
            graph_id = self._summary_writer_to_graph_id[writer]
        else:
            graph_id = str(uuid.uuid4())
            self._summary_writer_to_graph_id[writer] = graph_id
        try:
            self._experiment_holder().send_graph(graph_id, str(graph_def))
        except NeptuneException:
            pass

    def _send_numeric_value(self, log_dir, value_tag, x, simple_value):
        writer_name = self.get_writer_name(log_dir)
        self._experiment_holder().send_metric(channel_name='{}_{}'.format(writer_name, value_tag),
                                              x=x,
                                              y=simple_value)

    def _send_image(self, log_dir, image_tag, x, encoded_image):
        writer_name = self.get_writer_name(log_dir)
        image_desc = "({}. Step {})".format(image_tag, x)
        self._experiment_holder().send_image(channel_name='{}_{}'.format(writer_name, image_tag),
                                             x=x,
                                             y=Image.open(io.BytesIO(encoded_image)),
                                             name=image_desc,
                                             description=image_desc)

    def _send_text(self, log_dir, value_tag, x, text):
        writer_name = self.get_writer_name(log_dir)
        self._experiment_holder().send_text(channel_name='{}_{}'.format(writer_name, value_tag),
                                            x=x,
                                            y=text)

    @staticmethod
    def get_writer_name(log_dir):
        return os.path.basename(os.path.normpath(log_dir))

    @staticmethod
    def _calculate_x_value(global_step):
        if global_step is not None:
            return int(global_step)
        else:
            return time.time()


def _integrate_with_tensorflow(experiment_getter):
    try:
        import tensorflow
    except ImportError:
        raise LibraryNotInstalled('tensorflow')

    tensorflow_integrator = TensorflowIntegrator(experiment_getter=experiment_getter)

    # pylint: disable=no-member, protected-access, no-name-in-module, import-error

    _add_summary_method = tensorflow.summary.FileWriter.add_summary
    _add_graph_def_method = tensorflow.summary.FileWriter._add_graph_def

    def _neptune_add_summary(summary_writer, summary, global_step=None):
        tensorflow_integrator.add_summary(summary_writer.get_logdir(), summary, global_step)
        _add_summary_method(summary_writer.get_logdir(), summary, global_step=global_step)

    def _neptune_add_graph_def(summary_writer, graph_def, global_step=None):

        try:
            from tensorflow.tensorboard.backend import process_graph
        except ImportError:
            from tensorboard.backend import process_graph

        # Restricting graph only to UI relevant information.
        process_graph.prepare_graph_for_ui(graph_def)
        # pylint: disable=protected-access
        tensorflow_integrator.add_graph_def(graph_def, summary_writer.get_logdir())
        _add_graph_def_method(summary_writer, graph_def, global_step=global_step)

    tensorflow.summary.FileWriter.add_summary = _neptune_add_summary
    tensorflow.summary.FileWriter._add_graph_def = _neptune_add_graph_def

    return tensorflow_integrator
