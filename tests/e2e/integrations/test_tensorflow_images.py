#
# Copyright (c) 2024, Neptune Labs Sp. z o.o.
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
import numpy
import pytest
import tensorflow as tf
from PIL import Image

import neptune
from neptune.types import File


@pytest.mark.integrations
@pytest.mark.tensorflow
def test_tensorflow_image_logging():
    # given
    image_tensor = tf.random.uniform(shape=[200, 300, 3], dtype=tf.int32, maxval=5, minval=1)

    # when
    with neptune.Run() as run:
        run_id = run["sys/id"].fetch()
        run["test_image"] = File.as_image(image_tensor)

        run.sync()

    # then
    with neptune.Run(with_id=run_id) as run:
        run["test_image"].download()

    image_fetched = Image.open("test_image.png")

    assert numpy.array_equal(image_tensor.numpy(), numpy.array(image_fetched))
