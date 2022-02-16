#
# Copyright (c) 2021, Neptune Labs Sp. z o.o.
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
import re

import pytest
import pytorch_lightning as pl

from e2e_tests.base import BaseE2ETest


@pytest.mark.integrations
class TestPytorchLightning(BaseE2ETest):
    def test_logging_values(self, pytorch_run):
        # correct integration version is logged
        logged_version = pytorch_run[
            "source_code/integrations/pytorch-lightning"
        ].fetch()
        assert logged_version == pl.__version__  # pylint: disable=E1101

        # epoch are logged in steps [1, 1, ...., 2, 2, ..., 3, 3 ...]
        logged_epochs = list(pytorch_run["custom_prefix/epoch"].fetch_values()["value"])
        assert sorted(logged_epochs) == logged_epochs
        assert set(logged_epochs) == {0, 1, 2}

        assert pytorch_run.exists("custom_prefix/valid/loss")
        assert len(pytorch_run["custom_prefix/valid/loss"].fetch_values()) == 3

    def test_saving_models(self, pytorch_run):
        best_model_path = pytorch_run["custom_prefix/model/best_model_path"].fetch()
        assert re.match(
            r".*my_model/checkpoints/epoch=.*-valid/loss=.*\.ckpt$",
            best_model_path,
        )
        assert pytorch_run["custom_prefix/model/best_model_score"].fetch() is not None

        # make sure that exactly `save_top_k` checkpoints
        # NOTE: when `max_epochs` is close to `save_top_k` there may be less than `save_top_k` saved models
        checkpoints = pytorch_run["custom_prefix/model/checkpoints"].fetch()
        assert all((checkpoint.startswith("epoch=") for checkpoint in checkpoints))
        assert len(checkpoints) == 2
