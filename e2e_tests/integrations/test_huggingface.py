#
# Copyright (c) 2022, Neptune Labs Sp. z o.o.
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
import numpy as np
import pandas as pd
import pytest
import torch
from faker import Faker
from transformers import Trainer, TrainingArguments
from transformers.integrations import NeptuneCallback
from transformers.utils import logging

from e2e_tests.base import BaseE2ETest
from e2e_tests.utils import catch_time, modified_environ
from neptune.new import get_project, init_run

EXPECTED_OVERWHELMING_FACTOR = 1.2


logging.set_verbosity_error()
fake = Faker()


class RegressionModel(torch.nn.Module):
    def __init__(self, initial_a=0, initial_b=0):
        super().__init__()

        self._initial_a = initial_a
        self._initial_b = initial_b

        # pylint: disable=no-member
        self.a = torch.nn.Parameter(torch.tensor(initial_a).float())
        self.b = torch.nn.Parameter(torch.tensor(initial_b).float())

    def forward(self, input_x, labels=None):
        y = input_x * self.a + self.b

        if labels is None:
            return (y,)

        loss = torch.nn.functional.mse_loss(y, labels)

        return loss, y

    @property
    def config(self):
        return pd.DataFrame([["a", self._initial_a], ["b", self._initial_a]])


class RegressionDataset:
    def __init__(self, a=2, b=3, length=64, seed=2501):
        np.random.seed(seed)

        self.label_names = ["labels"]
        self.length = length
        self.x = np.random.normal(size=(length,)).astype(np.float32)
        self.ys = [
            a * self.x + b + np.random.normal(scale=0.1, size=(length,)) for _ in self.label_names
        ]
        self.ys = [y.astype(np.float32) for y in self.ys]

    def __len__(self):
        return self.length

    def __getitem__(self, i):
        result = {name: y[i] for name, y in zip(self.label_names, self.ys)}
        result["input_x"] = self.x[i]
        return result


@pytest.mark.integrations
class TestHuggingFace(BaseE2ETest):
    @property
    def _trainer_default_attributes(self):
        model = RegressionModel(initial_a=2, initial_b=3)
        train_dataset = RegressionDataset(length=32)
        validation_dataset = RegressionDataset(length=16)

        train_args = TrainingArguments(
            "model", report_to=[], num_train_epochs=1000, learning_rate=0.5
        )

        return {
            "model": model,
            "args": train_args,
            "train_dataset": train_dataset,
            "eval_dataset": validation_dataset,
        }

    def test_every_train_should_create_new_run(self, environment):
        common_tag = fake.nic_handle()
        project = get_project(name=environment.project, api_token=environment.user_token)

        trainer = Trainer(
            **self._trainer_default_attributes,
            callbacks=[
                NeptuneCallback(
                    api_token=environment.user_token, project=environment.project, tags=[common_tag]
                )
            ],
        )

        expected_times = 5
        for _ in range(expected_times):
            trainer.train()

        assert len(project.fetch_runs_table(tag=common_tag).to_rows()) == expected_times

    def test_runtime_factor(self, environment):
        with catch_time() as standard:
            trainer = Trainer(**self._trainer_default_attributes)
            trainer.train()
            del trainer

        with catch_time() as with_neptune_callback:
            trainer = Trainer(
                **self._trainer_default_attributes,
                callbacks=[
                    NeptuneCallback(api_token=environment.user_token, project=environment.project)
                ],
            )
            trainer.train()
            del trainer

        assert with_neptune_callback() / standard() <= EXPECTED_OVERWHELMING_FACTOR

    def test_run_access_methods(self, environment):
        callback = NeptuneCallback(api_token=environment.user_token, project=environment.project)
        trainer = Trainer(**self._trainer_default_attributes, callbacks=[callback])

        assert callback.run.get_run_url() == NeptuneCallback.get_run(trainer).get_run_url()

    def test_initialization_with_run_provided(self, environment):
        run = init_run(project=environment.project, api_token=environment.user_token)
        callback = NeptuneCallback(run=run)
        trainer = Trainer(**self._trainer_default_attributes, callbacks=[callback])

        assert run.get_run_url() == NeptuneCallback.get_run(trainer).get_run_url()

    def test_run_reinitialization_failure(self, environment):
        run = init_run(project=environment.project, api_token=environment.user_token)

        with modified_environ("NEPTUNE_API_TOKEN", "NEPTUNE_PROJECT"):
            callback = NeptuneCallback(run=run)
            trainer = Trainer(**self._trainer_default_attributes, callbacks=[callback])

            trainer.train()

            # pylint: disable=no-member
            with pytest.raises(NeptuneCallback.MissingConfiguration):
                trainer.train()

    def test_run_access_without_callback_configured(self):
        trainer = Trainer(**self._trainer_default_attributes)

        with pytest.raises(Exception):
            NeptuneCallback.get_run(trainer)

    def test_log_parameters_indicator(self, environment):
        with init_run(project=environment.project, api_token=environment.user_token) as run:
            base_namespace = "custom/base/path"
            callback = NeptuneCallback(run=run, base_namespace=base_namespace)
            trainer = Trainer(**self._trainer_default_attributes, callbacks=[callback])
            trainer.train()
            run.sync()

            assert run.exists(f"{base_namespace}/trainer_parameters")
            assert run.exists(f"{base_namespace}/model_parameters")

        with init_run(project=environment.project, api_token=environment.user_token) as run:
            callback = NeptuneCallback(run=run, log_parameters=False)
            trainer = Trainer(**self._trainer_default_attributes, callbacks=[callback])
            trainer.train()
            run.sync()

            assert not run.exists("finetuning/trainer_parameters")
            assert not run.exists("finetuning/model_parameters")

    def test_log_with_custom_base_namespace(self, environment):
        with init_run(project=environment.project, api_token=environment.user_token) as run:
            base_namespace = "just/a/sample/path"
            callback = NeptuneCallback(run=run, base_namespace=base_namespace)
            trainer = Trainer(**self._trainer_default_attributes, callbacks=[callback])
            trainer.log({"metric1": 123, "another/metric": 0.2})
            trainer.train()
            trainer.log({"after_training_metric": 2501})
            run.sync()

            assert run.exists(f"{base_namespace}/train")
            assert run.exists(f"{base_namespace}/train/metric1")
            assert run.exists(f"{base_namespace}/train/another/metric")
            assert run.exists(f"{base_namespace}/train/after_training_metric")

            assert run[f"{base_namespace}/train/metric1"].fetch_last() == 123
            assert run[f"{base_namespace}/train/another/metric"].fetch_last() == 0.2
            assert run[f"{base_namespace}/train/after_training_metric"].fetch_last() == 2501

    def test_integration_version_is_logged(self, environment):
        with init_run(project=environment.project, api_token=environment.user_token) as run:
            callback = NeptuneCallback(run=run)
            trainer = Trainer(**self._trainer_default_attributes, callbacks=[callback])
            trainer.train()
            run.sync()

            assert run.exists("source_code/integrations/transformers")
