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
import os
import time
from zipfile import ZipFile

import numpy as np
import pytest
from faker import Faker

from neptune import init_run
from neptune.table import TableEntry
from tests.e2e.base import BaseE2ETest
from tests.e2e.utils import (
    catch_time,
    modified_environ,
    tmp_context,
)

torch = pytest.importorskip("torch")
transformers = pytest.importorskip("transformers")
transformers.integrations = pytest.importorskip("transformers.integrations")
transformers.utils = pytest.importorskip("transformers.utils")

MAX_OVERWHELMING_FACTOR = 1.2
SECONDS_TO_WAIT_FOR_UPDATE = 15


transformers.utils.logging.set_verbosity_error()
fake = Faker()


class RegressionModelConfig(transformers.PretrainedConfig):
    def __init__(self, a=2, b=3, **kwargs):
        super().__init__(**kwargs)
        self.a = a
        self.b = b


class RegressionPreTrainedModel(transformers.PreTrainedModel):
    config_class = RegressionModelConfig
    base_model_prefix = "regression"

    def __init__(self, config):
        super().__init__(config)
        self.a = torch.nn.Parameter(torch.tensor(config.a).float())
        self.b = torch.nn.Parameter(torch.tensor(config.b).float())

    def forward(self, input_x, labels=None):
        y = input_x * self.a + self.b
        if labels is None:
            return (y,)
        loss = torch.nn.functional.mse_loss(y, labels)
        return loss, y


class RegressionDataset:
    def __init__(self, a=2, b=3, length=64, seed=2501):
        np.random.seed(seed)

        self.label_names = ["labels"]
        self.length = length
        self.x = np.random.normal(size=(length,)).astype(np.float32)
        self.ys = [a * self.x + b + np.random.normal(scale=0.1, size=(length,)) for _ in self.label_names]
        self.ys = [y.astype(np.float32) for y in self.ys]

    def __len__(self):
        return self.length

    def __getitem__(self, i):
        result = {name: y[i] for name, y in zip(self.label_names, self.ys)}
        result["input_x"] = self.x[i]
        return result


@pytest.mark.huggingface
@pytest.mark.integrations
class TestHuggingFace(BaseE2ETest):
    @property
    def _trainer_default_attributes(self):
        config = RegressionModelConfig()
        model = RegressionPreTrainedModel(config)
        train_dataset = RegressionDataset(length=32)
        validation_dataset = RegressionDataset(length=16)

        train_args = transformers.TrainingArguments("model", report_to=[], num_train_epochs=500, learning_rate=0.5)

        return {
            "model": model,
            "args": train_args,
            "train_dataset": train_dataset,
            "eval_dataset": validation_dataset,
        }

    def _test_with_run_initialization(self, environment, *, pre, post):
        with init_run(project=environment.project, api_token=environment.user_token) as run:
            run_id = run["sys/id"].fetch()
            pre(run)

        time.sleep(SECONDS_TO_WAIT_FOR_UPDATE)
        run = init_run(
            with_id=run_id,
            project=environment.project,
            api_token=environment.user_token,
            mode="read-only",
        )
        post(run)

    def _test_monitoring_cpu_present(self, environment, table_entry: TableEntry) -> bool:
        with init_run(
            with_id=table_entry.get_attribute_value("sys/id"),
            project=environment.project,
            api_token=environment.user_token,
        ) as neptune_run:
            hash_key = list(neptune_run.get_structure()["monitoring"].keys())[0]

            return neptune_run.exists(f"monitoring/{hash_key}/cpu")

    def test_every_train_should_create_new_run(self, environment, project, common_tag):
        trainer = transformers.Trainer(
            **self._trainer_default_attributes,
            callbacks=[
                transformers.integrations.NeptuneCallback(
                    api_token=environment.user_token, project=environment.project, tags=[common_tag]
                )
            ],
        )

        expected_times = 5
        for _ in range(expected_times):
            trainer.train()

        time.sleep(SECONDS_TO_WAIT_FOR_UPDATE)
        assert len(project.fetch_runs_table(tag=common_tag).to_rows()) == expected_times

    def test_runtime_factor(self, environment):
        with catch_time() as standard:
            trainer = transformers.Trainer(**self._trainer_default_attributes)
            trainer.train()
            del trainer

        with catch_time() as with_neptune_callback:
            trainer = transformers.Trainer(
                **self._trainer_default_attributes,
                callbacks=[
                    transformers.integrations.NeptuneCallback(
                        api_token=environment.user_token, project=environment.project
                    )
                ],
            )
            trainer.train()
            del trainer

        assert with_neptune_callback() / standard() <= MAX_OVERWHELMING_FACTOR

    def test_run_access_methods(self, environment):
        callback = transformers.integrations.NeptuneCallback(
            api_token=environment.user_token, project=environment.project
        )
        trainer = transformers.Trainer(**self._trainer_default_attributes, callbacks=[callback])

        assert callback.run.get_url() == transformers.integrations.NeptuneCallback.get_run(trainer).get_url()

    def test_initialization_with_run_provided(self, environment):
        run = init_run(project=environment.project, api_token=environment.user_token)
        callback = transformers.integrations.NeptuneCallback(run=run)
        trainer = transformers.Trainer(**self._trainer_default_attributes, callbacks=[callback])

        assert run.get_url() == transformers.integrations.NeptuneCallback.get_run(trainer).get_url()

    def test_run_reinitialization_failure(self, environment):
        run = init_run(project=environment.project, api_token=environment.user_token)

        with modified_environ("NEPTUNE_API_TOKEN", "NEPTUNE_PROJECT"):
            callback = transformers.integrations.NeptuneCallback(run=run)
            trainer = transformers.Trainer(**self._trainer_default_attributes, callbacks=[callback])

            trainer.train()

            with pytest.raises(transformers.integrations.NeptuneMissingConfiguration):
                trainer.train()

    def test_run_access_without_callback_configured(self):
        trainer = transformers.Trainer(**self._trainer_default_attributes)

        with pytest.raises(Exception):
            transformers.integrations.NeptuneCallback.get_run(trainer)

    def test_log_parameters_with_base_namespace(self, environment):
        base_namespace = "custom/base/path"

        def run_test(run):
            callback = transformers.integrations.NeptuneCallback(run=run, base_namespace=base_namespace)
            trainer = transformers.Trainer(**self._trainer_default_attributes, callbacks=[callback])
            trainer.train()

        def assert_metadata_structure(run):
            assert run.exists(f"{base_namespace}/trainer_parameters")
            assert run.exists(f"{base_namespace}/trainer_parameters/num_train_epochs")
            assert run[f"{base_namespace}/trainer_parameters/num_train_epochs"].fetch() == 500

            assert run.exists(f"{base_namespace}/model_parameters")
            assert run.exists(f"{base_namespace}/model_parameters/a")
            assert run.exists(f"{base_namespace}/model_parameters/b")
            assert run[f"{base_namespace}/model_parameters/a"].fetch() == 2
            assert run[f"{base_namespace}/model_parameters/b"].fetch() == 3

        self._test_with_run_initialization(environment, pre=run_test, post=assert_metadata_structure)

    def test_log_parameters_disabled(self, environment):
        def run_test(run):
            callback = transformers.integrations.NeptuneCallback(run=run, log_parameters=False)
            trainer = transformers.Trainer(**self._trainer_default_attributes, callbacks=[callback])
            trainer.train()

        def assert_metadata_structure(run):
            assert not run.exists("finetuning/trainer_parameters")
            assert not run.exists("finetuning/model_parameters")

        self._test_with_run_initialization(environment, pre=run_test, post=assert_metadata_structure)

    def test_log_with_custom_base_namespace(self, environment):
        base_namespace = "just/a/sample/path"

        def run_test(run):
            callback = transformers.integrations.NeptuneCallback(
                run=run,
                base_namespace=base_namespace,
                project=environment.project,
                api_token=environment.user_token,
            )
            trainer = transformers.Trainer(**self._trainer_default_attributes, callbacks=[callback])
            trainer.log({"metric1": 123, "another/metric": 0.2})
            trainer.train()
            trainer.log({"after_training_metric": 2501})

        def assert_metadata_structure(run):
            assert run.exists(f"{base_namespace}/train")
            assert run.exists(f"{base_namespace}/train/metric1")
            assert run.exists(f"{base_namespace}/train/another/metric")
            assert run.exists(f"{base_namespace}/train/after_training_metric")

            assert run[f"{base_namespace}/train/metric1"].fetch_last() == 123
            assert run[f"{base_namespace}/train/another/metric"].fetch_last() == 0.2
            assert run[f"{base_namespace}/train/after_training_metric"].fetch_last() == 2501

        self._test_with_run_initialization(environment, pre=run_test, post=assert_metadata_structure)

    def test_integration_version_is_logged(self, environment):
        def run_test(run):
            callback = transformers.integrations.NeptuneCallback(run=run)
            trainer = transformers.Trainer(**self._trainer_default_attributes, callbacks=[callback])
            trainer.train()

        def assert_metadata_structure(run):
            assert run.exists("source_code/integrations/transformers")

        self._test_with_run_initialization(environment, pre=run_test, post=assert_metadata_structure)

    def test_non_monitoring_runs_creation(self, environment, project, common_tag):
        # given
        callback = transformers.integrations.NeptuneCallback(
            project=environment.project, api_token=environment.user_token, tags=common_tag
        )
        trainer = transformers.Trainer(**self._trainer_default_attributes, callbacks=[callback])

        # when
        trainer.log({"metric1": 123})
        time.sleep(SECONDS_TO_WAIT_FOR_UPDATE)

        # then
        runs = project.fetch_runs_table(tag=common_tag).to_rows()
        assert len(runs) == 1
        with pytest.raises(ValueError):
            runs[0].get_attribute_value("monitoring/cpu")
        assert runs[0].get_attribute_value("finetuning/train/metric1") == 123

        # when
        trainer.train()
        time.sleep(SECONDS_TO_WAIT_FOR_UPDATE)

        # then
        runs = project.fetch_runs_table(tag=common_tag).to_rows()
        assert len(runs) == 1
        assert self._test_monitoring_cpu_present(environment, runs[0])

        # when
        trainer.log({"metric2": 234})
        time.sleep(SECONDS_TO_WAIT_FOR_UPDATE)

        # then
        runs = project.fetch_runs_table(tag=common_tag).to_rows()
        assert len(runs) == 1
        # assert runs[0].get_attribute_value("monitoring/cpu") is not None
        assert runs[0].get_attribute_value("finetuning/train/metric2") == 234

        # when
        trainer.train()
        time.sleep(SECONDS_TO_WAIT_FOR_UPDATE)

        # then
        runs = sorted(
            project.fetch_runs_table(tag=common_tag).to_rows(),
            key=lambda run: run.get_attribute_value("sys/id"),
        )
        assert len(runs) == 2
        assert self._test_monitoring_cpu_present(environment, runs[1])

        # when
        trainer.log({"metric3": 345})
        time.sleep(SECONDS_TO_WAIT_FOR_UPDATE)

        # then
        runs = sorted(
            project.fetch_runs_table(tag=common_tag).to_rows(),
            key=lambda run: run.get_attribute_value("sys/id"),
        )
        assert len(runs) == 2
        assert runs[1].get_attribute_value("finetuning/train/metric3") == 345

    def test_non_monitoring_runs_creation_with_initial_run(self, environment, project, common_tag):
        # given
        initial_run = init_run(project=environment.project, api_token=environment.user_token, tags=common_tag)
        callback = transformers.integrations.NeptuneCallback(
            project=environment.project,
            api_token=environment.user_token,
            tags=common_tag,
            run=initial_run,
        )
        trainer = transformers.Trainer(**self._trainer_default_attributes, callbacks=[callback])

        # when
        trainer.log({"metric1": 123})
        time.sleep(SECONDS_TO_WAIT_FOR_UPDATE)

        # then
        runs = project.fetch_runs_table(tag=common_tag).to_rows()
        assert len(runs) == 1
        assert self._test_monitoring_cpu_present(environment, runs[0])
        assert runs[0].get_attribute_value("finetuning/train/metric1") == 123

        # when
        trainer.train()
        time.sleep(SECONDS_TO_WAIT_FOR_UPDATE)

        # then
        runs = project.fetch_runs_table(tag=common_tag).to_rows()
        assert len(runs) == 1
        assert self._test_monitoring_cpu_present(environment, runs[0])

        # when
        trainer.log({"metric2": 234})
        time.sleep(SECONDS_TO_WAIT_FOR_UPDATE)

        # then
        runs = project.fetch_runs_table(tag=common_tag).to_rows()
        assert len(runs) == 1
        assert self._test_monitoring_cpu_present(environment, runs[0])
        assert runs[0].get_attribute_value("finetuning/train/metric2") == 234

        # when
        trainer.train()
        time.sleep(SECONDS_TO_WAIT_FOR_UPDATE)

        # then
        runs = sorted(
            project.fetch_runs_table(tag=common_tag).to_rows(),
            key=lambda run: run.get_attribute_value("sys/id"),
        )
        assert len(runs) == 2
        assert self._test_monitoring_cpu_present(environment, runs[0])

        # when
        trainer.log({"metric3": 345})
        time.sleep(SECONDS_TO_WAIT_FOR_UPDATE)

        # then
        runs = sorted(
            project.fetch_runs_table(tag=common_tag).to_rows(),
            key=lambda run: run.get_attribute_value("sys/id"),
        )
        assert len(runs) == 2
        assert runs[1].get_attribute_value("finetuning/train/metric3") == 345

    def test_hyperparameter_optimization(self, environment, project, common_tag):
        # given
        n_trials = 5

        # and
        def model_init():
            config = RegressionModelConfig()
            return RegressionPreTrainedModel(config)

        # and
        callback = transformers.integrations.NeptuneCallback(
            project=environment.project, api_token=environment.user_token, tags=common_tag
        )
        trainer_config = self._trainer_default_attributes
        del trainer_config["model"]
        trainer = transformers.Trainer(**trainer_config, model_init=model_init, callbacks=[callback])

        # when
        trainer.hyperparameter_search(
            backend="optuna", n_trials=n_trials, hp_name=lambda trial: f"trial_{trial.number}"
        )
        time.sleep(SECONDS_TO_WAIT_FOR_UPDATE)

        # then
        runs = sorted(
            project.fetch_runs_table(tag=common_tag).to_rows(),
            key=lambda run: run.get_attribute_value("sys/id"),
        )
        assert len(runs) == n_trials
        for run_id, run in enumerate(runs):
            assert run.get_attribute_value("finetuning/trial") == f"trial_{run_id}"
            assert self._test_monitoring_cpu_present(environment, run)

    def test_usages(self):
        # given
        trainer_args = self._trainer_default_attributes
        trainer_args["args"] = transformers.TrainingArguments("model", report_to=["all"])

        # when
        trainer = transformers.Trainer(**trainer_args)

        # then
        assert "NeptuneCallback" in [type(callback).__name__ for callback in trainer.callback_handler.callbacks]

        # given
        trainer_args = self._trainer_default_attributes
        trainer_args["args"] = transformers.TrainingArguments("model", report_to=["neptune"])

        # when
        trainer = transformers.Trainer(**trainer_args)

        # then
        assert "NeptuneCallback" in [type(callback).__name__ for callback in trainer.callback_handler.callbacks]

        # when
        trainer = transformers.Trainer(
            **self._trainer_default_attributes, callbacks=[transformers.integrations.NeptuneCallback]
        )

        # then
        assert "NeptuneCallback" in [type(callback).__name__ for callback in trainer.callback_handler.callbacks]

        # when
        trainer = transformers.Trainer(
            **self._trainer_default_attributes, callbacks=[transformers.integrations.NeptuneCallback()]
        )

        # then
        assert "NeptuneCallback" in [type(callback).__name__ for callback in trainer.callback_handler.callbacks]

    def _test_checkpoints_creation(
        self,
        environment,
        log_checkpoints,
        expected_checkpoints=None,
        expected_checkpoints_number=None,
        additional_training_args=None,
        checkpoints_key="",
    ):
        if expected_checkpoints is not None:
            expected_checkpoints.update({"/", "model"})

        if additional_training_args is None:
            additional_training_args = {}

        def run_test(run):
            callback = transformers.integrations.NeptuneCallback(run=run, log_checkpoints=log_checkpoints)
            training_args = self._trainer_default_attributes
            training_args["args"] = transformers.TrainingArguments(
                "model",
                report_to=[],
                num_train_epochs=500,
                save_steps=1000,
                save_strategy="steps",
                **additional_training_args,
            )
            trainer = transformers.Trainer(**training_args, callbacks=[callback])
            trainer.train()

        def assert_metadata_structure(run):
            assert run.exists("finetuning/checkpoints")
            assert run["finetuning/train/epoch"].fetch() == 500
            with tmp_context():
                run[f"finetuning/checkpoints/{checkpoints_key}"].download("checkpoints.zip")

                with ZipFile("checkpoints.zip") as handler:
                    subdirectories = set([os.path.dirname(x) for x in handler.namelist()])

                    if expected_checkpoints_number is not None:
                        assert len(subdirectories) == expected_checkpoints_number

                    if expected_checkpoints is not None:
                        assert subdirectories == expected_checkpoints
                    handler.extractall(".")

        self._test_with_run_initialization(environment, pre=run_test, post=assert_metadata_structure)

    def _test_restore_from_checkpoint(self, environment):
        def run_test(run):
            callback = transformers.integrations.NeptuneCallback(run=run)
            training_args = self._trainer_default_attributes
            training_args["args"] = transformers.TrainingArguments("model", report_to=[], num_train_epochs=1000)
            trainer = transformers.Trainer(**training_args, callbacks=[callback])
            checkpoint_id = max(os.listdir("model"))
            trainer.train(resume_from_checkpoint=f"model/{checkpoint_id}")

        def assert_metadata_structure(run):
            assert run["finetuning/train/epoch"].fetch() == 1000

        self._test_with_run_initialization(environment, pre=run_test, post=assert_metadata_structure)

    @pytest.mark.parametrize("checkpoint_settings", [{}, {"save_total_limit": 1}, {"overwrite_output_dir": True}])
    def test_model_checkpoints_same(self, environment, checkpoint_settings):
        with tmp_context():
            self._test_checkpoints_creation(
                environment=environment,
                log_checkpoints="same",
                expected_checkpoints={"model/checkpoint-1000", "model/checkpoint-2000"},
                additional_training_args=checkpoint_settings,
            )
            self._test_restore_from_checkpoint(environment=environment)

    @pytest.mark.parametrize("checkpoint_settings", [{}, {"save_total_limit": 1}, {"overwrite_output_dir": True}])
    def test_model_checkpoints_last(self, environment, checkpoint_settings):
        with tmp_context():
            self._test_checkpoints_creation(
                environment=environment,
                log_checkpoints="last",
                expected_checkpoints={"model/checkpoint-2000"},
                checkpoints_key="last",
                additional_training_args=checkpoint_settings,
            )
            self._test_restore_from_checkpoint(environment=environment)

    @pytest.mark.parametrize("checkpoint_settings", [{}, {"save_total_limit": 1}, {"overwrite_output_dir": True}])
    def test_model_checkpoints_best(self, environment, checkpoint_settings):
        with tmp_context():
            self._test_checkpoints_creation(
                environment=environment,
                log_checkpoints="best",
                additional_training_args={
                    "load_best_model_at_end": True,
                    "evaluation_strategy": "steps",
                    "eval_steps": 500,
                    **checkpoint_settings,
                },
                expected_checkpoints_number=3,
                checkpoints_key="best",
            )
            self._test_restore_from_checkpoint(environment=environment)

    def test_model_checkpoints_best_invalid_load_best_model_at_end(self, environment):
        with init_run(project=environment.project, api_token=environment.user_token) as run:
            callback = transformers.integrations.NeptuneCallback(run=run, log_checkpoints="best")
            training_args = self._trainer_default_attributes
            training_args["args"] = transformers.TrainingArguments(
                "model",
                report_to=[],
                num_train_epochs=500,
                learning_rate=0.5,
                save_steps=500,
                save_strategy="steps",
                load_best_model_at_end=False,
                evaluation_strategy="steps",
                eval_steps=500,
            )
            with pytest.raises(ValueError):
                transformers.Trainer(**training_args, callbacks=[callback])
