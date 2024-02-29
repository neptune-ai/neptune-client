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
import os
import re

import pytest

import neptune
from tests.e2e.base import BaseE2ETest

torch = pytest.importorskip("torch")
torch.utils.data = pytest.importorskip("torch.utils.data")
pytorch_lightning = pytest.importorskip("pytorch_lightning")
pytorch_lightning.callbacks = pytest.importorskip("pytorch_lightning.callbacks")
pytorch_lightning.loggers.neptune = pytest.importorskip("pytorch_lightning.loggers.neptune")

LIGHTNING_ECOSYSTEM_ENV_PROJECT = "NEPTUNE_LIGHTNING_ECOSYSTEM_CI_PROJECT"

skip_if_on_regular_env = pytest.mark.skipif(
    LIGHTNING_ECOSYSTEM_ENV_PROJECT not in os.environ, reason="Tests weren't invoked in Lightning Ecosystem CI"
)
skip_if_on_lightning_ecosystem = pytest.mark.skipif(
    LIGHTNING_ECOSYSTEM_ENV_PROJECT in os.environ, reason="Tests invoked in Lightning Ecosystem CI"
)


class RandomDataset(torch.utils.data.Dataset):
    def __init__(self, size, length):
        self.len = length
        self.data = torch.randn(length, size)

    def __getitem__(self, index):
        return self.data[index]

    def __len__(self):
        return self.len


class BoringModel(pytorch_lightning.LightningModule):
    def __init__(self):
        super().__init__()
        self.layer = torch.nn.Linear(32, 2)

    def forward(self, *args, **kwargs):
        return self.layer(*args, **kwargs)

    def training_step(self, *args, **kwargs):
        batch, *_ = args
        loss = self(batch).sum()
        self.log("train/loss", loss)
        return {"loss": loss}

    def validation_step(self, *args, **kwargs):
        batch, *_ = args
        loss = self(batch, **kwargs).sum()
        self.log("valid/loss", loss)

    def test_step(self, *args, **kwargs):
        batch, *_ = args
        loss = self(batch, **kwargs).sum()
        self.log("test/loss", loss)

    def configure_optimizers(self):
        return torch.optim.SGD(self.layer.parameters(), lr=0.1)


def prepare(project):
    # given
    run = neptune.init_run(name="Pytorch-Lightning integration", project=project)
    # and
    model_checkpoint = pytorch_lightning.callbacks.ModelCheckpoint(
        dirpath="my_model/checkpoints/",
        filename="{epoch:02d}-{valid/loss:.2f}",
        save_weights_only=True,
        save_top_k=2,
        save_last=True,
        monitor="valid/loss",
        every_n_epochs=1,
    )
    neptune_logger = pytorch_lightning.loggers.neptune.NeptuneLogger(run=run, prefix="custom_prefix")
    # and (Subject)
    model = BoringModel()
    trainer = pytorch_lightning.Trainer(
        limit_train_batches=1,
        limit_val_batches=1,
        log_every_n_steps=1,
        max_epochs=3,
        logger=neptune_logger,
        callbacks=[model_checkpoint],
    )
    train_data = torch.utils.data.DataLoader(RandomDataset(32, 64), batch_size=2)
    val_data = torch.utils.data.DataLoader(RandomDataset(32, 64), batch_size=2)
    test_data = torch.utils.data.DataLoader(RandomDataset(32, 64), batch_size=2)

    # then
    trainer.fit(model, train_dataloaders=train_data, val_dataloaders=val_data)
    trainer.test(model, dataloaders=test_data)
    run.sync()

    return run


@pytest.fixture(scope="session")
def model_in_regular_env(environment):
    yield prepare(project=environment.project)


@pytest.fixture(scope="session")
def model_in_lightning_ci_project():
    yield prepare(project=os.getenv("NEPTUNE_LIGHTNING_ECOSYSTEM_CI_PROJECT"))


@pytest.mark.integrations
@pytest.mark.lightning
class TestPytorchLightning(BaseE2ETest):
    def _test_logging_values(self, pytorch_run):
        # correct integration version is logged
        if pytorch_run.exists("source_code/integrations/lightning"):
            logged_version = pytorch_run["source_code/integrations/lightning"].fetch()
        else:
            logged_version = pytorch_run["source_code/integrations/pytorch-lightning"].fetch()
        assert logged_version == pytorch_lightning.__version__

        assert pytorch_run.exists("custom_prefix/valid/loss")
        assert len(pytorch_run["custom_prefix/valid/loss"].fetch_values()) == 3

    @skip_if_on_lightning_ecosystem
    def test_logging_values(self, model_in_regular_env):
        self._test_logging_values(model_in_regular_env)

    @skip_if_on_regular_env
    def test_logging_values_in_lightning_ci(self, model_in_lightning_ci_project):
        self._test_logging_values(model_in_lightning_ci_project)

    def _test_saving_models(self, pytorch_run):
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

    @skip_if_on_lightning_ecosystem
    def test_saving_models(self, model_in_regular_env):
        self._test_saving_models(model_in_regular_env)

    @skip_if_on_regular_env
    def test_saving_models_in_lightning_ci(self, model_in_lightning_ci_project):
        self._test_saving_models(model_in_lightning_ci_project)
