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
# pylint: disable=redefined-outer-name
import re

import pytest
import pytorch_lightning
import torch
from pytorch_lightning import LightningModule, Trainer
from pytorch_lightning.callbacks import ModelCheckpoint
from pytorch_lightning.loggers.neptune import NeptuneLogger
from torch.utils.data import DataLoader, Dataset

import neptune.new as neptune
from e2e_tests.base import BaseE2ETest


class RandomDataset(Dataset):
    def __init__(self, size, length):
        self.len = length
        # pylint: disable=no-member
        self.data = torch.randn(length, size)

    def __getitem__(self, index):
        return self.data[index]

    def __len__(self):
        return self.len


class BoringModel(LightningModule):
    # pylint: disable=abstract-method
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


@pytest.fixture(scope="session")
def pytorch_run(environment):
    # given
    run = neptune.init(name="Pytorch-Lightning integration", project=environment.project)
    # and
    model_checkpoint = ModelCheckpoint(
        dirpath="my_model/checkpoints/",
        filename="{epoch:02d}-{valid/loss:.2f}",
        save_weights_only=True,
        save_top_k=2,
        save_last=True,
        monitor="valid/loss",
        every_n_epochs=1,
    )
    neptune_logger = NeptuneLogger(run=run, prefix="custom_prefix")
    # and (Subject)
    model = BoringModel()
    trainer = Trainer(
        limit_train_batches=1,
        limit_val_batches=1,
        log_every_n_steps=1,
        max_epochs=3,
        logger=neptune_logger,
        callbacks=[model_checkpoint],
    )
    train_data = DataLoader(RandomDataset(32, 64), batch_size=2)
    val_data = DataLoader(RandomDataset(32, 64), batch_size=2)
    test_data = DataLoader(RandomDataset(32, 64), batch_size=2)

    # then
    trainer.fit(model, train_dataloaders=train_data, val_dataloaders=val_data)
    trainer.test(model, dataloaders=test_data)
    run.sync()

    yield run


@pytest.mark.integrations
class TestPytorchLightning(BaseE2ETest):
    def test_logging_values(self, pytorch_run):
        # correct integration version is logged
        if pytorch_run.exists("source_code/integrations/lightning"):
            logged_version = pytorch_run["source_code/integrations/lightning"].fetch()
        else:
            logged_version = pytorch_run["source_code/integrations/pytorch-lightning"].fetch()
        assert logged_version == pytorch_lightning.__version__  # pylint: disable=E1101

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
