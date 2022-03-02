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
import pytest

import torch
from torch.utils.data import Dataset, DataLoader

from pytorch_lightning import LightningModule, Trainer
from pytorch_lightning.loggers.neptune import NeptuneLogger
from pytorch_lightning.callbacks import ModelCheckpoint

import neptune.new as neptune


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
    run = neptune.init(
        name="Pytorch-Lightning integration", project=environment.project
    )
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
