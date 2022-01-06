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

import pytest

import neptune.new as neptune
import numpy as np
import torch
import torch.nn.functional as F
from pytorch_lightning.utilities.types import EVAL_DATALOADERS, TRAIN_DATALOADERS
from sklearn.metrics import accuracy_score
from torch.optim.lr_scheduler import LambdaLR
from torch.utils.data import DataLoader, random_split
from torchvision import transforms
from torchvision.datasets import MNIST

import pytorch_lightning as pl
from pytorch_lightning.callbacks import ModelCheckpoint
from pytorch_lightning.loggers import NeptuneLogger


class LitModel(pl.LightningModule):
    def train_dataloader(self) -> TRAIN_DATALOADERS:
        """Not used, for pylint only"""

    def test_dataloader(self) -> EVAL_DATALOADERS:
        """Not used, for pylint only"""

    def val_dataloader(self) -> EVAL_DATALOADERS:
        """Not used, for pylint only"""

    def predict_dataloader(self) -> EVAL_DATALOADERS:
        """Not used, for pylint only"""

    def __init__(self, linear, learning_rate, decay_factor, neptune_logger):
        super().__init__()
        self.linear = linear
        self.learning_rate = learning_rate
        self.decay_factor = decay_factor
        self.train_img_max = 10
        self.train_img = 0
        self.layer_1 = torch.nn.Linear(28 * 28, linear)
        self.layer_2 = torch.nn.Linear(linear, 20)
        self.layer_3 = torch.nn.Linear(20, 10)
        self.neptune_logger = neptune_logger

    def forward(self, x):
        x = x.view(x.size(0), -1)
        x = self.layer_1(x)
        x = F.relu(x)
        x = self.layer_2(x)
        x = F.relu(x)
        x = self.layer_3(x)
        return x

    def configure_optimizers(self):
        optimizer = torch.optim.Adam(self.parameters(), lr=self.learning_rate)
        scheduler = LambdaLR(optimizer, lambda epoch: self.decay_factor ** epoch)
        return [optimizer], [scheduler]

    def training_step(self, batch, batch_idx):
        x, y = batch
        y_hat = self(x)
        loss = F.cross_entropy(y_hat, y)
        self.log("train/loss", loss, prog_bar=False)
        y_true = y.cpu().detach().numpy()
        y_pred = y_hat.argmax(axis=1).cpu().detach().numpy()
        return {"loss": loss, "y_true": y_true, "y_pred": y_pred}

    def training_epoch_end(self, outputs):
        y_true = np.array([])
        y_pred = np.array([])
        for results_dict in outputs:
            y_true = np.append(y_true, results_dict["y_true"])
            y_pred = np.append(y_pred, results_dict["y_pred"])
        acc = accuracy_score(y_true, y_pred)
        self.log("train/loader_acc", acc)

    def validation_step(self, batch, batch_idx, dataset_idx):
        x, y = batch
        y_hat = self(x)
        loss = F.cross_entropy(y_hat, y)
        self.log("val/loss", loss, prog_bar=False)
        y_true = y.cpu().detach().numpy()
        y_pred = y_hat.argmax(axis=1).cpu().detach().numpy()
        return {"loss": loss, "y_true": y_true, "y_pred": y_pred}

    def validation_epoch_end(self, outputs):
        for dl_idx in range(2):
            y_true = np.array([])
            y_pred = np.array([])
            for results_dict in outputs[dl_idx]:
                y_true = np.append(y_true, results_dict["y_true"])
                y_pred = np.append(y_pred, results_dict["y_pred"])
            acc = accuracy_score(y_true, y_pred)
            self.log("val/loader_acc", acc)

    def test_step(self, batch, batch_idx):
        x, y = batch
        y_hat = self(x)
        loss = F.cross_entropy(y_hat, y)
        self.log("test/loss", loss, prog_bar=False)
        y_true = y.cpu().detach().numpy()
        y_pred = y_hat.argmax(axis=1).cpu().detach().numpy()
        for j in np.where(np.not_equal(y_true, y_pred))[0]:
            img = np.squeeze(x[j].cpu().detach().numpy())
            img[img < 0] = 0
            img = img / np.amax(img)
            self.neptune_logger.experiment["model_code/test/misclassified_images"].log(
                neptune.types.File.as_image(img),
                description=f"y_pred={y_pred[j]}, y_true={y_true[j]}",
            )
        return {"loss": loss, "y_true": y_true, "y_pred": y_pred}

    def test_epoch_end(self, outputs):
        y_true = np.array([])
        y_pred = np.array([])
        for results_dict in outputs:
            y_true = np.append(y_true, results_dict["y_true"])
            y_pred = np.append(y_pred, results_dict["y_pred"])
        acc = accuracy_score(y_true, y_pred)
        self.log("test/acc", acc)


class MNISTDataModule(pl.LightningDataModule):
    def predict_dataloader(self) -> EVAL_DATALOADERS:
        # not required for e2e test
        pass

    def __init__(self, batch_size, normalization_vector):
        super().__init__()
        self.batch_size = batch_size
        self.normalization_vector = normalization_vector
        self.mnist_train = None
        self.mnist_val1 = None
        self.mnist_val2 = None
        self.mnist_test = None

    def prepare_data(self):
        MNIST(os.getcwd(), train=True, download=True)
        MNIST(os.getcwd(), train=False, download=True)

    def setup(self, stage=None):
        # transforms
        transform = transforms.Compose(
            [
                transforms.ToTensor(),
                transforms.Normalize(
                    self.normalization_vector[0], self.normalization_vector[1]
                ),
            ]
        )
        if stage == "fit":
            mnist_train = MNIST(os.getcwd(), train=True, transform=transform)
            # do not use whole set, to save time spent on training
            self.mnist_train, self.mnist_val1, self.mnist_val2, _ = random_split(
                mnist_train, [5000, 500, 500, 54000]
            )
        if stage == "test":
            self.mnist_test = MNIST(os.getcwd(), train=False, transform=transform)

    def train_dataloader(self):
        mnist_train = DataLoader(
            self.mnist_train, batch_size=self.batch_size, num_workers=4
        )
        return mnist_train

    def val_dataloader(self):
        mnist_val1 = DataLoader(
            self.mnist_val1, batch_size=self.batch_size, num_workers=4
        )
        mnist_val2 = DataLoader(
            self.mnist_val2, batch_size=self.batch_size, num_workers=4
        )
        return [mnist_val1, mnist_val2]

    def test_dataloader(self):
        mnist_test = DataLoader(
            self.mnist_test, batch_size=self.batch_size, num_workers=1
        )
        return mnist_test


@pytest.fixture(scope="session")
def pytorch_run(environment):
    # given
    PARAMS = {
        "max_epochs": 3,
        "save_top_k": 2,
        "learning_rate": 0.005,
        "decay_factor": 0.99,
        "batch_size": 64,
        "linear": 64,
    }
    # and
    run = neptune.init(
        name="Integration pytorch-lightning", project=environment.project
    )
    # and
    model_checkpoint = ModelCheckpoint(
        dirpath="my_model/checkpoints/",
        filename="{epoch:02d}-{val/loss/dataloader_idx_1:.2f}",
        save_weights_only=True,
        save_top_k=PARAMS["save_top_k"],
        save_last=True,
        monitor="val/loss/dataloader_idx_1",
        every_n_epochs=1,
    )
    neptune_logger = NeptuneLogger(run=run, prefix="custom_prefix")
    # and (Subject)
    trainer = pl.Trainer(
        max_epochs=PARAMS["max_epochs"],
        log_every_n_steps=10,
        logger=neptune_logger,
        track_grad_norm=2,
        callbacks=[model_checkpoint],
    )
    model = LitModel(
        linear=PARAMS["linear"],
        learning_rate=PARAMS["learning_rate"],
        decay_factor=PARAMS["decay_factor"],
        neptune_logger=neptune_logger,
    )
    data_module = MNISTDataModule(
        normalization_vector=((0.1307,), (0.3081,)), batch_size=PARAMS["batch_size"]
    )

    # then
    trainer.fit(model, datamodule=data_module)
    trainer.test(model, datamodule=data_module)
    run.sync()

    yield run
