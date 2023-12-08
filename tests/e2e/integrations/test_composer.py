#
# Copyright (c) 2023, Neptune Labs Sp. z o.o.
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
from composer import Trainer
from composer.algorithms import (
    ChannelsLast,
    CutMix,
    LabelSmoothing,
)
from composer.callbacks import ImageVisualizer
from composer.loggers import NeptuneLogger
from composer.models import mnist_model
from torch.utils.data import (
    DataLoader,
    Subset,
)
from torchvision import (
    datasets,
    transforms,
)


@pytest.mark.integrations
@pytest.mark.composer
def test_e2e(environment):
    transform = transforms.Compose([transforms.ToTensor()])

    train_dataset = datasets.MNIST("data", download=True, train=True, transform=transform)
    eval_dataset = datasets.MNIST("data", download=True, train=False, transform=transform)

    train_dataset = Subset(train_dataset, indices=range(len(train_dataset) // 50))
    eval_dataset = Subset(eval_dataset, indices=range(len(eval_dataset) // 50))
    train_dataloader = DataLoader(train_dataset, batch_size=128)
    eval_dataloader = DataLoader(eval_dataset, batch_size=128)
    logger = NeptuneLogger(project=environment.project, base_namespace="composer-training")

    trainer = Trainer(
        model=mnist_model(),
        train_dataloader=train_dataloader,
        eval_dataloader=eval_dataloader,
        max_duration="1ep",
        algorithms=[
            ChannelsLast(),
            CutMix(alpha=1.0),
            LabelSmoothing(smoothing=0.1),
        ],
        loggers=logger,
        callbacks=ImageVisualizer(),
    )
    trainer.fit()

    logger.neptune_run.sync()

    assert logger.neptune_run.exists("composer-training")

    assert logger.neptune_run.exists("composer-training/Images/Train")
    assert logger.neptune_run.exists("composer-training/Images/Eval")

    assert logger.neptune_run.exists("composer-training/metrics/loss/train/total")

    assert logger.neptune_run["composer-training/hyperparameters/num_nodes"].fetch() == 1

    assert logger.neptune_run.exists("composer-training/traces/algorithm_traces/ChannelsLast")
