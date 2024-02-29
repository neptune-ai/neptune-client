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

torch = pytest.importorskip("torch")
torch.utils.data = pytest.importorskip("torch.utils.data")
nn = pytest.importorskip("torch.nn")
F = pytest.importorskip("torch.nn.functional")

torchvision = pytest.importorskip("torchvision")

composer = pytest.importorskip("composer")
composer.loggers = pytest.importorskip("composer.loggers")
composer.models = pytest.importorskip("composer.models")
composer.algorithms = pytest.importorskip("composer.algorithms")
composer.callbacks = pytest.importorskip("composer.callbacks")


@pytest.fixture(scope="module")
def model() -> composer.models.ComposerClassifier:
    # https://github.com/mosaicml/composer/blob/dev/examples/checkpoint_with_wandb.py
    class Model(nn.Module):
        """Toy convolutional neural network architecture in pytorch for MNIST."""

        def __init__(self, num_classes: int = 10):
            super().__init__()

            self.num_classes = num_classes

            self.conv1 = nn.Conv2d(1, 16, (3, 3), padding=0)
            self.conv2 = nn.Conv2d(16, 32, (3, 3), padding=0)
            self.bn = nn.BatchNorm2d(32)
            self.fc1 = nn.Linear(32 * 16, 32)
            self.fc2 = nn.Linear(32, num_classes)

        def forward(self, x):
            out = self.conv1(x)
            out = F.relu(out)
            out = self.conv2(out)
            out = self.bn(out)
            out = F.relu(out)
            out = F.adaptive_avg_pool2d(out, (4, 4))
            out = torch.flatten(out, 1, -1)
            out = self.fc1(out)
            out = F.relu(out)
            return self.fc2(out)

    return composer.models.ComposerClassifier(module=Model(num_classes=10))


@pytest.mark.integrations
@pytest.mark.mosaicml
def test_e2e(environment, model):
    transform = torchvision.transforms.Compose([torchvision.transforms.ToTensor()])

    train_dataset = torchvision.datasets.MNIST("data", download=True, train=True, transform=transform)
    eval_dataset = torchvision.datasets.MNIST("data", download=True, train=False, transform=transform)

    train_dataset = torch.utils.data.Subset(train_dataset, indices=range(len(train_dataset) // 50))
    eval_dataset = torch.utils.data.Subset(eval_dataset, indices=range(len(eval_dataset) // 50))
    train_dataloader = torch.utils.data.DataLoader(train_dataset, batch_size=128)
    eval_dataloader = torch.utils.data.DataLoader(eval_dataset, batch_size=128)
    logger = composer.loggers.NeptuneLogger(project=environment.project, base_namespace="composer-training")

    trainer = composer.Trainer(
        model=model,
        train_dataloader=train_dataloader,
        eval_dataloader=eval_dataloader,
        max_duration="1ep",
        algorithms=[
            composer.algorithms.ChannelsLast(),
            composer.algorithms.CutMix(alpha=1.0),
            composer.algorithms.LabelSmoothing(smoothing=0.1),
        ],
        loggers=logger,
        callbacks=composer.callbacks.ImageVisualizer(),
    )
    trainer.fit()

    logger.neptune_run.sync()

    assert logger.neptune_run.exists("composer-training")

    assert logger.neptune_run.exists("composer-training/Images/Train")
    assert logger.neptune_run.exists("composer-training/Images/Eval")

    assert logger.neptune_run.exists("composer-training/metrics/loss/train/total")

    assert logger.neptune_run["composer-training/hyperparameters/num_nodes"].fetch() == 1

    assert logger.neptune_run.exists("composer-training/traces/algorithm_traces/ChannelsLast")
