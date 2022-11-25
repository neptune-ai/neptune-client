import hashlib

import numpy as np
import tensorflow as tf
from pytest import fixture
from zenml.client import Client
from zenml.enums import StackComponentType
from zenml.exceptions import (
    InitializationException,
    StackComponentExistsError,
)
from zenml.integrations.neptune.experiment_trackers import NeptuneExperimentTrackerSettings
from zenml.integrations.neptune.experiment_trackers.run_state import get_neptune_run
from zenml.models.component_model import ComponentModel
from zenml.models.stack_models import StackModel
from zenml.pipelines import pipeline
from zenml.steps import (
    BaseParameters,
    Output,
    step,
)

import neptune.new as neptune
from e2e_tests.base import BaseE2ETest
from neptune.new.integrations.tensorflow_keras import NeptuneCallback

NEPTUNE_EXPERIMENT_TRACKER_NAME = "neptune_tracker"
NEPTUNE_STACK_NAME = "neptune_stack"


@fixture
def zenml_client() -> Client:
    return Client()


@fixture
def experiment_tracker_comp(zenml_client):
    return ComponentModel(
        name=NEPTUNE_EXPERIMENT_TRACKER_NAME,
        type=StackComponentType.EXPERIMENT_TRACKER,
        flavor="neptune",
        user=zenml_client.active_user.id,
        project=zenml_client.active_project.id,
        configuration={},
    )


@fixture
def stack_with_neptune(zenml_client, experiment_tracker_comp):
    a_id = zenml_client.active_stack.artifact_store.id
    o_id = zenml_client.active_stack.orchestrator.id
    e_id = experiment_tracker_comp.id

    return StackModel(
        name=NEPTUNE_STACK_NAME,
        components={
            StackComponentType.ARTIFACT_STORE: [a_id],
            StackComponentType.ORCHESTRATOR: [o_id],
            StackComponentType.EXPERIMENT_TRACKER: [e_id],
        },
        user=zenml_client.active_user.id,
        project=zenml_client.active_project.id,
    )


@fixture
def registered_stack(zenml_client, experiment_tracker_comp, stack_with_neptune):
    try:
        zenml_client.initialize()
    except InitializationException:
        pass

    if zenml_client.active_stack.name != NEPTUNE_STACK_NAME:
        try:
            zenml_client.register_stack_component(experiment_tracker_comp)
            zenml_client.register_stack(stack_with_neptune)
        except StackComponentExistsError:
            stacks = zenml_client.stacks
            for stack in stacks:
                if stack.name == NEPTUNE_STACK_NAME:
                    stack_with_neptune = stack
        zenml_client.activate_stack(stack_with_neptune)


@step
def loader_mnist() -> Output(x_train=np.ndarray, y_train=np.ndarray, x_test=np.ndarray, y_test=np.ndarray):
    """Download the MNIST data store it as an artifact"""
    (x_train, y_train), (
        x_test,
        y_test,
    ) = tf.keras.datasets.mnist.load_data()
    return x_train, y_train, x_test, y_test


@step
def normalizer(x_train: np.ndarray, x_test: np.ndarray) -> Output(x_train_normed=np.ndarray, x_test_normed=np.ndarray):
    """Normalize the values for all the images so they are between 0 and 1"""
    x_train_normed = x_train / 255.0
    x_test_normed = x_test / 255.0
    return x_train_normed, x_test_normed


class TrainerParameters(BaseParameters):
    """Trainer params"""

    epochs: int = 1
    lr: float = 0.001


settings = NeptuneExperimentTrackerSettings(tags={"keras", "mnist"})


@step
def tf_trainer(
    params: TrainerParameters,
    x_train: np.ndarray,
    y_train: np.ndarray,
) -> tf.keras.Model:
    """Train a neural net from scratch to recognize MNIST digits return our
    model or the learner"""
    neptune_run = get_neptune_run()
    neptune_run["params/lr"] = params.lr

    neptune_cbk = NeptuneCallback(run=neptune_run, base_namespace="metrics")

    model = tf.keras.Sequential(
        [
            tf.keras.layers.Flatten(input_shape=(28, 28)),
            tf.keras.layers.Dense(10),
        ]
    )

    model.compile(
        optimizer=tf.keras.optimizers.Adam(params.lr),
        loss=tf.keras.losses.SparseCategoricalCrossentropy(from_logits=True),
        metrics=["accuracy"],
    )

    model.fit(
        x_train,
        y_train,
        epochs=params.epochs,
        batch_size=64,
        callbacks=[neptune_cbk],
    )

    return model


@step
def tf_evaluator(
    x_test: np.ndarray,
    y_test: np.ndarray,
    model: tf.keras.Model,
) -> float:
    """Calculate the loss for the model for each epoch in a graph"""
    neptune_run = get_neptune_run()
    _, test_acc = model.evaluate(x_test, y_test, verbose=2)
    neptune_run["metrics/val_accuracy"] = test_acc
    return test_acc


@pipeline(
    enable_cache=False,
)
def neptune_example_pipeline(
    importer,
    normalizer,
    trainer,
    evaluator,
):
    """
    Link all the steps artifacts together
    """
    x_train, y_train, x_test, y_test = importer()
    x_trained_normed, x_test_normed = normalizer(x_train=x_train, x_test=x_test)
    model = trainer(x_train=x_trained_normed, y_train=y_train)
    evaluator(x_test=x_test_normed, y_test=y_test, model=model)


class TestZenML(BaseE2ETest):
    def _test_setup_creates_stack_with_neptune_experiment_tracker(self, zenml_client):
        assert zenml_client.active_stack.experiment_tracker.name == NEPTUNE_EXPERIMENT_TRACKER_NAME

    def _test_pipeline_runs_without_errors(self):
        run = neptune_example_pipeline(
            importer=loader_mnist(),
            normalizer=normalizer(),
            trainer=tf_trainer(params=TrainerParameters(epochs=5, lr=0.0001)),
            evaluator=tf_evaluator(),
        )
        run.run(config_path="zenml_config.yaml")

        self.zenml_run_name = run.get_runs()[-1].name

    def _test_fetch_neptune_run(self):
        custom_run_id = hashlib.md5(self.zenml_run_name.encode()).hexdigest()
        neptune_run = neptune.init_run(custom_run_id=custom_run_id)
        assert neptune_run["params/lr"].fetch() == 0.0001
        assert neptune_run["sys/tags"].fetch() == {"keras", "mnist"}
        assert neptune_run["metrics/val_accuracy"].fetch() <= 1

    def test_zenml(self, registered_stack, zenml_client):
        self._test_setup_creates_stack_with_neptune_experiment_tracker(zenml_client)
        self._test_pipeline_runs_without_errors()
        self._test_fetch_neptune_run()
