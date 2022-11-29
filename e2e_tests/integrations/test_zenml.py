import hashlib

from pytest import fixture
from sklearn.datasets import load_digits
from sklearn.model_selection import train_test_split
from sklearn.svm import SVC
from zenml.client import Client
from zenml.enums import StackComponentType
from zenml.exceptions import (
    InitializationException,
    StackComponentExistsError,
)
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
def example_step() -> None:
    """A very minimalistic pipeline step.

    Loads a sample dataset, trains a simple classifier and logs
    a couple of metrics.
    """
    neptune_run = get_neptune_run()
    digits = load_digits()
    data = digits.images.reshape((len(digits.images), -1))

    x_train, x_test, y_train, y_test = train_test_split(data, digits.target, test_size=0.3)
    gamma = 0.001
    neptune_run["params/gamma"] = gamma
    model = SVC(gamma=gamma)
    model.fit(x_train, y_train)
    test_acc = model.score(x_test, y_test)
    neptune_run["metrics/val_accuracy"] = test_acc


@pipeline(
    enable_cache=False,
)
def neptune_example_pipeline(ex_step):
    """
    Link all the steps artifacts together
    """
    ex_step()


class TestZenML(BaseE2ETest):
    def _test_setup_creates_stack_with_neptune_experiment_tracker(self, zenml_client):
        assert zenml_client.active_stack.experiment_tracker.name == NEPTUNE_EXPERIMENT_TRACKER_NAME

    def _test_pipeline_runs_without_errors(self):
        run = neptune_example_pipeline(ex_step=example_step())
        run.run(config_path="zenml_config.yaml")

        self.zenml_run_name = run.get_runs()[-1].name

    def _test_fetch_neptune_run(self):
        custom_run_id = hashlib.md5(self.zenml_run_name.encode()).hexdigest()
        neptune_run = neptune.init_run(custom_run_id=custom_run_id)
        assert neptune_run["params/gamma"].fetch() == 0.001
        assert neptune_run["sys/tags"].fetch() == {"keras", "mnist"}
        assert neptune_run["metrics/val_accuracy"].fetch() <= 1

    def test_zenml(self, registered_stack, zenml_client):
        self._test_setup_creates_stack_with_neptune_experiment_tracker(zenml_client)
        self._test_pipeline_runs_without_errors()
        self._test_fetch_neptune_run()
