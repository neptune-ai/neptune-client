import hashlib

import pytest
from sklearn.datasets import load_digits
from sklearn.model_selection import train_test_split
from sklearn.svm import SVC

import neptune
from tests.e2e.base import BaseE2ETest

zenml = pytest.importorskip("zenml")
pipelines = pytest.importorskip("zenml.pipelines")
flavors = pytest.importorskip("zenml.integrations.neptune.flavors")

NEPTUNE_EXPERIMENT_TRACKER_NAME = "neptune_tracker"
NEPTUNE_STACK_NAME = "neptune_stack"


@pytest.fixture(scope="session")
def registered_stack(zenml_client, experiment_tracker_comp, stack_with_neptune):
    try:
        zenml_client.initialize()
    except zenml.exceptions.InitializationException:
        pass

    zenml_client.activate_stack(NEPTUNE_STACK_NAME)


@pytest.fixture(scope="session")
def zenml_client():
    from zenml.client import Client

    return Client()


@pytest.fixture(scope="session")
def experiment_tracker_comp(zenml_client, environment):
    try:
        return zenml_client.create_stack_component(
            name=NEPTUNE_EXPERIMENT_TRACKER_NAME,
            component_type=zenml.enums.StackComponentType.EXPERIMENT_TRACKER,
            flavor="neptune",
            configuration={"api_token": environment.user_token, "project": environment.project},
        )
    except zenml.exceptions.StackComponentExistsError:
        return zenml_client.get_stack_component(
            component_type=zenml.enums.StackComponentType.EXPERIMENT_TRACKER,
            name_id_or_prefix=NEPTUNE_EXPERIMENT_TRACKER_NAME,
        )


@pytest.fixture(scope="session")
def stack_with_neptune(zenml_client, experiment_tracker_comp):
    a_id = zenml_client.active_stack.artifact_store.id
    o_id = zenml_client.active_stack.orchestrator.id
    e_id = experiment_tracker_comp.id

    try:
        return zenml_client.create_stack(
            name=NEPTUNE_STACK_NAME,
            components={
                zenml.enums.StackComponentType.ARTIFACT_STORE: a_id,
                zenml.enums.StackComponentType.ORCHESTRATOR: o_id,
                zenml.enums.StackComponentType.EXPERIMENT_TRACKER: e_id,
            },
        )
    except zenml.exceptions.StackExistsError:
        return zenml_client.get_stack(name_id_or_prefix=NEPTUNE_STACK_NAME)


@zenml.steps.step(
    experiment_tracker=NEPTUNE_EXPERIMENT_TRACKER_NAME,
    settings={"experiment_tracker.neptune": flavors.NeptuneExperimentTrackerSettings(tags={"sklearn", "digits"})},
    enable_cache=False,
)
def example_step() -> None:
    """A very minimalistic pipeline step.
    Loads a sample dataset, trains a simple classifier and logs
    a couple of metrics.
    """
    neptune_run = zenml.integrations.neptune.experiment_trackers.run_state.get_neptune_run()
    digits = load_digits()
    data = digits.images.reshape((len(digits.images), -1))

    x_train, x_test, y_train, y_test = train_test_split(data, digits.target, test_size=0.3)
    gamma = 0.001
    neptune_run["params/gamma"] = gamma
    model = SVC(gamma=gamma)
    model.fit(x_train, y_train)
    test_acc = model.score(x_test, y_test)
    neptune_run["metrics/val_accuracy"] = test_acc


@pipelines.pipeline(
    enable_cache=False,
)
def neptune_example_pipeline(ex_step):
    """
    Link all the steps artifacts together
    """
    ex_step()


@pytest.mark.integrations
@pytest.mark.zenml
class TestZenML(BaseE2ETest):
    def _test_setup_creates_stack_with_neptune_experiment_tracker(self, zenml_client):
        assert zenml_client.active_stack.experiment_tracker.name == NEPTUNE_EXPERIMENT_TRACKER_NAME

    def _test_pipeline_runs_without_errors(self):
        run = neptune_example_pipeline(ex_step=example_step())
        run.run()

        self.zenml_run_name = run.get_runs()[-1].name

    def _test_fetch_neptune_run(self, environment):
        custom_run_id = hashlib.md5(self.zenml_run_name.encode()).hexdigest()
        neptune_run = neptune.init_run(
            custom_run_id=custom_run_id, project=environment.project, api_token=environment.user_token
        )
        assert neptune_run["params/gamma"].fetch() == 0.001
        assert neptune_run["sys/tags"].fetch() == {"sklearn", "digits"}
        assert neptune_run["metrics/val_accuracy"].fetch() <= 1

    def test_zenml(self, registered_stack, zenml_client, environment):
        self._test_setup_creates_stack_with_neptune_experiment_tracker(zenml_client)
        self._test_pipeline_runs_without_errors()
        self._test_fetch_neptune_run(environment)
