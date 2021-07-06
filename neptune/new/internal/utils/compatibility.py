from neptune.experiments import Experiment
from neptune.new import Run
from neptune.new.exceptions import NeptuneLegacyIncompatibilityException


def expect_not_an_experiment(run: 'Run'):
    if isinstance(run, Experiment):
        raise NeptuneLegacyIncompatibilityException()
