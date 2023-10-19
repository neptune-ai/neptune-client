import os
from unittest.mock import patch

from neptune.common.exceptions import NeptuneException
from neptune.envs import NEPTUNE_SAMPLE_SERIES_STEPS_ERRORS
from neptune.exceptions import MetadataInconsistency
from neptune.internal.operation_processors.operations_errors_processor import OperationsErrorsProcessor


class TestOperationsErrorsProcessor:
    @patch.dict(os.environ, {NEPTUNE_SAMPLE_SERIES_STEPS_ERRORS: "True"})
    def test_sample_only_repeated_steps(self, capsys):
        processor = OperationsErrorsProcessor()
        duplicated_errors = [
            MetadataInconsistency(
                "X-coordinates (step) must be strictly increasing for series attribute: a. Invalid point: 2.0"
            ),
            MetadataInconsistency(
                "X-coordinates (step) must be strictly increasing for series attribute: b. Invalid point: 2.0"
            ),
            MetadataInconsistency(
                "X-coordinates (step) must be strictly increasing for series attribute: c. Invalid point: 2.0"
            ),
        ]

        processor.handle(errors=duplicated_errors)

        captured = capsys.readouterr()
        assert str(duplicated_errors[0]) in captured.out
        assert str(duplicated_errors[1]) not in captured.out
        assert str(duplicated_errors[2]) not in captured.out

    @patch.dict(os.environ, {NEPTUNE_SAMPLE_SERIES_STEPS_ERRORS: "True"})
    def test_not_affect_other_errors(self, capsys):
        processor = OperationsErrorsProcessor()
        duplicated_errors = list(
            [
                MetadataInconsistency("X-coordinates (step) must be strictly increasing for series attribute: a."),
                NeptuneException("General error"),
                MetadataInconsistency("X-coordinates (step) must be strictly increasing for series attribute: a."),
            ]
        )

        processor.handle(errors=duplicated_errors)

        captured = capsys.readouterr()
        assert str(duplicated_errors[0]) in captured.out
        assert str(duplicated_errors[1]) in captured.out
        assert str(duplicated_errors[2]) in captured.out

    @patch.dict(os.environ, {NEPTUNE_SAMPLE_SERIES_STEPS_ERRORS: "False"})
    def test_not_sample_when_disabled(self, capsys):
        processor = OperationsErrorsProcessor()
        duplicated_errors = [
            MetadataInconsistency(
                "X-coordinates (step) must be strictly increasing for series attribute: a. Invalid point: 2.0"
            ),
            MetadataInconsistency(
                "X-coordinates (step) must be strictly increasing for series attribute: b. Invalid point: 2.0"
            ),
            MetadataInconsistency(
                "X-coordinates (step) must be strictly increasing for series attribute: c. Invalid point: 2.0"
            ),
        ]

        processor.handle(errors=duplicated_errors)

        captured = capsys.readouterr()
        assert str(duplicated_errors[0]) in captured.out
        assert str(duplicated_errors[1]) in captured.out
        assert str(duplicated_errors[2]) in captured.out
