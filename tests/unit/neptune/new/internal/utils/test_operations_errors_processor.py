from unittest.mock import MagicMock

from neptune.common.exceptions import NeptuneException
from neptune.exceptions import MetadataInconsistency
from neptune.internal.operation_processors.operations_errors_processor import OperationsErrorsProcessor


class TestOperationsErrorsProcessor:
    def test_suppressing_only_repeated_steps(self):
        logger = MagicMock()
        processor = OperationsErrorsProcessor(logger)
        duplicated_errors = list(
            [
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
        )

        processor.handle(errors=duplicated_errors)

        assert logger.error.call_count == 1

    def test_not_affect_other_errors(self):
        logger = MagicMock()
        processor = OperationsErrorsProcessor(logger)
        duplicated_errors = list(
            [
                MetadataInconsistency("X-coordinates (step) must be strictly increasing for series attribute: a."),
                NeptuneException("General error"),
                MetadataInconsistency("X-coordinates (step) must be strictly increasing for series attribute: a."),
            ]
        )

        processor.handle(errors=duplicated_errors)

        assert logger.error.call_count == 3
