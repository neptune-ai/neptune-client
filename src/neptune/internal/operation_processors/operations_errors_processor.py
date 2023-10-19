import re
from logging import Logger

from neptune.common.exceptions import NeptuneException
from neptune.exceptions import MetadataInconsistency


class OperationsErrorsProcessor:
    def __init__(self, logger: Logger):
        self._logger = logger
        self._error_sampling_exp = re.compile(
            r"X-coordinates \(step\) must be strictly increasing for series attribute: (.*)\. Invalid point: (.*)"
        )
        self._logged_steps = set[str]()

    def handle(self, errors: list[NeptuneException]) -> None:
        for error in errors:
            if isinstance(error, MetadataInconsistency):
                match_exp = self._error_sampling_exp.match(str(error))
                if match_exp:
                    self._handle_not_increased_error_for_step(error, match_exp.group(2))
                    continue

            self._logger.error("Error occurred during asynchronous operation processing: %s", str(error))

    def _handle_not_increased_error_for_step(self, error: MetadataInconsistency, step: str) -> None:
        if step not in self._logged_steps:
            self._logged_steps.add(step)
            self._logger.error(
                f"Error occurred during asynchronous operation processing: {str(error)}. "
                + f"Suppressing other errors for step: {step}."
            )
