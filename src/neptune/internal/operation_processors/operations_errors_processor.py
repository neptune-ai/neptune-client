import os
import re
from typing import (
    List,
    Set,
)

from neptune.common.exceptions import NeptuneException
from neptune.envs import NEPTUNE_SAMPLE_SERIES_STEPS_ERRORS
from neptune.exceptions import MetadataInconsistency
from neptune.internal.utils.logger import logger


class OperationsErrorsProcessor:
    def __init__(self) -> None:
        self._sampling_enabled = os.getenv(NEPTUNE_SAMPLE_SERIES_STEPS_ERRORS, "false").lower() in ("true", "1", "t")
        self._error_sampling_exp = re.compile(
            r"X-coordinates \(step\) must be strictly increasing for series attribute: (.*)\. Invalid point: (.*)"
        )
        self._logged_steps: Set[str] = set()

    def handle(self, errors: List[NeptuneException]) -> None:
        for error in errors:
            if self._sampling_enabled and isinstance(error, MetadataInconsistency):
                match_exp = self._error_sampling_exp.match(str(error))
                if match_exp:
                    self._handle_not_increased_error_for_step(error, match_exp.group(2))
                    continue

            logger.error("Error occurred during asynchronous operation processing: %s", str(error))

    def _handle_not_increased_error_for_step(self, error: MetadataInconsistency, step: str) -> None:
        if step not in self._logged_steps:
            self._logged_steps.add(step)
            logger.error(
                f"Error occurred during asynchronous operation processing: {str(error)}. "
                + f"Suppressing other errors for step: {step}."
            )
