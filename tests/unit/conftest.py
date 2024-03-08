import logging

import pytest

from neptune.internal.utils.logger import (
    NEPTUNE_LOGGER_NAME,
    NEPTUNE_NO_PREFIX_LOGGER_NAME,
)


@pytest.fixture(autouse=True)
def set_logging_level():
    logging.getLogger(NEPTUNE_LOGGER_NAME).setLevel(logging.DEBUG)
    logging.getLogger(NEPTUNE_NO_PREFIX_LOGGER_NAME).setLevel(logging.DEBUG)
