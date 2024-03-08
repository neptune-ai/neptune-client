import logging

from neptune.internal.utils.logger import (
    NEPTUNE_LOGGER_NAME,
    NEPTUNE_NO_PREFIX_LOGGER_NAME,
)


def pytest_sessionstart(session):
    logging.getLogger(NEPTUNE_LOGGER_NAME).setLevel(logging.DEBUG)
    logging.getLogger(NEPTUNE_NO_PREFIX_LOGGER_NAME).setLevel(logging.DEBUG)
