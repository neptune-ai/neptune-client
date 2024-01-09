import logging
import pytest
from neptune.internal.utils.logger import LOGGER_NAME, ROOT_LOGGER_NAME, CommonPrefixLogger
from contextlib import contextmanager


@contextmanager
def assert_stdout(capsys: pytest.CaptureFixture, msg: str):
    _ = capsys.readouterr()
    yield
    captured = capsys.readouterr()
    assert msg in captured.out


def test_logger_default_handler_stdout_format(capsys: pytest.CaptureFixture):
    # given
    local_logger_name = 'local-logger'
    logger = logging.getLogger(local_logger_name)

    # then
    expected_log_output = f"{LOGGER_NAME}:{local_logger_name} message\n"
    with assert_stdout(capsys, expected_log_output):
        logger.info("message")

def test_logger_is_correct_instance():
    # given
    logger = logging.getLogger("local-logger")

    # then
    assert isinstance(logger, CommonPrefixLogger)
    assert logger.name == "local-logger"


def test_root_logger_is_correct_instance():
    # given
    logger = logging.getLogger()

    # then
    assert isinstance(logger, CommonPrefixLogger)
    assert logger.name == ROOT_LOGGER_NAME
