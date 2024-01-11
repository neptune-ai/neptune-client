from contextlib import contextmanager

import pytest

from neptune.internal.utils.logger import (
    NEPTUNE_LOGGER_NAME,
    get_logger,
)


@contextmanager
def assert_out(capsys: pytest.CaptureFixture, out_msg: str = "", err_msg: str = ""):
    _ = capsys.readouterr()
    yield
    captured = capsys.readouterr()
    assert out_msg in captured.out
    assert err_msg in captured.err


def test_interal_logger_default_handler_stdout_format(capsys: pytest.CaptureFixture):
    # given
    logger = get_logger()

    # then
    expected_log_std_out = "message\n"
    with assert_out(capsys, expected_log_std_out):
        logger.info("message")

    assert logger.name == NEPTUNE_LOGGER_NAME


def test_internal_logger_loglevels(capsys: pytest.CaptureFixture):
    # given
    logger = get_logger()

    # then
    msg = "message"
    expected_log_std_out = f"{msg}\n"
    with assert_out(capsys, expected_log_std_out):
        logger.debug("message")

    with assert_out(capsys, expected_log_std_out):
        logger.info("message")

    with assert_out(capsys, expected_log_std_out):
        logger.warning("message")

    with assert_out(capsys, expected_log_std_out):
        logger.error("message")

    with assert_out(capsys, expected_log_std_out):
        logger.critical("message")
