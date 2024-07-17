import logging
from contextlib import contextmanager
from functools import partial

import pytest

import neptune
from neptune.internal.utils.logger import (
    NEPTUNE_LOGGER_NAME,
    get_disabled_logger,
    get_logger,
)
from tests.unit.neptune.new.utils.logging import format_log


@contextmanager
def assert_out(capsys: pytest.CaptureFixture, out_msg: str = "", err_msg: str = ""):
    _ = capsys.readouterr()
    yield
    captured = capsys.readouterr()
    assert out_msg == captured.out
    assert err_msg == captured.err


@pytest.fixture
def log_level_teardown() -> None:
    yield

    logger = logging.getLogger(NEPTUNE_LOGGER_NAME)
    logger.setLevel(logging.INFO)


class TestLogger:
    def test_internal_logger_loglevels(self, capsys: pytest.CaptureFixture, log_level_teardown):
        # given
        logger = get_logger()
        logger.setLevel(logging.DEBUG)

        # when
        _log = partial(format_log, msg="message\n")

        # then
        with assert_out(capsys, _log("DEBUG")):
            logger.debug("message")

        with assert_out(capsys, _log("INFO")):
            logger.info("message")

        with assert_out(capsys, _log("WARNING")):
            logger.warning("message")

        with assert_out(capsys, _log("ERROR")):
            logger.error("message")

        with assert_out(capsys, _log("CRITICAL")):
            logger.critical("message")

    def test_user_can_set_logging_levels(self, capsys, log_level_teardown):
        # given
        logger = logging.getLogger(NEPTUNE_LOGGER_NAME)

        # when
        logger.setLevel(logging.ERROR)

        # then
        with assert_out(capsys, out_msg="", err_msg=""):
            with neptune.init_run(mode="disabled"):
                ...

    def test_disabled_logger(self, capsys):
        # given
        logger = get_disabled_logger()

        # then
        with assert_out(capsys, out_msg="", err_msg=""):
            logger.debug("message")
            logger.info("message")
            logger.warning("message")
            logger.error("message")
