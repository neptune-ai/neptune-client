import logging
from contextlib import contextmanager
from functools import partial

import pytest

import neptune
from neptune.internal.utils.logger import (
    NEPTUNE_LOGGER_NAME,
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


@contextmanager
def preserve_logging_level(logger: logging.Logger) -> None:
    yield

    logger.setLevel(logging.DEBUG)


def test_internal_logger_loglevels(capsys: pytest.CaptureFixture):
    # given
    logger = get_logger()

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


def test_user_can_set_logging_levels(capsys: pytest.CaptureFixture):
    # given
    logger = get_logger(NEPTUNE_LOGGER_NAME)

    # when
    logger.setLevel(logging.CRITICAL)

    # then
    with preserve_logging_level(logger):
        with assert_out(capsys):
            with neptune.init_run(mode="debug"):
                ...
