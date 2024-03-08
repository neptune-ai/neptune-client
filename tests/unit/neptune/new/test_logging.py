import os
from contextlib import contextmanager
from functools import partial

import pytest

import neptune
from neptune.envs import NEPTUNE_LOG_LEVEL
from neptune.internal.utils.logger import get_logger
from tests.unit.neptune.new.utils.logging import format_log


@contextmanager
def assert_out(capsys: pytest.CaptureFixture, out_msg: str = "", err_msg: str = ""):
    _ = capsys.readouterr()
    yield
    captured = capsys.readouterr()
    assert out_msg == captured.out
    assert err_msg == captured.err


@pytest.fixture
def log_level_env_var_teardown() -> None:
    yield

    os.unsetenv(NEPTUNE_LOG_LEVEL)


class TestLogger:
    def test_internal_logger_loglevels(self, capsys: pytest.CaptureFixture):
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

    def test_user_can_set_logging_levels(self, capsys, log_level_env_var_teardown):
        # when
        os.environ["NEPTUNE_LOG_LEVEL"] = "CRITICAL"

        # then
        with assert_out(capsys, out_msg="", err_msg=""):
            with neptune.init_run(mode="debug"):
                ...
