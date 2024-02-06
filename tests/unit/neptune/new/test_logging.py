from contextlib import contextmanager
from functools import partial

import pytest

from neptune.internal.utils.logger import get_logger
from tests.unit.neptune.new.utils.logging import format_log


@contextmanager
def assert_out(capsys: pytest.CaptureFixture, out_msg: str = "", err_msg: str = ""):
    _ = capsys.readouterr()
    yield
    captured = capsys.readouterr()
    assert out_msg == captured.out
    assert err_msg == captured.err


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
