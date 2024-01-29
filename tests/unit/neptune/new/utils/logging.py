from neptune.internal.utils.logger import NEPTUNE_LOGGER_NAME


def format_log(loglevel: str, msg: str) -> str:
    return f"[{NEPTUNE_LOGGER_NAME}][{loglevel}] {msg}"
