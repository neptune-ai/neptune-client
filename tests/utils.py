__all__ = [
    "catch_time",
    "modified_environ",
    "preserve_cwd",
    "tmp_context",
]

import os
import tempfile
from contextlib import contextmanager
from time import perf_counter


# from https://stackoverflow.com/a/62956469
@contextmanager
def catch_time() -> float:
    start = perf_counter()
    yield lambda: perf_counter() - start


# from https://stackoverflow.com/a/34333710
@contextmanager
def modified_environ(*remove, **update):
    """
    Temporarily updates the ``os.environ`` dictionary in-place.

    The ``os.environ`` dictionary is updated in-place so that the modification
    is sure to work in all situations.

    :param remove: Environment variables to remove.
    :param update: Dictionary of environment variables and values to add/update.
    """
    env = os.environ
    update = update or {}
    remove = remove or []

    # List of environment variables being updated or removed.
    stomped = (set(update.keys()) | set(remove)) & set(env.keys())
    # Environment variables and values to restore on exit.
    update_after = {k: env[k] for k in stomped}
    # Environment variables and values to remove on exit.
    remove_after = frozenset(k for k in update if k not in env)

    try:
        env.update(update)
        for k in remove:
            env.pop(k, None)
        yield
    finally:
        env.update(update_after)
        for k in remove_after:
            env.pop(k)


@contextmanager
def preserve_cwd(path):
    cwd = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(cwd)


@contextmanager
def tmp_context():
    with tempfile.TemporaryDirectory() as tmp:
        with preserve_cwd(tmp):
            yield tmp
