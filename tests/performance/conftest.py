import pytest


def pytest_addoption(parser):
    parser.addoption("--run-performance", action="store_true", default=False, help="run performance tests")


def pytest_configure(config):
    config.addinivalue_line("markers", "performance: mark test as performance to run")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--run-performance"):
        return
    skip_performance = pytest.mark.skip(reason="need --run-performance option to run")
    for item in items:
        if "performance" in item.keywords:
            item.add_marker(skip_performance)
