import os

import pytest
from _pytest.config import PytestPluginManager
from _pytest.config.argparsing import Parser

os.environ["LOCALSTACK_INTERNAL_TEST_RUN"] = "1"

pytest_plugins = [
    "localstack.testing.pytest.cloudtrail_tracking",
    "localstack.testing.pytest.fixtures",
    "localstack.testing.pytest.snapshot",
    "localstack.testing.pytest.filters",
    "localstack.testing.pytest.fixture_conflicts",
    "localstack.testing.pytest.detect_thread_leakage",
]


@pytest.hookimpl
def pytest_addoption(parser: Parser, pluginmanager: PytestPluginManager):
    parser.addoption(
        "--offline",
        action="store_true",
        default=False,
        help="test run will not have an internet connection",
    )


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "skip_offline: mark the test to be skipped when the tests are run offline "
        "(this test explicitly / semantically needs an internet connection)",
    )
    config.addinivalue_line(
        "markers",
        "only_in_docker: mark the test as running only in Docker (e.g., requires installation of system packages)",
    )
    config.addinivalue_line(
        "markers",
        "resource_heavy: mark the test as resource-heavy, e.g., downloading very large external dependencies, "
        "or requiring high amount of RAM/CPU (can be systematically sampled/optimized in the future)",
    )
    config.addinivalue_line(
        "markers",
        "aws_validated: mark the test as validated / verified against real AWS",
    )

    config.addinivalue_line(
        "markers",
        "only_localstack: mark the test as inherently incompatible with AWS, e.g. when testing localstack-specific features",
    )
    config.addinivalue_line(
        "markers",
        "should_be_aws_validated: test fails against AWS but it shouldn't. Might need refactoring, additional permissions, etc.",
    )
    config.addinivalue_line(
        "markers",
        "manual_setup_required: validated against real AWS but needs additional setup or account configuration (e.g. increased service quotas)",
    )
    config.addinivalue_line(
        "markers",
        "multiruntime: parametrize test against multiple Lambda runtimes",
    )


def pytest_collection_modifyitems(config, items):
    if not config.getoption("--offline"):
        # The tests are not executed offline, so we don't skip the tests marked to need an internet connection
        return
    skip_offline = pytest.mark.skip(
        reason="Test cannot be executed offline / in a restricted network environment. "
        "Add network connectivity and remove the --offline option when running "
        "the test."
    )

    for item in items:
        if "skip_offline" in item.keywords:
            item.add_marker(skip_offline)
