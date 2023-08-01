from __future__ import annotations

import logging

import pytest

from localstack.utils.bootstrap import LocalstackContainer
from localstack.utils.container_utils.container_client import PortMappings

LOG = logging.getLogger(__name__)


class ContainerFactory:
    def __init__(self):
        self._containers: list[LocalstackContainer] = []

    def __call__(
        self,
        # convenience properties
        pro: bool = False,
        publish: list[int] | None = None,
        # ContainerConfig properties
        **kwargs,
    ) -> LocalstackContainer:
        container = LocalstackContainer()

        # override some default configuration
        container.config.ports = PortMappings()

        # allow for randomised container names
        container.config.name = None

        for key, value in kwargs.items():
            setattr(container.config, key, value)

        # handle the convenience options
        if pro:
            container.config.env_vars["GATEWAY_LISTEN"] = "0.0.0.0:4566,0.0.0.0:443"
            container.config.env_vars["LOCALSTACK_API_KEY"] = "test"

        port_mappings = PortMappings()
        if publish:
            for port in publish:
                port_mappings.add(port)
        container.config.ports = port_mappings

        # track the container so we can remove it later
        self._containers.append(container)
        return container

    def remove_all_containers(self):
        failures = []
        for container in self._containers:
            if not container.running_container:
                # container is not running
                continue

            try:
                container.running_container.shutdown()
            except Exception as e:
                failures.append((container, e))

        if failures:
            for container, ex in failures:
                LOG.error(
                    f"Failed to remove container {container.running_container.id}",
                    exc_info=LOG.isEnabledFor(logging.DEBUG),
                )


@pytest.fixture(scope="session")
def container_factory() -> ContainerFactory:
    factory = ContainerFactory()
    yield factory
    factory.remove_all_containers()
