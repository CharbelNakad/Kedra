"""Keep unit tests independent of external services by blocking network access."""

import socket

import pytest
from dns.resolver import Resolver


@pytest.fixture(autouse=True)
def no_network(monkeypatch, request):
    if request.node.get_closest_marker("storage") and request.config.getoption("--storage"):
        return
    if request.node.get_closest_marker("live_discovery") and request.config.getoption(
        "--live-discovery"
    ):
        return

    def reject(*args, **kwargs):
        raise AssertionError("Unit tests must not access the network")

    monkeypatch.setattr(socket, "create_connection", reject)
    monkeypatch.setattr(socket, "getaddrinfo", reject)
    monkeypatch.setattr(socket.socket, "connect", reject)
    monkeypatch.setattr(socket.socket, "connect_ex", reject)
    monkeypatch.setattr(socket.socket, "sendto", reject)
    monkeypatch.setattr(Resolver, "resolve", reject)


def pytest_addoption(parser):
    parser.addoption("--storage", action="store_true", help="Enable explicit local storage checks")
    parser.addoption(
        "--live-discovery",
        action="store_true",
        help="Enable one bounded WRC discovery check",
    )


def pytest_collection_modifyitems(config, items):
    if not config.getoption("--storage"):
        for item in items:
            if item.get_closest_marker("storage"):
                item.add_marker(
                    pytest.mark.skip(reason="Use --storage for local Docker integration checks")
                )
    if not config.getoption("--live-discovery"):
        for item in items:
            if item.get_closest_marker("live_discovery"):
                item.add_marker(
                    pytest.mark.skip(reason="Use --live-discovery for the bounded WRC check")
                )


@pytest.fixture
def example_env():
    # Synthetic values only; no real credentials or environment reads in fixtures.
    return {
        "KEDRA_MONGO_URI": "mongodb://fixture-user:fixture-password@localhost:27017",
        "KEDRA_S3_ACCESS_KEY_ID": "fixture-access",
        "KEDRA_S3_SECRET_ACCESS_KEY": "fixture-secret",
    }
