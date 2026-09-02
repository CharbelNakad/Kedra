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
    if request.node.get_closest_marker("local_http"):
        return

    def reject(*args, **kwargs):
        raise AssertionError("Unit tests must not access the network")

    monkeypatch.setattr(socket, "create_connection", reject)
    monkeypatch.setattr(socket, "getaddrinfo", reject)
    if request.node.get_closest_marker("local_runtime"):
        original_connect = socket.socket.connect
        original_connect_ex = socket.socket.connect_ex

        def connect_loopback_only(instance, address):
            if isinstance(address, tuple) and address[0] in ("127.0.0.1", "::1"):
                return original_connect(instance, address)
            return reject(instance, address)

        def connect_ex_loopback_only(instance, address):
            if isinstance(address, tuple) and address[0] in ("127.0.0.1", "::1"):
                return original_connect_ex(instance, address)
            return reject(instance, address)

        monkeypatch.setattr(socket.socket, "connect", connect_loopback_only)
        monkeypatch.setattr(socket.socket, "connect_ex", connect_ex_loopback_only)
    else:
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
