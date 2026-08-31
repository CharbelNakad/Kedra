"""Keep unit tests independent of external services by blocking network access."""

import socket

import pytest


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def reject(*args, **kwargs):
        raise AssertionError("Unit tests must not access the network")

    monkeypatch.setattr(socket, "create_connection", reject)
    monkeypatch.setattr(socket, "getaddrinfo", reject)
    monkeypatch.setattr(socket.socket, "connect", reject)
    monkeypatch.setattr(socket.socket, "connect_ex", reject)


@pytest.fixture
def example_env():
    # Synthetic values only; no real credentials or environment reads in fixtures.
    return {
        "KEDRA_MONGO_URI": "mongodb://fixture-user:fixture-password@localhost:27017",
        "KEDRA_S3_ACCESS_KEY_ID": "fixture-access",
        "KEDRA_S3_SECRET_ACCESS_KEY": "fixture-secret",
    }
