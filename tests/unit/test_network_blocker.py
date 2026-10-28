"""The unit-suite socket blocker (conftest.block_network) and its loopback exception."""

from __future__ import annotations

import socket

import pytest


def test_loopback_connections_stay_open():
    # asyncio's event loop builds its self-pipe with socket.socketpair(); on
    # Windows that emulation connect()s to 127.0.0.1 and must keep working.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen(5)
        port = listener.getsockname()[1]
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
            client.settimeout(5)
            client.connect(("127.0.0.1", port))
            assert client.getpeername()[1] == port
            accepted, _ = listener.accept()  # Windows enforces the backlog strictly
            accepted.close()
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
            client.settimeout(5)
            assert client.connect_ex(("127.0.0.1", port)) == 0
            accepted, _ = listener.accept()
            accepted.close()


def test_other_hosts_are_blocked():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
        with pytest.raises(OSError, match="network access is blocked"):
            client.connect(("192.0.2.1", 80))  # TEST-NET-1: never routable
        with pytest.raises(OSError, match="network access is blocked"):
            client.connect_ex(("192.0.2.1", 80))
    with pytest.raises(socket.gaierror, match="network access is blocked"):
        socket.getaddrinfo("example.com", 443)
