import os

import pytest

import redgrease
import redgrease.client

redisgears_name = "REDISLABS_REDISGEARS"


@pytest.fixture
def server_port():
    server_port = os.getenv(f"{redisgears_name}_6379_TCP_PORT")
    return None if server_port is None else int(server_port)


@pytest.fixture
def server_hostname():
    return os.getenv(f"{redisgears_name}_HOST")


@pytest.fixture
def server(server_hostname, server_port):
    return redgrease.client.RedisGears(host=server_hostname, port=server_port)


def test_port_variable(server_port):
    print(f"Server Port: {server_port}")
    assert server_port is not None


def test_host_variable(server_hostname):
    print(f"Server Host: {server_hostname}")
    assert server_hostname is not None


def test_server_initialized(server):
    assert server is not None
    assert server.set("TESTKEY", "TESTVALUE")
    assert server.keys() == [b"TESTKEY"]
    assert server.delete("TESTKEY")
    assert not server.keys()


def test_default_builder():
    assert redgrease.GearsBuilder() is not None


def test_shorthand_builder():
    assert redgrease.GB() is not None
