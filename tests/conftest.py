import os
from datetime import datetime
from typing import Callable
from uuid import uuid4

import pytest
import redis
import redis.exceptions

import redgrease
import redgrease.client

redisgears_name = "REDISLABS_REDISGEARS"


def instantiate(client_cls: Callable[..., redis.Redis], *args, **kwargs):
    """Helper function to ensure that the SOT Redis Gears client object use the same
    instantiation logic as for the baseline Redis client object.
    """
    instance = client_cls(*args, **kwargs)
    connected = False
    while not connected:
        try:
            connected = instance.ping()
        except redis.exceptions.ConnectionError:
            pass
    return instance


@pytest.fixture(scope="session")
def server_connection_params():
    """Connection parameters, at minimum including hostname and port,
    for the Redis Gears test server
    """
    return {
        "host": os.environ.get(f"{redisgears_name}_HOST", "localhost"),
        "port": os.environ.get(f"{redisgears_name}_6379_TCP_PORT", 6379),
    }


@pytest.fixture(scope="session")
def rg(server_connection_params):
    """redgrease.client.RedisGears client instance from the redgrease package,
    connected to the test server.

    This client instance is the SOT for most of the tests.
    """
    return instantiate(
        redgrease.client.RedisGears,
        **server_connection_params,
    )


@pytest.fixture(scope="session")
def r(server_connection_params):
    """Vanilla redis.Redis() client instance, from the redis-py package,
    connected to the test server.

    Considered valid / correctly implemented baseline for any and all non-gears
    operations.
    """
    return instantiate(
        redis.Redis,
        **server_connection_params,
    )


@pytest.fixture(scope="session")
def testrun_timestamp():
    return datetime.utcnow()


@pytest.fixture()
def var(request, testrun_timestamp):
    ts = testrun_timestamp.strftime("%Y-%m-%dT%H:%M:%S")
    base = f"testrun@{ts}::redgrease/{request.node.nodeid}::"

    def var_generator(*names, base=base):

        if not names:
            name = ""
        elif names is ...:
            name = uuid4().hex
        else:
            name = "/".join(names)

        if not base:
            base = ""

        return f"{base}{name}"

    return var_generator
