from datetime import datetime
from typing import Callable
from uuid import uuid4

import pytest
import redis
import redis.exceptions
from pytest_docker_tools import container, fetch

import redgrease
import redgrease.client

redis_port = "6379/tcp"

# The environment variable 'TOX_ENV_NAME' contains the name of the Tox environment,
# as specified in tox.ini
# I.e. something like '1.0.0-py38' or 'latest-pypy'
# Ideally there should be a parameterized fixture that instantiates the correct
# RedisGears image repo based on this parsing this variable, and extracting the version
# Note: current tox env naming has the version of the 'redislabs/redisgears' repo
# as the first part, without any prefix and delimited by a '-'
# (followed by other env identifiers such as python version and whatnot)
redisgears_repo = "redislabs/redisgears:latest"

redisgears_image = fetch(repository=redisgears_repo)

# Note: function scope significantly slows down testing
# but ensures clean environment for each test
redisgears_container = container(
    image="{redisgears_image.id}", scope="class", ports={redis_port: None}
)


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


@pytest.fixture(scope="class")
def server_connection_params(redisgears_container):
    """Connection parameters, at minimum including hostname and port,
    for the Redis Gears test server
    """
    ip, port = redisgears_container.get_addr(redis_port)
    return {"host": ip, "port": port}


@pytest.fixture(scope="class")
def rg(server_connection_params):
    """redgrease.client.RedisGears client instance from the redgrease package,
    connected to the test server.

    This client instance is the SOT for most of the tests.
    """
    return instantiate(
        redgrease.client.RedisGears,
        **server_connection_params,
    )


@pytest.fixture(scope="class")
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
