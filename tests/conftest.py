import os
import time
from datetime import datetime
from typing import Callable
from uuid import uuid4

import importlib_metadata
import pytest
import pytest_docker_tools as docker
import redis
import redis.exceptions
from docker import DockerClient
from docker.errors import APIError as DockerError

import redgrease
import redgrease.client

redgrease_version = os.getenv(
    "REDGREASE_VERSION", importlib_metadata.version("redgrease")
)

redgrease_runtime_repo = "lyngon/redisgears"
redgrease_runtime_image_name = f"{redgrease_runtime_repo}:{redgrease_version}"

redgrease_runtime_package = os.getenv(
    "REDGREASE_RUNTIME_PACKAGE", "redgrease[runtime]=={redgrease_version}"
)

redis_port = "6379/tcp"
redisgears_repo = os.getenv("REDISGEARS_IMAGE", "redislabs/redisgears:latest")
redisgears_image = docker.fetch(repository=redisgears_repo)

# Note: function scope significantly slows down testing
# but ensures clean environment for each test
redisgears_container = docker.container(
    image="{redisgears_image.id}", ports={redis_port: None}
)


def instantiate(client_cls: Callable[..., redis.Redis], *args, **kwargs):
    """Helper function to ensure that the SOT Redis Gears client object use the same
    instantiation logic as for the baseline Redis client object.
    """
    # Could instead use the "ready" cheeck feature of pytest-docker-tools
    instance = client_cls(*args, **kwargs)
    connected = False
    while not connected:
        try:
            connected = instance.ping()
        except redis.exceptions.ConnectionError:
            pass
        time.sleep(1)
    return instance


@pytest.fixture()
def redgrease_runtime_image(docker_client: DockerClient, redisgears_container):
    try:
        image = docker_client.images.get(redgrease_runtime_image_name)
    except DockerError:
        ip, port = redisgears_container.get_addr(redis_port)
        r = instantiate(redis.Redis, host=ip, port=port)

        # TODO: Use this approach inst
        # f"redgrease[runtime]@git+https://github.com/lyngon/redgrease.git@{branch}"

        r.execute_command(f"RG.PYEXECUTE '' REQUIREMENTS {redgrease_runtime_package}")
        redisgears_container = docker_client.containers.get(redisgears_container.id)

        redisgears_container.commit(redgrease_runtime_repo, redgrease_version)

        image = docker_client.images.get(redgrease_runtime_image_name)

    return image


redgrease_runtime_container = docker.container(
    image="{redgrease_runtime_image.id}",
    ports={redis_port: None},
)


@pytest.fixture()
def server_connection_params(redgrease_runtime_container):
    """Connection parameters, at minimum including hostname and port,
    for the Redis Gears test server
    """
    ip, port = redgrease_runtime_container.get_addr(redis_port)
    return {"host": ip, "port": port}


@pytest.fixture()
def clean_redisgears_instance(server_connection_params):
    """redgrease.client.RedisGears client instance from the redgrease package,
    connected to the test server.

    This client instance is the SOT for most of the tests.
    """
    return instantiate(
        redgrease.client.RedisGears,
        **server_connection_params,
    )


rg = clean_redisgears_instance


@pytest.fixture()
def clean_redis_instance(server_connection_params):
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
