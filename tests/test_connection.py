from datetime import datetime
from typing import Dict

from redis import Redis


def test_server_connection_params(server_connection_params: Dict):
    """Test that server parameters are defined, most importanty hostname and port"""
    assert server_connection_params
    assert server_connection_params.get("host")
    assert server_connection_params.get("port")


def test_basic_redis_functions(r: Redis):
    """Test that the Redis test server is connected
    and responds to basic commands as expected.
    """
    key = "TESTKEY"
    value = str(datetime.utcnow())
    # Ensure the redis_instance fixture is sert
    assert r is not None
    # Ensure that we can set a key in the redis_instance
    assert r.set(key, value)
    # Ensure that the newly set key is set
    assert r.keys(key) == [key.encode()]
    # Ensure that the newly set key has the right valued
    assert r.get(key) == value.encode()
    # Ensure that there are no set keys after we delete the newly added ke
    assert r.delete(key)
    # Ensure that the key is not defined
    assert not r.keys(key)


def test_basic_gears_functions(r: Redis):
    """Test that the Redis test server has the Gears Module loaded"""
    assert r.execute_command("RG.PYEXECUTE GB().run()")
