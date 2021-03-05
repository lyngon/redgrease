# -*- coding: utf-8 -*-
"""


Todo:
    * Use config as much as possible

"""
__author__ = "Anders Åström"
__contact__ = "anders@lyngon.com"
__copyright__ = "2021, Lyngon Pte. Ltd."
__licence__ = """The MIT License
Copyright © 2021 Lyngon Pte. Ltd.

Permission is hereby granted, free of charge, to any person obtaining a copy of this
 software and associated documentation files (the “Software”), to deal in the Software
 without restriction, including without limitation the rights to use, copy, modify,
 merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 permit persons to whom the Software is furnished to do so, subject to the following
 conditions:

The above copyright notice and this permission notice shall be included in all copies
 or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF
 CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE
 OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""

from datetime import datetime
from typing import Dict

from redis import Redis


def test_server_connection_params(server_connection_params: Dict):
    """Test that server parameters are defined, most importanty hostname and port"""
    assert server_connection_params
    assert server_connection_params.get("host")
    assert server_connection_params.get("port")


def test_basic_redis_functions(clean_redis_instance: Redis):
    """Test that the Redis test server is connected
    and responds to basic commands as expected.
    """
    key = "TESTKEY"
    value = str(datetime.utcnow())
    # Ensure the redis_instance fixture is sert
    assert clean_redis_instance is not None
    # Ensure that we can set a key in the redis_instance
    assert clean_redis_instance.set(key, value)
    # Ensure that the newly set key is set
    assert clean_redis_instance.keys(key) == [key.encode()]
    # Ensure that the newly set key has the right valued
    assert clean_redis_instance.get(key) == value.encode()
    # Ensure that there are no set keys after we delete the newly added ke
    assert clean_redis_instance.delete(key)
    # Ensure that the key is not defined
    assert not clean_redis_instance.keys(key)


def test_basic_gears_functions(clean_redis_instance: Redis):
    """Test that the Redis test server has the Gears Module loaded"""
    assert clean_redis_instance.execute_command("RG.PYEXECUTE GB().run()")
