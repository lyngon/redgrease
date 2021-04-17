# -*- coding: utf-8 -*-
"""
Serverside Redis and Redis Gears command
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

import functools
import types

import redgrease.client
import redgrease.runtime


def _runtime_execute_command(self, command_name, *args, **options):
    """Override replacement method for `redis.Redis.execute_command` in
    Redis Geear server runtime.

    Args:
        command_name (str):
            Name of command to execute

        *args (Any):
            Command arguments

        **options (Any):
            Result parsing options (keyword args)

    Returns:
        bytes:
            Command response
    """
    response = redgrease.runtime.execute(command_name, *args)
    if command_name in self.response_callbacks:
        return self.response_callbacks[command_name](response, **options)
    return response


@functools.lru_cache()
def get_runtime_client():
    """Creates a Redis client for the Redis server runtime environment

    Returns:
        redis.Redis:
            Redis client
    """
    # By setting the connection pool to Elipsis, the client is 'tricked' to believe it
    # alraeady has a set of connections.
    # Swapping out the `execute_command` method for a replacement that uses the global
    # `execute` internally instead of the connection pool.

    runtime_client = redgrease.client.Redis(connection_pool=...)

    runtime_client.execute_command = types.MethodType(
        _runtime_execute_command, runtime_client
    )
    return runtime_client


cmd = get_runtime_client()  # noqa: F811 - Used as exported var
""" Runtime / serverside redis client with gears features"""

__all__ = ["cmd"]
