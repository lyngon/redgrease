import types
from functools import lru_cache

import redgrease
from redgrease.client import RedisGears


def runtime_execute_command(self, *args, **options):
    "Execute a command in local Gear runtime and return a parsed response"
    command_name = args[0]
    response = redgrease.execute(*args)
    if command_name in self.response_callbacks:
        return self.response_callbacks[command_name](response, **options)
    return response


@lru_cache()
def get_runtime_client():
    runtime_client = RedisGears(connection_pool=...)
    runtime_client.execute_command = types.MethodType(
        runtime_execute_command, runtime_client
    )
    return runtime_client


redis = get_runtime_client()  # noqa: F811 - Used as exported var
