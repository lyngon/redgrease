import functools
import types

import redgrease.client
import redgrease.runtime


def runtime_execute_command(self, command_name, *args, **options):
    "Execute a command in local Gear runtime and return a parsed response"
    response = redgrease.runtime.execute(command_name, *args)
    if command_name in self.response_callbacks:
        return self.response_callbacks[command_name](response, **options)
    return response


@functools.lru_cache()
def get_runtime_client():
    runtime_client = redgrease.client.RedisGears(connection_pool=...)
    runtime_client.execute_command = types.MethodType(
        runtime_execute_command, runtime_client
    )
    return runtime_client


redis = get_runtime_client()  # noqa: F811 - Used as exported var
