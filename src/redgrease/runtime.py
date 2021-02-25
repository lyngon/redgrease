import logging

import redgrease.gears
import redgrease.sugar as sugar

logger = logging.getLogger(__name__)


class GearsBuilder(redgrease.gears.PartialGearFunction):
    def __init__(
        self,
        reader: str = sugar.Reader.KeysReader,
        defaultArg: str = "*",
        desc: str = None,
    ):
        """Gear function / process factory
        Args:
            reader (str, optional): Input records reader
                Defining where the input to the gear will come from.
                One of:
                - 'KeysReader':
                - 'KeysOnlyReader':
                - 'StreamReader':
                - 'PythonReader':
                - 'ShardsReader':
                - 'CommandReader':
                Defaults to 'KeysReader'.

            defaultArg (str, optional):
                Additional arguments to the reader.
                These are usually a key's name, prefix, glob-like
                or a regular expression.
                Its use depends on the function's reader type and action.
                Defaults to '*'.

            desc (str, optional): An optional description.
                Defaults to None.
        """
        reader_op = redgrease.gears.Reader(reader, defaultArg, desc)
        super().__init__(operation=reader_op, input_function=None)


GB = GearsBuilder


# # Suppress warnings for missing redisgears packagae
# # As this package only lives on the Redis Gears server
# pyright: reportMissingImports=false
class atomic:
    def __init__(self):
        from redisgears import atomicCtx as redisAtomic

        self.atomic = redisAtomic()

    def __enter__(self):
        self.atomic.__enter__()
        return self

    def __exit__(self, *args, **kwargs):
        self.atomic.__exit__(*args, **kwargs)


def execute(command: str, *args) -> bytes:
    """Execute an arbitrary Redis command.

    Args:
        command (str): The commant to execute

    Returns:
        bytes: Raw command response
    """
    from redisgears import executeCommand as redisExecute

    return redisExecute(command, *args)


def hashtag() -> bytes:
    """Returns a hashtag that maps to the lowest hash slot served by the local
    engine's shard. Put differently, it is useful as a hashtag for partitioning
    in a cluster.

    Returns:
        str: [description]
    """
    from redisgears import getMyHashTag as redisHashtag

    return redisHashtag()


def log(message: str, level: str = sugar.LogLevel.Notice):
    """Print a message to Redis' log.

    Args:
        message (str): The message to output
        level (str, optional): Message loglevel. Either:
        'debug', 'verbose', 'notice' or 'wartning'
            Defaults to 'notice'.
    """
    from redisgears import log as redisLog

    return redisLog(str(message), level=level)


def configGet(key: str) -> bytes:
    """Fetches the current value of a RedisGears configuration option.

    Args:
        key (str): The configuration option key
    """
    from __main__ import configGet as redisConfigGet

    return redisConfigGet(key)


def gearsConfigGet(key: str, default=None) -> bytes:
    """Fetches the current value of a RedisGears configuration option and returns a
    default value if that key does not exist.

    Args:
        key (str): The configuration option key.
        default ([type], optional): A default value.
            Defaults to None.
    """
    from __main__ import gearsConfigGet as redisGearsConfigGet

    return redisGearsConfigGet(key)


def run(
    function: redgrease.gears.GearFunction,
    builder: GearsBuilder,
):
    if function.input_function:
        input_builder = run(function.input_function, builder)
    else:
        input_builder = builder

    return function.operation.add_to(input_builder)
