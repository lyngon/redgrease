import logging

import redgrease.operation as gearop
import redgrease.sugar as sugar
from redgrease.gears import PartialGearFunction

logger = logging.getLogger(__name__)


class GearsBuilder(PartialGearFunction):
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
        reader_op = gearop.Reader(reader, defaultArg, desc)
        super().__init__(operation=reader_op, input_function=None)


GB = GearsBuilder


class atomic:
    def __init__(self):
        from redisgears import atomicCtx as redisAtomic

        self.atomic = redisAtomic()
        pass

    def __enter__(self):
        self.atomic.__enter__()
        return self

    def __exit__(self, type, value, traceback):
        self.atomic.__exit__()


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

    return redisLog(message, level=level)


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
