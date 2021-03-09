# -*- coding: utf-8 -*-
"""
Redgrease's (overloaded) variants of the symbols loaded per default into the top level
namespace of Gear functions in the Redis server Python runtime.
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


import redgrease.gears
import redgrease.sugar as sugar


class GearsBuilder(redgrease.gears.PartialGearFunction):
    """The GearsBuilder class is imported to the runtime's environment by default.

    It exposes the functionality of the function's context builder.
    """

    def __init__(
        self,
        reader: str = sugar.ReaderType.KeysReader,
        defaultArg: str = "*",
        desc: str = None,
        *args,
        **kwargs,
    ):
        """Gear function / process factory

        Args:
            reader (str, optional):
                Input records reader
                Defining where the input to the gear will come from.
                One of::
                    - 'KeysReader'
                    - 'KeysOnlyReader'
                    - 'StreamReader'
                    - 'PythonReader'
                    - 'ShardsReader'
                    - 'CommandReader'
                Defaults to 'KeysReader'.

            defaultArg (str, optional):
                Additional arguments to the reader.
                These are usually a key's name, prefix, glob-like or a regular
                expression.
                Its use depends on the function's reader type and action.
                Defaults to '*'.

            desc (str, optional):
                An optional description.
                Defaults to None.
        """
        reader_op = redgrease.gears.Reader(reader, defaultArg, desc, *args, **kwargs)
        super().__init__(
            operation=reader_op,
            input_function=None,
        )


GB = GearsBuilder
"""Convnenience shorthand for GearsBuilder."""


# # Suppress warnings for missing redisgears packagae
# # As this package only lives on the Redis Gears server
# pyright: reportMissingImports=false
class atomic:
    """The atomic() Python context is imported to the runtime's environment by default.

    The context ensures that all operations in it are executed atomically by blocking
    he main Redis process.
    """

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
        command (str):
            The commant to execute

    Returns:
        bytes:
            Raw command response
    """
    from redisgears import executeCommand as redisExecute

    return redisExecute(command, *args)


def hashtag() -> bytes:
    """Returns a hashtag that maps to the lowest hash slot served by the local
    engine's shard. Put differently, it is useful as a hashtag for partitioning
    in a cluster.

    Returns:
        str:
            A hastag that maps to the lowest hash slot served by the local engine.
    """
    from redisgears import getMyHashTag as redisHashtag

    return redisHashtag()


def log(message: str, level: str = sugar.LogLevel.Notice):
    """Print a message to Redis' log.

    Args:
        message (str):
            The message to output

        level (str, optional):
            Message loglevel. Either::

                - 'debug'
                - 'verbose'
                - 'notice'
                - 'wartning'

            Defaults to 'notice'.
    """
    from redisgears import log as redisLog

    return redisLog(str(message), level=level)


def configGet(key: str) -> bytes:
    """Fetches the current value of a RedisGears configuration option.

    Args:
        key (str):
            The configuration option key
    """
    from __main__ import configGet as redisConfigGet

    return redisConfigGet(key)


def gearsConfigGet(key: str, default=None) -> bytes:
    """Fetches the current value of a RedisGears configuration option and returns a
    default value if that key does not exist.

    Args:
        key (str):
            The configuration option key.

        default ([type], optional):
            A default value.
            Defaults to None.
    """
    from __main__ import gearsConfigGet as redisGearsConfigGet

    return redisGearsConfigGet(key)


def run(
    function: redgrease.gears.GearFunction,
    builder: GearsBuilder,
):
    """Transforms a RedGrease GearFunction into a native Gears function.

    Note: This function is specific to RedGrease and is NOT a standard Redis Gears
    runtime function.

    Args:
        function (redgrease.gears.GearFunction):
            A GearsFunction, as created with RedGrease constructs.

        builder (GearsBuilder):
            The Redis Gears native GearsBuilder class.

    Returns:
        GearsBuilder:
            Redis Gears native GearsBuilder object, reconstructed from the input.
    """
    if function.input_function:
        input_builder = run(function.input_function, builder)
    else:
        input_builder = builder

    return function.operation.add_to(input_builder)
