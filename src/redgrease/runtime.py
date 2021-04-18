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
 HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHsER LIABILITY, WHETHER IN AN ACTION OF
 CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE
 OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""
from typing import TYPE_CHECKING, Hashable, Iterable, TypeVar

import redgrease.gears

if TYPE_CHECKING:
    import redgrease.typing as optype

T = TypeVar("T")


class GearsBuilder(redgrease.gears.PartialGearFunction):
    """The GearsBuilder class is imported to the runtime's environment by default.

    It exposes the functionality of the function's context builder.

    Unlike Readers, the GearsBuilder mutates its function instead of creating a new one
    for each operation. This behaviour is deliberate, in order to be consistent with
    the "raw" string Gear functions.
    """

    def __init__(
        self,
        reader: str = "KeysReader",
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
        requirements = kwargs.pop("requirements", None)
        reader_op = redgrease.gears.Reader(reader, defaultArg, desc, *args, **kwargs)
        self._function: redgrease.gears.PartialGearFunction = (
            redgrease.gears.PartialGearFunction(
                operation=reader_op,
                requirements=requirements,
            )
        )

    def run(
        self,
        arg: str = None,  # TODO: This can also be a Python generator
        convertToStr: bool = True,
        collect: bool = True,
        # Helpers, all must be None
        # Other Redgrease args
        requirements: Iterable[str] = None,
        on=None,
        # Other Redis Gears args
        **kwargs,
        # TODO: Add all the Reader specific args here
    ) -> redgrease.gears.ClosedGearFunction:
        return self._function.run(
            arg=arg,
            convertToStr=convertToStr,
            collect=collect,
            requirements=requirements,
            on=on,
            **kwargs,
        )

    def register(  # noqa: C901
        self,
        prefix: str = "*",
        convertToStr: bool = True,
        collect: bool = True,
        # Helpers, all must be None
        mode: str = None,
        onRegistered: "optype.Callback" = None,
        eventTypes: Iterable[str] = None,
        keyTypes: Iterable[str] = None,
        readValue: bool = None,
        batch: int = None,
        duration: int = None,
        onFailedPolcy: str = None,
        onFailedRetryInterval: int = None,
        trimStream: bool = None,
        trigger: str = None,  # Reader Specific: CommandReader
        # Other Redgrease args
        requirements: Iterable[str] = None,
        on=None,
        # Other Redis Gears args
        **kwargs,
        # TODO: Add all the Reader specific args here
    ) -> redgrease.gears.ClosedGearFunction:
        return self._function.register(
            prefix=prefix,
            convertToStr=convertToStr,
            collect=collect,
            # Helpers
            mode=mode,
            onRegistered=onRegistered,
            eventTypes=eventTypes,
            keyTypes=keyTypes,
            readValue=readValue,
            batch=batch,
            duration=duration,
            onFailedPolcy=onFailedPolcy,
            onFailedRetryInterval=onFailedRetryInterval,
            trimStream=trimStream,
            trigger=trigger,  # CommandReader specific
            # Other Redgrease args
            requirements=requirements,
            on=on,
            # Other Redis Gears args
            **kwargs,
        )

    def map(
        self,
        op: "optype.Mapper[optype.InputRecord, optype.OutputRecord]",
        # Other Redgrease args
        requirements: Iterable[str] = None,
        # Other Redis Gears args
        **kwargs,
    ) -> "GearsBuilder":
        self._function = self._function.map(op=op, requirements=requirements, **kwargs)

        return self

    def flatmap(
        self,
        op: "optype.Expander[optype.InputRecord, optype.OutputRecord]" = None,
        # Other Redgrease args
        requirements: Iterable[str] = None,
        # Other Redis Gears args
        **kwargs,
    ) -> "GearsBuilder":
        self._function = self._function.flatmap(
            op=op, requirements=requirements, **kwargs
        )

        return self

    def foreach(
        self,
        op: "optype.Processor[optype.InputRecord]",
        # Other Redgrease args
        requirements: Iterable[str] = None,
        # Other Redis Gears args
        **kwargs,
    ) -> "GearsBuilder":
        self._function = self._function.foreach(
            op=op, requirements=requirements, **kwargs
        )

        return self

    def filter(
        self,
        op: "optype.Filterer[optype.InputRecord]" = None,
        # Other Redgrease args
        requirements: Iterable[str] = None,
        # Other Redis Gears args
        **kwargs,
    ) -> "GearsBuilder":
        self._function = self._function.filter(
            op=op, requirements=requirements, **kwargs
        )

        return self

    def accumulate(
        self,
        op: "optype.Accumulator[T, optype.InputRecord]" = None,
        # Other Redgrease args
        requirements: Iterable[str] = None,
        # Other Redis Gears args
        **kwargs,
    ) -> "GearsBuilder":
        self._function = self._function.accumulate(
            op=op, requirements=requirements, **kwargs
        )

        return self

    def localgroupby(
        self,
        extractor: "optype.Extractor[optype.InputRecord, optype.Key]" = None,
        reducer: "optype.Reducer[optype.Key, T, optype.InputRecord]" = None,
        # Other Redgrease args
        requirements: Iterable[str] = None,
        # Other Redis Gears args
        **kwargs,
    ) -> "GearsBuilder":
        if self._function:
            self._function = self._function.localgroupby(
                extractor=extractor,
                reducer=reducer,
                requirements=requirements,
                **kwargs,
            )

        return self

    def limit(
        self,
        length: int,
        start: int = 0,
        # Other Redis Gears args
        **kwargs,
    ) -> "GearsBuilder":
        self._function = self._function.limit(length=length, start=start, **kwargs)

        return self

    def collect(self, **kwargs) -> "GearsBuilder":
        if self._function:
            self._function = self._function.collect(**kwargs)

        return self

    def repartition(
        self,
        extractor: "optype.Extractor[optype.InputRecord, Hashable]",
        # Other Redgrease args
        requirements: Iterable[str] = None,
        # Other Redis Gears args
        **kwargs,
    ) -> "GearsBuilder":
        self._function = self._function.repartition(
            extractor=extractor, requirements=requirements, **kwargs
        )

        return self

    def aggregate(
        self,
        zero: T = None,
        seqOp: "optype.Accumulator[T, optype.InputRecord]" = None,
        combOp: "optype.Accumulator[T, T]" = None,
        # Other Redgrease args
        requirements: Iterable[str] = None,
        # Other Redis Gears args
        **kwargs,
    ) -> "GearsBuilder":
        self._function = self._function.aggregate(
            zero=zero,
            seqOp=seqOp,
            combOp=combOp,
            requirements=requirements,
            **kwargs,
        )

        return self

    def aggregateby(
        self,
        extractor: "optype.Extractor[optype.InputRecord, optype.Key]" = None,
        zero: T = None,
        seqOp: "optype.Reducer[optype.Key, T, optype.InputRecord]" = None,
        combOp: "optype.Reducer[optype.Key, T, T]" = None,
        # Other Redgrease args
        requirements: Iterable[str] = None,
        # Other Redis Gears args
        **kwargs,
    ) -> "GearsBuilder":
        self._function = self._function.aggregateby(
            extractor=extractor,
            zero=zero,
            seqOp=seqOp,
            combOp=combOp,
            requirements=requirements,
            **kwargs,
        )

        return self

    def groupby(
        self,
        extractor: "optype.Extractor[optype.InputRecord, optype.Key]" = None,
        reducer: "optype.Reducer[optype.Key, T, optype.InputRecord]" = None,
        # Other Redgrease args
        requirements: Iterable[str] = None,
        # Other Redis Gears args
        **kwargs,
    ) -> "GearsBuilder":
        self._function = self._function.groupby(
            extractor=extractor, reducer=reducer, requirements=requirements, **kwargs
        )

        return self

    def batchgroupby(
        self,
        extractor: "optype.Extractor[optype.InputRecord, optype.Key]" = None,
        reducer: "optype.BatchReducer[optype.Key, T, optype.InputRecord]" = None,
        # Other Redgrease args
        requirements: Iterable[str] = None,
        # Other Redis Gears args
        **kwargs,
    ) -> "GearsBuilder":
        self._function = self._function.batchgroupby(
            extractor=extractor, reducer=reducer, requirements=requirements, **kwargs
        )

        return self

    def sort(
        self,
        reverse: bool = True,
        # Other Redgrease args
        requirements: Iterable[str] = None,
        # Other Redis Gears args
        **kwargs,
    ) -> "GearsBuilder":
        self._function = self._function.sort(
            reverse=reverse, requirements=requirements, **kwargs
        )

        return self

    def distinct(self, **kwargs) -> "GearsBuilder":
        self._function = self._function.distinct(**kwargs)

        return self

    def count(self, **kwargs) -> "GearsBuilder":
        self._function = self._function.count(**kwargs)

        return self

    def countby(
        self,
        extractor: "optype.Extractor[optype.InputRecord, Hashable]" = lambda x: str(x),
        # Other Redgrease args
        requirements: Iterable[str] = None,
        # Other Redis Gears args
        **kwargs,
    ) -> "GearsBuilder":
        self._function = self._function.countby(
            extractor=extractor,
            # Other Redgrease args
            requirements=requirements,
            # Other Redis Gears args
            **kwargs,
        )

        return self

    def avg(
        self,
        extractor: "optype.Extractor[optype.InputRecord, float]" = lambda x: float(
            x if isinstance(x, (int, float, str)) else str(x)
        ),
        # Other Redgrease args
        requirements: Iterable[str] = None,
        # Other Redis Gears args
        **kwargs,
    ) -> "GearsBuilder":
        self._function = self._function.avg(
            extractor=extractor,
            # Other Redgrease args
            requirements=requirements,
            # Other Redis Gears args
            **kwargs,
        )

        return self


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


def hashtag() -> str:
    """Returns a hashtag that maps to the lowest hash slot served by the local
    engine's shard. Put differently, it is useful as a hashtag for partitioning
    in a cluster.

    Returns:
        str:
            A hastag that maps to the lowest hash slot served by the local engine.
    """
    from redisgears import getMyHashTag as redisHashtag

    return redisHashtag()


def hashtag3() -> str:
    """Provides a the same value as `hashtag`, but surrounded by curly braces.

    For example, if `hashtag()` generates "06S", then `hashtag3' gives "{06S}".

    This is useful for creating slot-specific keys using f-strings,
    inside gear functions, as the braces are already escaped. Example:

    .. code-block:: python

        redgrease.cmd.set(f"{hastag}",  some_value)

    Returns:
        str:
            A braces-enclosed hashtag string
    """
    return f"{{{hashtag()}}}"


def log(message: str, level: str = "notice"):
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


def configGet(key: str) -> str:
    """Fetches the current value of a RedisGears configuration option.

    Args:
        key (str):
            The configuration option key
    """
    from __main__ import configGet as redisConfigGet

    return redisConfigGet(key)


def gearsConfigGet(key: str, default=None) -> str:
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
