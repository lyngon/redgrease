# -*- coding: utf-8 -*-
# from __future__ import annotations

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
from typing import Hashable, Iterable, TypeVar

import redgrease.gears

# if TYPE_CHECKING:
from redgrease.typing import (
    Accumulator,
    BatchReducer,
    Expander,
    Extractor,
    Filterer,
    InputRecord,
    Key,
    Mapper,
    OutputRecord,
    Processor,
    Reducer,
    Registrator,
)

T = TypeVar("T")


class GearsBuilder(redgrease.gears.OpenGearFunction):
    """The RedisGears :ref:`GearsBuilder` class is imported to the runtime's
    environment by default, and this class is a RedGrease wrapper of it.

    It exposes the functionality of the function's `context builder
    <https://oss.redislabs.com/redisgears/1.0/functions.html#context-builder>`_.

    .. warning::

        GearsBuilder is mutable with respect to the operations.

        The :class:`.GearsBuilder` is a subclass of :class:`.gears.OpenGearFunction`,
        but unlike other OpenGearFunctions, the GearsBuilder mutates an internal
        GearFunction instead of creating a new one for each operation.
        This behavior is deliberate, in order to be consistent with the original
        GearsBuilder.
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
                One of:

                    - ``"KeysReader"``
                    - ``"KeysOnlyReader"``
                    - ``"StreamReader"``
                    - ``"PythonReader"``
                    - ``"ShardsReader"``
                    - ``"CommandReader"``

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
        self._function: redgrease.gears.OpenGearFunction = (
            redgrease.gears.OpenGearFunction(
                operation=reader_op,
                requirements=requirements,
            )
        )

    @property
    def gearfunction(self):
        """The "open" GearFunction object at this step in the pipeline.

        This GearFunction is itself immutable but can be built upon to create new
        GearFunctions, independently from the GearsBuilder.

        Returns:
            redgrease.gears.OpenGearFunction:
                The current GearFunction object.
        """
        return self._function

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

    def register(
        self,
        prefix: str = "*",
        convertToStr: bool = True,
        collect: bool = True,
        # Helpers, all must be None
        mode: str = None,
        onRegistered: Registrator = None,
        eventTypes: Iterable[str] = None,
        keyTypes: Iterable[str] = None,
        readValue: bool = None,
        batch: int = None,
        duration: int = None,
        onFailedPolicy: str = None,
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
            onFailedPolicy=onFailedPolicy,
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
        op: Mapper[InputRecord, OutputRecord],
        # Other Redgrease args
        requirements: Iterable[str] = None,
        # Other Redis Gears args
        **kwargs,
    ) -> "GearsBuilder":
        """Instance-local :ref:`op_map` operation that performs a one-to-one (1:1) mapping of
        records.

        Args:
            op :data:`redgrease.typing.Mapper`):
                Function to map on the input records.
                The function must take one argument as input (input record) and
                return something as an output (output record).

            requirements (Iterable[str], optional):
                Additional requirements / dependency Python packages.
                Defaults to None.

            **kwargs:
                Additional parameters to the Map operation.

        Returns:
            GearsBuilder:
                Itself, i.e. the same GearsBuilder, but with its internal gear function
                updated with a Map operation as last step.
        """
        self._function = self._function.map(op=op, requirements=requirements, **kwargs)

        return self

    def flatmap(
        self,
        op: Expander[InputRecord, OutputRecord] = None,
        # Other Redgrease args
        requirements: Iterable[str] = None,
        # Other Redis Gears args
        **kwargs,
    ) -> "GearsBuilder":
        """Instance-local :ref:`op_flatmap` operation that performs one-to-many (1:N) mapping
        of records.

        Args:
            op :data:`redgrease.typing.Expander`, optional):
                Function to map on the input records.
                The function must take one argument as input (input record) and
                return an iterable as an output (output records).
                Defaults to the 'identity-function', I.e. if input is an iterable will
                be expanded.

            requirements (Iterable[str], optional):
                Additional requirements / dependency Python packages.
                Defaults to None.

            **kwargs:
                Additional parameters to the FlatMap operation.

        Returns:
            GearsBuilder:
                Itself, i.e. the same GearsBuilder, but with its internal gear function
                updated with a FlatMap operation as last step.
        """
        self._function = self._function.flatmap(
            op=op, requirements=requirements, **kwargs
        )

        return self

    def foreach(
        self,
        op: Processor[InputRecord],
        # Other Redgrease args
        requirements: Iterable[str] = None,
        # Other Redis Gears args
        **kwargs,
    ) -> "GearsBuilder":
        """Instance-local :ref:`op_foreach` operation performs one-to-the-same (1=1) mapping.

        Args:
            op :data:`redgrease.typing.Processor`):
                Function to run on each of the input records.
                The function must take one argument as input (input record) and
                should not return anything.

            requirements (Iterable[str], optional):
                Additional requirements / dependency Python packages.
                Defaults to None.

            **kwargs:
                Additional parameters to the ForEach operation.

        Returns:
            GearsBuilder:
                Itself, i.e. the same GearsBuilder, but with its internal gear function
                updated with a ForEach operation as last step.
        """
        self._function = self._function.foreach(
            op=op, requirements=requirements, **kwargs
        )

        return self

    def filter(
        self,
        op: Filterer[InputRecord] = None,
        # Other Redgrease args
        requirements: Iterable[str] = None,
        # Other Redis Gears args
        **kwargs,
    ) -> "GearsBuilder":
        """Instance-local :ref:`op_filter` operation performs one-to-zero-or-one (1:bool)
        filtering of records.

        Args:
            op :data:`redgrease.typing.Filterer`, optional):
                Function to apply on the input records, to decide which ones to keep.
                The function must take one argument as input (input record) and
                return a bool. The input records evaluated to `True` will be kept as
                output records.
                Defaults to the 'identity-function', i.e. records are filtered based on
                their own trueess or falseness.

            requirements (Iterable[str], optional):
                Additional requirements / dependency Python packages.
                Defaults to None.

            **kwargs:
                Additional parameters to the Filter operation.

        Returns:
            GearsBuilder:
                Itself, i.e. the same GearsBuilder, but with its internal gear function
                updated with a FIlter operation as last step.
        """
        self._function = self._function.filter(
            op=op, requirements=requirements, **kwargs
        )

        return self

    def accumulate(
        self,
        op: Accumulator[T, InputRecord] = None,
        # Other Redgrease args
        requirements: Iterable[str] = None,
        # Other Redis Gears args
        **kwargs,
    ) -> "GearsBuilder":
        """Instance-local :ref:`op_accumulate` operation performs many-to-one mapping (N:1) of
        records.

        Args:
            op :data:`redgrease.typing.Accumulator`, optional):
                Function to to apply on the input records.
                The function must take two arguments as input:
                    - An accumulator value, and
                    - The input record.
                It should aggregate the input record into the accumulator variable,
                which stores the state between the function's invocations.
                The function must return the accumulator's updated value.
                Defaults to a list accumulator, I.e. the output will be a list of
                all inputs.

            requirements (Iterable[str], optional):
                Additional requirements / dependency Python packages.
                Defaults to None.

            **kwargs:
                Additional parameters to the Accumulate operation.

        Returns:
            GearsBuilder:
                Itself, i.e. the same GearsBuilder, but with its internal gear function
                updated with Accumulate operation as last step.
        """
        self._function = self._function.accumulate(
            op=op, requirements=requirements, **kwargs
        )

        return self

    def localgroupby(
        self,
        extractor: Extractor[InputRecord, Key] = None,
        reducer: Reducer[Key, T, InputRecord] = None,
        # Other Redgrease args
        requirements: Iterable[str] = None,
        # Other Redis Gears args
        **kwargs,
    ) -> "GearsBuilder":
        """Instance-local :ref:`op_localgroupby` operation performs many-to-less mapping (N:M)
        of records.

        Args:
            extractor :data:`redgrease.typing.Extractor`, optional):
                Function to apply on the input records, to extact the grouping key.
                The function must take one argument as input (input record) and
                return a string (key).
                The groups are defined by the value of the key.
                Defaults to the hash of the input.

            reducer :data:`redgrease.typing.Reducer`, optional):
                Function to apply on the records of each group, to reduce to a single
                value (per group).
                The function must take (a) a key, (b) an input record and (c) a
                variable that's called an accumulator.
                It performs similarly to the accumulator callback, with the difference
                being that it maintains an accumulator per reduced key / group.
                Defaults to a list accumulator, I.e. the output will be a list of
                all inputs, for each group.

            requirements (Iterable[str], optional):
                Additional requirements / dependency Python packages.
                Defaults to None.

            **kwargs:
                Additional parameters to the LocalGroupBy operation.

        Returns:
            GearsBuilder:
                Itself, i.e. the same GearsBuilder, but with its internal gear function
                updated with a LocalGroupBy operation as last step.
        """
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
        """Instance-local :ref:`op_limit` operation limits the number of records.

        Args:
            length (int):
                The maximum number of records.

            start (int, optional):
                The index of the first input record.
                Defaults to 0.

            requirements (Iterable[str], optional):
                Additional requirements / dependency Python packages.
                Defaults to None.

            **kwargs:
                Additional parameters to the Limit operation.

        Returns:
            GearsBuilder:
                Itself, i.e. the same GearsBuilder, but with its internal gear function
                updated with a Limit operation as last step.
        """
        self._function = self._function.limit(length=length, start=start, **kwargs)

        return self

    def collect(self, **kwargs) -> "GearsBuilder":
        """Cluster-global :ref:`op_collect` operation collects the result records.

        Args:
            **kwargs:
                Additional parameters to the Collect operation.

        Returns:
            GearsBuilder:
                Itself, i.e. the same GearsBuilder, but with its internal gear function
                updated with a Collect operation as last step.
        """
        if self._function:
            self._function = self._function.collect(**kwargs)

        return self

    def repartition(
        self,
        extractor: Extractor[InputRecord, Hashable],
        # Other Redgrease args
        requirements: Iterable[str] = None,
        # Other Redis Gears args
        **kwargs,
    ) -> "GearsBuilder":
        """Cluster-global :ref:`op_repartition` operation repartitions the records by shuffling
        them between shards.

        Args:
            extractor :data:`redgrease.typing.Extractor`):
                Function that takes a record and calculates a key that is used to
                determine the hash slot, and consequently the shard, that the record
                should migrate to to.
                The function must take one argument as input (input record) and
                return a string (key).
                The hash slot, and consequently the destination shard, is determined by
                the value of the key.

            requirements (Iterable[str], optional):
                Additional requirements / dependency Python packages.
                Defaults to None.

            **kwargs:
                Additional parameters to the Repartition operation.

        Returns:
            GearsBuilder:
                Itself, i.e. the same GearsBuilder, but with its internal gear function
                updated with a Repartition operation as last step.
        """
        self._function = self._function.repartition(
            extractor=extractor, requirements=requirements, **kwargs
        )

        return self

    def aggregate(
        self,
        zero: T = None,
        seqOp: Accumulator[T, InputRecord] = None,
        combOp: Accumulator[T, T] = None,
        # Other Redgrease args
        requirements: Iterable[str] = None,
        # Other Redis Gears args
        **kwargs,
    ) -> "GearsBuilder":
        """Distributed :ref:`op_aggregate` operation perform an aggregation on local
        data then a global aggregation on the local aggregations.

        Args:
            zero (Any, optional):
                The initial / zero value of the accumulator variable.
                Defaults to an empty list.

            seqOp :data:`redgrease.typing.Accumulator`, optional):
                A function to be applied on each of the input records, locally per
                shard.
                It must take two parameters:
                - an accumulator value, from previous calls
                - an input record
                The function aggregates the input into the accumulator variable,
                which stores the state between the function's invocations.
                The function must return the accumulator's updated value.
                Defaults to addition, if 'zero' is a number and to a list accumulator
                if 'zero' is a list.

            combOp :data:`redgrease.typing.Accumulator`, optional):
                A function to be applied on each of the aggregated results of the local
                aggregation (i.e. the output of `seqOp`).
                It must take two parameters:
                - an accumulator value, from previous calls
                - an input record
                The function aggregates the input into the accumulator variable,
                which stores the state between the function's invocations.
                The function must return the accumulator's updated value.
                Defaults to re-use the `seqOp` function.

            requirements (Iterable[str], optional):
                Additional requirements / dependency Python packages.
                Defaults to None.

            **kwargs:
                Additional parameters to the Aggregate operation.

        Returns:
            GearsBuilder:
                Itself, i.e. the same GearsBuilder, but with its internal gear function
                updated with a Aggregate operation as last step.
        """
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
        extractor: Extractor[InputRecord, Key] = None,
        zero: T = None,
        seqOp: Reducer[Key, T, InputRecord] = None,
        combOp: Reducer[Key, T, T] = None,
        # Other Redgrease args
        requirements: Iterable[str] = None,
        # Other Redis Gears args
        **kwargs,
    ) -> "GearsBuilder":
        """Distributed :ref:`op_aggregateby` operation, behaves like aggregate, but
        separated on each key, extracted using the extractor.

        Args:
            extractor :data:`redgrease.typing.Extractor`, optional):
                Function to apply on the input records, to extact the grouping key.
                The function must take one argument as input (input record) and
                return a string (key).
                The groups are defined by the value of the key.
                Defaults to the hash of the input.

            zero (Any, optional):
                The initial / zero value of the accumulator variable.
                Defaults to an empty list.

            seqOp :data:`redgrease.typing.Accumulator`, optional):
                A function to be applied on each of the input records, locally per
                shard and group.
                It must take two parameters:
                - an accumulator value, from previous calls
                - an input record
                The function aggregates the input into the accumulator variable,
                which stores the state between the function's invocations.
                The function must return the accumulator's updated value.
                Defaults to a list reducer.

            combOp :data:`redgrease.typing.Accumulator`):
                A function to be applied on each of the aggregated results of the local
                aggregation (i.e. the output of `seqOp`).
                It must take two parameters:
                - an accumulator value, from previous calls
                - an input record
                The function aggregates the input into the accumulator variable,
                which stores the state between the function's invocations.
                The function must return the accumulator's updated value.
                Defaults to re-use the `seqOp` function.

            requirements (Iterable[str], optional):
                Additional requirements / dependency Python packages.
                Defaults to None.

            **kwargs:
                Additional parameters to the AggregateBy operation.

        Returns:
            GearsBuilder:
                Itself, i.e. the same GearsBuilder, but with its internal gear function
                updated with a AggregateBy operation as last step.
        """
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
        extractor: Extractor[InputRecord, Key] = None,
        reducer: Reducer[Key, T, InputRecord] = None,
        # Other Redgrease args
        requirements: Iterable[str] = None,
        # Other Redis Gears args
        **kwargs,
    ) -> "GearsBuilder":
        """Cluster-local :ref:`op_groupby` operation performing a many-to-less (N:M)
        grouping of records.

        Args:
            extractor :data:`redgrease.typing.Extractor`, optional):
                Function to apply on the input records, to extact the grouping key.
                The function must take one argument as input (input record) and
                return a string (key).
                The groups are defined by the value of the key.
                Defaults to the hash of the input.

            reducer :data:`redgrease.typing.Reducer`, optional):
                Function to apply on the records of each group, to reduce to a single
                value (per group).
                The function must take (a) a key, (b) an input record and (c) a
                variable that's called an accumulator.
                It performs similarly to the accumulator callback, with the difference
                being that it maintains an accumulator per reduced key / group.
                Defaults to a list reducer.

            requirements (Iterable[str], optional):
                Additional requirements / dependency Python packages.
                Defaults to None.

            **kwargs:
                Additional parameters to the GroupBy operation.

        Returns:
            GearsBuilder:
                Itself, i.e. the same GearsBuilder, but with its internal gear function
                updated with a GroupBy operation as last step.
        """
        self._function = self._function.groupby(
            extractor=extractor, reducer=reducer, requirements=requirements, **kwargs
        )

        return self

    def batchgroupby(
        self,
        extractor: Extractor[InputRecord, Key] = None,
        reducer: BatchReducer[Key, T, InputRecord] = None,
        # Other Redgrease args
        requirements: Iterable[str] = None,
        # Other Redis Gears args
        **kwargs,
    ) -> "GearsBuilder":
        """Cluster-local :ref:`op_groupby` operation, performing a many-to-less (N:M)
        grouping of records.

            Note: Using this operation may cause a substantial increase in memory usage
                during runtime. Consider using the GroupBy

        Args:
            extractor :data:`redgrease.typing.Extractor`, optional):
                Function to apply on the input records, to extact the grouping key.
                The function must take one argument as input (input record) and
                return a string (key).
                The groups are defined by the value of the key.
                Defaults to the hash of the input.

            reducer :data:`redgrease.typing.Reducer`):
                Function to apply on the records of each group, to reduce to a single
                value (per group).
                The function must take (a) a key, (b) an input record and (c) a
                variable that's called an accumulator.
                It performs similarly to the accumulator callback, with the difference
                being that it maintains an accumulator per reduced key / group.
                Default is the length (`len`) of the input.

            **kwargs:
                Additional parameters to the BatchGroupBy operation.

        Returns:
            GearsBuilder:
                Itself, i.e. the same GearsBuilder, but with its internal gear function
                updated with a BatchGroupBy operation as last step.
        """
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
        """:ref:`op_sort` the records

        Args:
            reverse (bool, optional):
                Sort in descending order (higher to lower).
                Defaults to True.

            requirements (Iterable[str], optional):
                Additional requirements / dependency Python packages.
                Defaults to None.

            **kwargs:
                Additional parameters to the Sort operation.

        Returns:
            GearsBuilder:
                Itself, i.e. the same GearsBuilder, but with its internal gear function
                updated with a Sort operation as last step.
        """
        self._function = self._function.sort(
            reverse=reverse, requirements=requirements, **kwargs
        )

        return self

    def distinct(self, **kwargs) -> "GearsBuilder":
        """Keep only the :ref:`op_distinct` values in the data.

        Args:
            **kwargs:
                Additional parameters to the Distinct operation.

        Returns:
            GearsBuilder:
                Itself, i.e. the same GearsBuilder, but with its internal gear function
                updated with a Distinct operation as last step.
        """
        self._function = self._function.distinct(**kwargs)

        return self

    def count(self, **kwargs) -> "GearsBuilder":
        """:ref:`op_count` the number of records in the execution.

        Args:
            **kwargs:
                Additional parameters to the Count operation.

        Returns:
            GearsBuilder:
                Itself, i.e. the same GearsBuilder, but with its internal gear function
                updated with a Count operation as last step.
        """
        self._function = self._function.count(**kwargs)

        return self

    def countby(
        self,
        extractor: Extractor[InputRecord, Hashable] = lambda x: str(x),
        # Other Redgrease args
        requirements: Iterable[str] = None,
        # Other Redis Gears args
        **kwargs,
    ) -> "GearsBuilder":
        """Distributed :ref:`op_countby` operation counting the records grouped by key.

        Args:
            extractor :data:`redgrease.typing.Extractor`):
                Function to apply on the input records, to extact the grouping key.
                The function must take one argument as input (input record) and
                return a string (key).
                The groups are defined by the value of the key.
                Defaults to 'lambda x: str(x)'.

            requirements (Iterable[str], optional):
                Additional requirements / dependency Python packages.
                Defaults to None.

            **kwargs:
                Additional parameters to the CountBy operation.

        Returns:
            GearsBuilder:
                Itself, i.e. the same GearsBuilder, but with its internal gear function
                updated with a CountBy operation as last step.
        """
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
        extractor: Extractor[InputRecord, float] = lambda x: float(
            x if isinstance(x, (int, float, str)) else str(x)
        ),
        # Other Redgrease args
        requirements: Iterable[str] = None,
        # Other Redis Gears args
        **kwargs,
    ) -> "GearsBuilder":
        """Distributed :ref:`op_avg` operation, calculating arithmetic average of the records.

        Args:
            extractor :data:`redgrease.typing.Extractor`):
                Function to apply on the input records, to extact the grouping key.
                The function must take one argument as input (input record) and
                return a string (key).
                The groups are defined by the value of the key.
                Defaults to 'lambda x: float(x)'.

            requirements (Iterable[str], optional):
                Additional requirements / dependency Python packages.
                Defaults to None.

            **kwargs:
                Additional parameters to the map operation.

        Returns:
            OpenGearFunction:
                A new "open" gear function with an avg operation as last step.
                GearsBuilder - The same GearBuilder, but with updated function.

                Note that for GearBuilder this method does **not** return a new
                GearFunction, but instead returns the same GearBuilder, but with its
                internal function updated.
        """
        self._function = self._function.avg(
            extractor=extractor,
            # Other Redgrease args
            requirements=requirements,
            # Other Redis Gears args
            **kwargs,
        )

        return self


GB = GearsBuilder
"""Convenience shorthand for GearsBuilder."""


# # Suppress warnings for missing redisgears package
# # As this package only lives on the Redis Gears server
# pyright: reportMissingImports=false
class atomic:
    """The atomic() Python context is imported to the runtime's environment by default.

    The context ensures that all operations in it are executed atomically by blocking
    he main Redis process.
    """

    def __init__(self):
        from redisgears import atomicCtx as redisAtomic

        self._atomic = redisAtomic()

    def __enter__(self):
        self._atomic.__enter__()
        return self

    def __exit__(self, *args, **kwargs):
        self._atomic.__exit__(*args, **kwargs)


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
    inside gear functions, as the braces are already escaped. Example::

        redgrease.cmd.set(f"{hashtag3()}",  some_value)

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
                - 'warning'

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


class gearsFuture:
    """The gearsFuture object allows another thread/process to process the
    record.

    Returning this object from a step's operation tells RedisGears to suspend execution
    until background processing had finished/failed.

    The gearsFuture object provides two control methods: ``continueRun()`` and
    ``continueFailed()``. Both methods are thread-safe and can be called at any time to
    signal that the background processing has finished.

    ``continueRun()`` signals success and its argument is a record for the main process.
    ``continueFailed()`` reports a failure to the main process and its argument is a
    string describing the failure.

    Calling gearsFuture() is supported only from the context of the following
    operations:

    * :ref:`Map`
    * flatmap
    * filter
    * foreach
    * aggregate
    * aggregateby

    An attempt to create a ``gearsFuture`` object outside of the supported contexts
    will result in an exception.
    """

    def __init__(self):
        from redisgears import gearsFutureCtx as redisGearsFuture

        self._gearsFuture = redisGearsFuture()

    def continueRun(self, record) -> None:
        """Signals success and its argument is a record for the main process.

        Args:
            record (Any):
                Record to yield to the blocked Gear function.
        """
        return self._gearsFuture.continueRun(record)

    def continueFail(self, message: str):
        """Reports a failure to the main process and its argument is a string describing
        the failure.

        Args:
            message (str):
                Message describing the failure.
        """
        return self._gearsFuture.continueFail(message)


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
