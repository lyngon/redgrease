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
from typing import TYPE_CHECKING, Hashable, Iterable, TypeVar

import redgrease.gears
import redgrease.sugar as sugar

if TYPE_CHECKING:
    import redgrease.typing as optype

T = TypeVar("T")


class GearsBuilder:  # (redgrease.gears.PartialGearFunction):
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
        """Create a closed batch function..

        Batch functions are executed once and exits once the data is
        exhausted by its reader.
        Args:
            arg (str, optional):
                An optional argument that's passed to the reader as its defaultArg.
                It means the following::
                    - A glob-like pattern for the KeysReader and KeysOnlyReader readers.
                    - A key name for the StreamReader reader.
                    - A Python generator for the PythonReader reader.

                Defaults to None.

            convertToStr (bool, optional):
                When `True` adds a map operation to the flow's end that stringifies
                records.
                Defaults to False.

            collect (bool, optional):
                When `True` adds a collect operation to flow's end.
                Defaults to False.

            requirements (Iterable[str], optional):
                Additional requirements / dedpendency Python packages.
                Defaults to None.

            on (redis.Redis):
                Immedeately execute the function on this Redis Gear server / cluster.

            **kwargs:
                Additional parameters to the run operation.

        Returns:
            Union[ClosedGearFunction, redgrease.data.ExecutionResult]:
                A new closed batch function, if `on` is **not** specified.
                An execution result, if `on` **is** specified.

        Raises:
            TypeError:
                If the function does not support batch mode.
        """

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
        """Create a closed event function.

        Event functions are executed each time an event arrives.
        Each time it is executed, the function operates on the event's
        data and once done is suspended until its future invocations by
        new events.
        Args:
            prefix (str, optional):
                Key prefix pattern to match on.
                Not relevant for 'CommandReader' readers (see 'trigger').
                Defaults to '*'.

            convertToStr (bool, optional):
                When `True` adds a map  operation to the flow's end that stringifies
                records.
                Defaults to True.

            collect (bool, optional):
                When True adds a collect operation to flow's end.
                Defaults to False.

            mode (str, optional):
                The execution mode of the function.
                Can be one of::

                    - 'async': Execution will be asynchronous across the entire
                        cluster.
                    - 'async_local': Execution will be asynchronous and restricted
                        to the handling shard.
                    - 'sync': Execution will be synchronous and local.
                Defaults to 'async'.

            onRegistered (Callback, optional):
                A function that's called on each shard upon function registration.
                It is a good place to initialize non-serializable objects such as
                network connections.
                Defaults to None.

            eventTypes (Iterable[str], optional):
                For KeysReader only.
                A whitelist of event types that trigger execution when the KeysReader
                are used. The list may contain one or more::

                    - Any Redis or module command
                    - Any Redis event

                Defaults to None.

            keyTypes (Iterable[str], optional):
                For KeysReader and KeysOnlyReader only.
                A whitelist of key types that trigger execution when using the
                KeysReader or KeysOnlyReader readers.
                The list may contain one or more from the following::

                    - Redis core types: 'string', 'hash', 'list', 'set', 'zset'
                        or 'stream'
                    - Redis module types: 'module'

                Defaults to None.

            readValue (bool, optional):
                For KeysReader only.
                When `False` the value will not be read, so the 'type' and 'value'
                of the record will be set to None.
                Defaults to True.

            batch (int, optional):
                For StreamReader only.
                The number of new messages that trigger execution.
                Defaults to 1.

            duration  (int, optional):
                For StreamReader only.
                The time to wait before execution is triggered, regardless of the batch
                size (0 for no duration).
                Defaults to 0.

            onFailedPolcy (str, optional):
                For StreamReader only.
                The policy for handling execution failures.
                May be one of::

                    - 'continue': Ignores a failure and continues to the next execution.
                        This is the default policy.
                    - 'abort': Stops further executions.
                    - 'retry': Retries the execution after an interval specified with
                        onFailedRetryInterval (default is one second).

                Defaults to 'continue'.

            onFailedRetryInterval (int, optional):
                For StreamReader only.
                The interval (in milliseconds) in which to retry in case onFailedPolicy
                is 'retry'.
                Defaults to 1.

            trimStream (bool, optional):
                For StreamReader only.
                When True the stream will be trimmed after execution
                Defaults to True.

            trigger (str):
                For 'CommandReader' only, and mandatory.
                The trigger string that will trigger the function.

            requirements (Iterable[str], optional):
                Additional requirements / dedpendency Python packages.
                Defaults to None.

            on (redis.Redis):
                Immedeately execute the function on this Redis Gear server / cluster.

            **kwargs:
                Additional parameters to the register operation.

        Returns:
            Union[ClosedGearFunction, redgrease.data.ExecutionResult]:
                A new closed event function, if `on` is **not** specified.
                An execution result, if `on` **is** specified.

        Raises:
            TypeError:
                If the function does not support event mode.
        """
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
        """Instance-local Map operation that performs a one-to-one (1:1) mapping of
        records.

        Args:
            op (redgrease.typing.Mapper):
                Function to map on the input records.
                The function must take one argument as input (input record) and
                return something as an output (output record).

            requirements (Iterable[str], optional):
                Additional requirements / dedpendency Python packages.
                Defaults to None.

            **kwargs:
                Additional parameters to the Map operation.

        Returns:
            redgrease.runtime.GearsBuilder:
                The GearsBuilder, updated with a Map operation as last step.
        """
        self._function = self._function.map(op=op, requirements=requirements, **kwargs)

        return self

    def flatmap(
        self,
        op: "optype.Expander[optype.InputRecord, optype.OutputRecord]",
        # Other Redgrease args
        requirements: Iterable[str] = None,
        # Other Redis Gears args
        **kwargs,
    ) -> "GearsBuilder":
        """Instance-local FlatMap operation that performs one-to-many (1:N) mapping
        of records.

        Args:
            op (redgrease.typing.Expander):
                Function to map on the input records.
                The function must take one argument as input (input record) and
                return an iterable as an output (output records).

            requirements (Iterable[str], optional):
                Additional requirements / dedpendency Python packages.
                Defaults to None.

            **kwargs:
                Additional parameters to the FlatMap operation.

        Returns:
            redgrease.runtime.GearsBuilder:
                The GearsBuilder, updated with a FlatMap operation as last step.
        """
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
        """Instance-local ForEach operation performs one-to-the-same (1=1) mapping.

        Args:
            op (redgrease.typing.Processor):
                Function to run on each of the input records.
                The function must take one argument as input (input record) and
                should not return anything.

            requirements (Iterable[str], optional):
                Additional requirements / dedpendency Python packages.
                Defaults to None.

            **kwargs:
                Additional parameters to the ForEach operation.

        Returns:
            redgrease.runtime.GearsBuilder:
                The GearsBuilder, updated with a ForEach operation as last step.
        """
        self._function = self._function.foreach(
            op=op, requirements=requirements, **kwargs
        )

        return self

    def filter(
        self,
        op: "optype.Filterer[optype.InputRecord]",
        # Other Redgrease args
        requirements: Iterable[str] = None,
        # Other Redis Gears args
        **kwargs,
    ) -> "GearsBuilder":
        """Instance-local Filter operation performs one-to-zero-or-one (1:bool)
        filtering of records.

        Args:
            op (redgrease.typing.Filterer):
                Function to apply on the input records, to decide which ones to keep.
                The function must take one argument as input (input record) and
                return a bool. The input records evaluated to `True` will be kept as
                output records.

            requirements (Iterable[str], optional):
                Additional requirements / dedpendency Python packages.
                Defaults to None.

            **kwargs:
                Additional parameters to the Filter operation.

        Returns:
            redgrease.runtime.GearsBuilder:
                The GearsBuilder, updated with a FIlter operation as last step.
        """
        self._function = self._function.filter(
            op=op, requirements=requirements, **kwargs
        )

        return self

    def accumulate(
        self,
        op: "optype.Accumulator[T, optype.InputRecord]",
        # Other Redgrease args
        requirements: Iterable[str] = None,
        # Other Redis Gears args
        **kwargs,
    ) -> "GearsBuilder":
        """Instance-local Accumulate operation performs many-to-one mapping (N:1) of
        records.

        Args:
            op (redgrease.typing.Accumulator):
                Function to to apply on the input records.
                The function must take two arguments as input:
                    - the input record, and
                    - An accumulator value.
                It should aggregate the input record into the accumulator variable,
                which stores the state between the function's invocations.
                The function must return the accumulator's updated value.

            requirements (Iterable[str], optional):
                Additional requirements / dedpendency Python packages.
                Defaults to None.

            **kwargs:
                Additional parameters to the Accumulate operation.

        Returns:
            redgrease.runtime.GearsBuilder:
                The GearsBuilder, updated with Accumulate operation as last step.
        """
        self._function = self._function.accumulate(
            op=op, requirements=requirements, **kwargs
        )

        return self

    def localgroupby(
        self,
        extractor: "optype.Extractor[optype.InputRecord, optype.Key]",
        reducer: "optype.Reducer[optype.Key, T, optype.InputRecord]",
        # Other Redgrease args
        requirements: Iterable[str] = None,
        # Other Redis Gears args
        **kwargs,
    ) -> "GearsBuilder":
        """Instance-local LocalGroupBy operation performs many-to-less mapping (N:M)
        of records.

        Args:
            extractor (redgrease.typing.Extractor):
                Function to apply on the input records, to extact the grouping key.
                The function must take one argument as input (input record) and
                return a string (key).
                The groups are defined by the value of the key.

            reducer (redgrease.typing.Reducer):
                Function to apply on the records of each group, to reduce to a single
                value (per group).
                The function must take (a) a key, (b) an input record and (c) a
                variable that's called an accumulator.
                It performs similarly to the accumulator callback, with the difference
                being that it maintains an accumulator per reduced key / group.

            requirements (Iterable[str], optional):
                Additional requirements / dedpendency Python packages.
                Defaults to None.

            **kwargs:
                Additional parameters to the LocalGroupBy operation.

        Returns:
            redgrease.runtime.GearsBuilder:
                The GearsBuilder, updated with a LocalGroupBy operation as last step.
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
        """Instance-local Limit operation limits the number of records.

        Args:
            length (int):
                The maximum number of records.

            start (int, optional):
                The index of the first input record.
                Defaults to 0.

            requirements (Iterable[str], optional):
                Additional requirements / dedpendency Python packages.
                Defaults to None.

            **kwargs:
                Additional parameters to the Limit operation.

        Returns:
            redgrease.runtime.GearsBuilder:
                The GearsBuilder, updated with a Limit operation as last step.
        """
        if self._function:
            self._function = self._function.limit(length=length, start=start, **kwargs)

        return self

    def collect(self, **kwargs) -> "GearsBuilder":
        """Cluster-global Collect operation collects the result records.

        Args:
            **kwargs:
                Additional parameters to the Collect operation.

        Returns:
            redgrease.runtime.GearsBuilder:
                The GearsBuilder, updated with a Collect operation as last step.
        """
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
        """Cluster-global Repartition operation repartitions the records by shuffling
        them between shards.

        Args:
            extractor (redgrease.typing.Extractor):
                Function that takes a record and calculates a key that is used to
                determine the hash slot, and consequently the shard, that the record
                should migrate to to.
                The function must take one argument as input (input record) and
                return a string (key).
                The hash slot, and consequently the destination shard, is determined by
                hthe value of the key.

            requirements (Iterable[str], optional):
                Additional requirements / dedpendency Python packages.
                Defaults to None.

            **kwargs:
                Additional parameters to the Repartition operation.

        Returns:
            redgrease.runtime.GearsBuilder:
                The GearsBuilder, updated with a Repartition operation as last step.
        """
        if self._function:
            self._function = self._function.repartition(
                extractor=extractor, requirements=requirements, **kwargs
            )

        return self

    def aggregate(
        self,
        zero: T,
        seqOp: "optype.Accumulator[T, optype.InputRecord]",
        combOp: "optype.Accumulator[T, T]",
        # Other Redgrease args
        requirements: Iterable[str] = None,
        # Other Redis Gears args
        **kwargs,
    ) -> "GearsBuilder":
        """Perform aggregation on all the execution data.

        Args:
            zero (Any):
                The initial / zero value of the accumulator variable.

            seqOp (redgrease.typing.Accumulator):
                A function to be applied on each of the input records, locally per
                shard.
                It must take two parameters:
                - an accumulator value, from previous calls
                - an input record
                The functoin aggregates the input into the accumulator variable,
                which stores the state between the function's invocations.
                The function must return the accumulator's updated value.

            combOp (redgrease.typing.Accumulator):
                A function to be applied on each of the aggregated results of the local
                aggregation (i.e. the output of `seqOp`).
                It must take two parameters:
                - an accumulator value, from previous calls
                - an input record
                The functoin aggregates the input into the accumulator variable,
                which stores the state between the function's invocations.
                The function must return the accumulator's updated value.

            requirements (Iterable[str], optional):
                Additional requirements / dedpendency Python packages.
                Defaults to None.

            **kwargs:
                Additional parameters to the Aggregate operation.

        Returns:
            redgrease.runtime.GearsBuilder:
                The GearsBuilder, updated with a Aggregate operation as last step.
        """
        if self._function:
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
        extractor: "optype.Extractor[optype.InputRecord, optype.Key]",
        zero: T,
        seqOp: "optype.Reducer[optype.Key, T, optype.InputRecord]",
        combOp: "optype.Reducer[optype.Key, T, T]",
        # Other Redgrease args
        requirements: Iterable[str] = None,
        # Other Redis Gears args
        **kwargs,
    ) -> "GearsBuilder":
        """Like aggregate, but on each key, the key is extracted using the extractor.

        Args:
            extractor (redgrease.typing.Extractor):
                Function to apply on the input records, to extact the grouping key.
                The function must take one argument as input (input record) and
                return a string (key).
                The groups are defined by the value of the key.

            zero (Any):
                The initial / zero value of the accumulator variable.

            seqOp (redgrease.typing.Accumulator):
                A function to be applied on each of the input records, locally per
                shard and group.
                It must take two parameters:
                - an accumulator value, from previous calls
                - an input record
                The functoin aggregates the input into the accumulator variable,
                which stores the state between the function's invocations.
                The function must return the accumulator's updated value.

            combOp (redgrease.typing.Accumulator):
                A function to be applied on each of the aggregated results of the local
                aggregation (i.e. the output of `seqOp`).
                It must take two parameters:
                - an accumulator value, from previous calls
                - an input record
                The functoin aggregates the input into the accumulator variable,
                which stores the state between the function's invocations.
                The function must return the accumulator's updated value.

            requirements (Iterable[str], optional):
                Additional requirements / dedpendency Python packages.
                Defaults to None.

            **kwargs:
                Additional parameters to the AggregateBy operation.

        Returns:
            redgrease.runtime.GearsBuilder:
                The GearsBuilder, updated with a AggregateBy operation as last step.
        """
        if self._function:
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
        extractor: "optype.Extractor[optype.InputRecord, optype.Key]",
        reducer: "optype.Reducer[optype.Key, T, optype.InputRecord]",
        # Other Redgrease args
        requirements: Iterable[str] = None,
        # Other Redis Gears args
        **kwargs,
    ) -> "GearsBuilder":
        """Perform a many-to-less (N:M) grouping of records.

        Args:
            extractor (redgrease.typing.Extractor):
                Function to apply on the input records, to extact the grouping key.
                The function must take one argument as input (input record) and
                return a string (key).
                The groups are defined by the value of the key.

            reducer (redgrease.typing.Reducer):
                Function to apply on the records of each group, to reduce to a single
                value (per group).
                The function must take (a) a key, (b) an input record and (c) a
                variable that's called an accumulator.
                It performs similarly to the accumulator callback, with the difference
                being that it maintains an accumulator per reduced key / group.
            requirements (Iterable[str], optional):
                Additional requirements / dedpendency Python packages.
                Defaults to None.

            **kwargs:
                Additional parameters to the GroupBy operation.

        Returns:
            redgrease.runtime.GearsBuilder:
                The GearsBuilder, updated withh a GroupBy operation as last step.
        """
        self._function = self._function.groupby(
            extractor=extractor, reducer=reducer, requirements=requirements, **kwargs
        )

        return self

    def batchgroupby(
        self,
        extractor: "optype.Extractor[optype.InputRecord, optype.Key]",
        reducer: "optype.BatchReducer[optype.Key, T, optype.InputRecord]",
        # Other Redgrease args
        requirements: Iterable[str] = None,
        # Other Redis Gears args
        **kwargs,
    ) -> "GearsBuilder":
        """Many-to-less (N:M) grouping of records.

            Note: Using this operation may cause a substantial increase in memory usage
                during runtime. Consider using the GroupBy

        Args:
            extractor (redgrease.typing.Extractor):
                Function to apply on the input records, to extact the grouping key.
                The function must take one argument as input (input record) and
                return a string (key).
                The groups are defined by the value of the key.

            reducer (redgrease.typing.Reducer):
                Function to apply on the records of each group, to reduce to a single
                value (per group).
                The function must take (a) a key, (b) an input record and (c) a
                variable that's called an accumulator.
                It performs similarly to the accumulator callback, with the difference
                being that it maintains an accumulator per reduced key / group.

            **kwargs:
                Additional parameters to the BatchGroupBy operation.

        Returns:
            redgrease.runtime.GearsBuilder:
                The GearsBuilder, updated with a BatchGroupBy operation as last step.
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
        """Sort the records

        Args:
            reverse (bool, optional):
                Sort in descending order (higer to lower).
                Defaults to True.

            requirements (Iterable[str], optional):
                Additional requirements / dedpendency Python packages.
                Defaults to None.

            **kwargs:
                Additional parameters to the Sort operation.

        Returns:
            redgrease.runtime.GearsBuilder:
                The GearsBuilder, updated with a Sort operation as last step.
        """
        self._function = self._function.sort(
            reverse=reverse, requirements=requirements, **kwargs
        )

        return self

    def distinct(self, **kwargs) -> "GearsBuilder":
        """Keep only the distinct values in the data.

        Args:
            **kwargs:
                Additional parameters to the Distinct operation.

        Returns:
            redgrease.runtime.GearsBuilder:
                The GearsBuilder, updated with a Distinct operation as last step.
        """
        self._function = self._function.distinct(**kwargs)

        return self

    def count(self, **kwargs) -> "GearsBuilder":
        """Count the number of records in the execution.

        Args:
            **kwargs:
                Additional parameters to the Count operation.

        Returns:
            redgrease.runtime.GearsBuilder:
                The GearsBuilder, updated witha Count operation as last step.
        """
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
        """Counts the records grouped by key.

        Args:
            extractor (redgrease.typing.Extractor):
                Function to apply on the input records, to extact the grouping key.
                The function must take one argument as input (input record) and
                return a string (key).
                The groups are defined by the value of the key.
                Defaults to 'lambda x: str(x)'.

            requirements (Iterable[str], optional):
                Additional requirements / dedpendency Python packages.
                Defaults to None.

            **kwargs:
                Additional parameters to the CountBy operation.

        Returns:
            redgrease.runtime.GearsBuilder:
                The GearsBuilder, updated with a CountBy operation as last step.
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
        extractor: "optype.Extractor[optype.InputRecord, float]" = lambda x: float(x),
        # Other Redgrease args
        requirements: Iterable[str] = None,
        # Other Redis Gears args
        **kwargs,
    ) -> "GearsBuilder":
        """Calculating arithmetic average of the records.

        Args:
            extractor (redgrease.typing.Extractor):
                Function to apply on the input records, to extact the grouping key.
                The function must take one argument as input (input record) and
                return a string (key).
                The groups are defined by the value of the key.
                Defaults to 'lambda x: float(x)'.

            requirements (Iterable[str], optional):
                Additional requirements / dedpendency Python packages.
                Defaults to None.

            **kwargs:
                Additional parameters to the map operation.

        Returns:
            redgrease.runtime.GearsBuilder:
                The GearsBuilder, updated withh an avg operation as last step.
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
