# -*- coding: utf-8 -*-
# from __future__ import annotations

"""
GearsFunction and Operation definitions
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
import numbers
import operator
from typing import Any, Dict, Generic, Hashable, Iterable, Optional, Type, TypeVar

import redgrease.sugar as sugar
import redgrease.utils
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

################################################################################
#                               Default Operands                               #
################################################################################


def _default_accumulator(acc, r):
    acc = acc if isinstance(acc, list) else [acc]
    acc.append(r)
    return acc


def _default_extractor(r):
    return hash(r)


def _default_reducer(_, acc, r):
    return _default_accumulator(acc, r)


def _default_batch_reducer(_, records):
    return len(records)


################################################################################
#                                 Operations                                   #
################################################################################


class Operation:
    """Abstract base class for Gear function operations.

    Operations are the building block of RedisGears functions.
    Different operation types can be used to achieve a variety of results to
    meet various data processing needs.

    Operations can have zero or more arguments that control their operation.
    Depending on the operation's type arguments may be language-native data types
    and function callbacks.

    Attributes:
        kwargs (dict):
            Any left-over keyword arguments not explicitly consumed by the operation.
    """

    def __init__(self, **kwargs):
        """Instantiate a Operation with left-over args"""
        self.kwargs = kwargs

    def add_to(self, function):
        """Placeholder method for adding the operation to the end of a Gear function.
        This method must be implemented in each subclass, and will throw a
        ``NotImplementedException`` if called directly on the `Operation` superclass.

        Args:
            function (Union[Type, OpenGearFunction]):
                The "open" gear function to append this operation to.

                If, and only if, the operation is a reader function (always and only
                the first operation in any Gear function), then the `function`
                argument must instead be a GearsBuilder class/type.

        Raises:
            NotImplementedError:
                If invoked on the `Operation` superclass, and,or not implemented in the
                subclass.
        """
        raise NotImplementedError(
            "Builder Operation has not implemented the `add_to` method: "
            f"'{self.__class__.__name__}'"
        )


class Nop(Operation):
    """No Operation.

    This Operation does nothing.
    """

    def add_to(self, function: "OpenGearFunction") -> "OpenGearFunction":
        """Returns the function, unmodified.

        Args:
            function (Union[Type, OpenGearFunction]):
                The "open" gear function to append this operation to.

                If, and only if, the operation is a reader function (always and only
                the first operation in any Gear function), then the `function`
                argument must instead be a GearsBuilder class/type.

        Returns:
            OpenGearFunction:
                The function unmodified.
        """
        return function


class Reader(Operation):
    """Reader operation

    The Reader operation is always and only the first operation of any GearFunction.

    It defines which reader type to use and its arguments.

    Attributes:
        reader (str):
            The type of reader (https://oss.redislabs.com/redisgears/readers.html)

                - ``"KeysReader"``
                - ``"KeysOnlyReader"``
                - ``"StreamReader"``
                - ``"PythonReader"``
                - ``"ShardsIDReader"``
                - ``"CommandReader"``

        defaultArg (str):
            Argument that the reader may need. These are usually a key's name, prefix,
            glob-like or a regular expression. Its use depends on the function's reader
            type and action.

        desc (str):
            Function description.
    """

    def __init__(
        self,
        reader: str = sugar.ReaderType.KeysReader,
        defaultArg: str = "*",
        desc: Optional[str] = None,
        **kwargs,
    ) -> None:
        """Instantiate Reader operation

        Args:
            reader (str, optional):
                Reader type.
                Defaults to sugar.ReaderType.KeysReader.

            defaultArg (str, optional):
                Reader default arguments.
                Defaults to "*".

            desc (Optional[str], optional):
                Function description.
                Defaults to ``None``.
        """
        super().__init__(**kwargs)
        self.reader = reader
        self.defaultArg = defaultArg
        self.desc = desc

    def add_to(self, builder: Type) -> "OpenGearFunction":
        """Create a new gear function based on this reader information.

        Args:
            builder (Type):
                GearsBuilder class. Defines the constructor to use to create the
                Gear function.

        Returns:
            OpenGearFunction:
                Returns a minimal "open" gear function, consisting only of the
                reader.
        """
        return builder(self.reader, self.defaultArg, self.desc, **self.kwargs)


class Run(Operation):
    """Run action

    The Run action runs a Gear function as a batch.
    The function is executed once and exits once the data is exhausted by its reader.

    The Run action can only be the last operation of any GearFunction, and it
    effectivesly 'closes' it to further operations.

    Attributes:
        arg (str, optional):
            Argument that's passed to the reader, overriding its defaultArg.
            It means the following:

                - A glob-like pattern for the KeysReader and KeysOnlyReader readers.
                - A key name for the StreamReader reader.
                - A Python generator for the PythonReader reader.

        convertToStr (bool):
            When `True`, adds a map operation to the flow's end that stringifies
            records.

        collect (bool):
            When `True`, adds a collect operation to flow's end.
    """

    def __init__(
        self,
        arg: Optional[str] = None,
        convertToStr: bool = True,
        collect: bool = True,
        **kwargs,
    ) -> None:
        """Instantiate a Run action

        Args:
            arg (Optional[str], optional):
                Optional argument that's passed to the reader, overriding its
                defaultArg.
                It means the following:

                    - A glob-like pattern for the KeysReader and KeysOnlyReader readers.
                    - A key name for the StreamReader reader.
                    - A Python generator for the PythonReader reader.

                Defaults to ``None``.

            convertToStr (bool, optional):
                When `True`, adds a map operation to the flow's end that stringifies
                records.
                Defaults to ``True``.

            collect (bool, optional):
                When `True`, adds a collect operation to flow's end.
                Defaults to ``True``.
        """
        super().__init__(**kwargs)
        self.arg = arg
        self.convertToStr = convertToStr
        self.collect = collect

    def add_to(self, function: "OpenGearFunction") -> "ClosedGearFunction":
        """Closes a Gear function with the Run action.

        Args:
            function (OpenGearFunction):
                The "open" Gear function to close with the run action.

        Returns:
            ClosedGearFunction:
                A closed Gear batch function that is ready to run in RedisGears.
        """
        import cloudpickle

        return function.map(lambda x: cloudpickle.dumps(x, protocol=4)).run(
            self.arg, False, self.collect, **self.kwargs
        )


class Register(Operation):
    """Register action

    The Register action registers a function as an event handler.
    The function is executed each time an event arrives.
    Each time it is executed, the function operates on the event's data and once done
    it is suspended until its future invocations by new events.

    The Register action can only be the last operation of any GearFunction, and it
    effectivesly 'closes' it to further operations.

    Attributes:
        prefix (str):
            Key prefix pattern to match on.
            Not relevant for 'CommandReader' readers (see 'trigger').

        convertToStr (bool):
            When `True`, adds a map operation to the flow's end that stringifies
            records.

        collect (bool):
            When `True`, adds a collect operation to flow's end.

        mode (str):
            The execution mode of the triggered function.

        onRegistered (Callable):
            A function callback that's called on each shard upon function registration.s
    """

    def __init__(
        self,
        prefix: str = "*",
        convertToStr: bool = True,
        collect: bool = True,
        mode: str = sugar.TriggerMode.Async,
        onRegistered: Registrator = None,
        **kwargs,
    ) -> None:
        """Instantiate a Register action

        Args:
            prefix (str, optional):
                Key prefix pattern to match on.
                Not relevant for 'CommandReader' readers (see 'trigger').
                Defaults to '*'.

            convertToStr (bool, optional):
                When ``True`` adds a map operation to the flow's end that stringifies
                records.
                Defaults to ``True``.

            collect (bool, optional):
                When ``True`` adds a collect operation to flow's end.
                Defaults to ``False``.

            mode (str, optional):
                The execution mode of the function.
                Can be one of::

                - ``"async"``:

                    Execution will be asynchronous across the entire cluster.

                - ``"async_local"``:

                    Execution will be asynchronous and restricted to the handling shard.

                - ``"sync"``:

                    Execution will be synchronous and local

                Defaults to `redgrease.TriggerMode.Async` (``"async"``)

            onRegistered (Registrator, optional):
                A function callback that's called on each shard upon function
                registration.
                It is a good place to initialize non-serializeable objects such as
                network connections.
                Defaults to ``None``.

        """
        super().__init__(**kwargs)
        self.prefix = prefix
        self.convertToStr = convertToStr
        self.collect = collect
        self.mode = mode
        self.onRegistered = onRegistered

    def add_to(self, function: "OpenGearFunction") -> "ClosedGearFunction":
        """Closes a Gear function with the Register action.

        Args:
            function (OpenGearFunction):
                The "open" Gear function to close with the register action.

        Returns:
            ClosedGearFunction:
                A closed "event-mode" Gear function that is ready to be registered on a
                RedisGears system.
        """
        import cloudpickle

        return function.map(lambda x: cloudpickle.dumps(x, protocol=4)).register(
            self.prefix,
            False,
            self.collect,
            mode=self.mode,
            onRegistered=self.onRegistered,
            **self.kwargs,
        )


################################################################################
#                                 Operations                                   #
################################################################################


class Map(Operation):
    """The local Map operation performs the one-to-one (1:1) mapping of records.

    It requires one mapper function.

    Attributes:
        op (:data:`redgrease.typing.Mapper`):
            The mapper function to map on all input records.
    """

    def __init__(self, op: Mapper, **kwargs) -> None:
        """Instantiate a Map operation.

        Args:
            op (:data:`redgrease.typing.Mapper`):
                Function to map on the input records.
                The function must take one argument as input (input record) and
                return something as an output (output record).
        """
        super().__init__(**kwargs)
        self.op = op

    def add_to(self, function: "OpenGearFunction") -> "OpenGearFunction":
        """Adds the operation to an "open" Gear function.

        Args:
            function (OpenGearFunction):
                The "open" gear function to add the operation to.

        Returns:
            OpenGearFunction:
                The function with the operation added to the end.
        """
        return function.map(self.op, **self.kwargs)


class FlatMap(Operation):
    """The local FlatMap operation performs one-to-many (1:N) mapping of records.

    It requires one expander function that maps a single input record to potentially
    multiple output records.

    FlatMap is nearly identical to the Map operation in purpose and use.
    Unlike regular mapping, however, when FlatMap returns a sequence / iterator,
    each element in the sequence is turned into a separate output record.

    Attributes:
        op (:data:`redgrease.typing.Expander`):
            The mapper function to map on all input records.
    """

    def __init__(self, op: Expander, **kwargs) -> None:
        """Instantiate a FlatMap operation.

        Args:
            op (:data:`redgrease.typing.Expander`):
                Function to map on the input records.
                The function must take one argument as input (input record) and
                return an iterable as an output (output records).
        """
        super().__init__(**kwargs)
        self.op = op

    def add_to(self, function: "OpenGearFunction") -> "OpenGearFunction":
        """Adds the operation to an "open" Gear function.

        Args:
            function (OpenGearFunction):
                The "open" gear function to add the operation to.

        Returns:
            OpenGearFunction:
                The function with the operation added to the end.
        """
        return function.flatmap(self.op, **self.kwargs)


class ForEach(Operation):
    """The local ForEach operation performs one-to-the-same (1=1) mapping.

    It requires one processor function to perform some work that's related to the
    input record.

    Its output record is a copy of the input, which means anything the callback returns
    is discarded.

    Args:
        op (:data:`redgrease.typing.Processor`):
            Function to run on the input records.
    """

    def __init__(self, op: Processor, **kwargs) -> None:
        """Instantiate a ForEach operation.

        Args:
            op (:data:`redgrease.typing.Processor`):
                Function to run on each of the input records.
                The function must take one argument as input (input record) and
                should not return anything.
        """
        super().__init__(**kwargs)
        self.op = op

    def add_to(self, function: "OpenGearFunction") -> "OpenGearFunction":
        """Adds the operation to an "open" Gear function.

        Args:
            function (OpenGearFunction):
                The "open" gear function to add the operation to.

        Returns:
            OpenGearFunction:
                The function with the operation added to the end.
        """
        return function.foreach(self.op, **self.kwargs)


class Filter(Operation):
    """The local Filter operation performs one-to-zero-or-one (1:(0|1)) filtering of
    records.

    It requires a filterer function.

    An input record that yields a falsehood will be discarded and only truthful ones
    will be output.

    Args:
        op (:data:`redgrease.typing.Filterer`):
            Predicate function to run on the input records.

    """

    def __init__(self, op: Filterer, **kwargs) -> None:
        """Instantiate a Filter operation.

        Args:
            op (:data:`redgrease.typing.Filterer`):
                Function to apply on the input records, to decide which ones to keep.
                The function must take one argument as input (input record) and
                return a bool. The input records evaluated to ``True`` will be kept as
                output records.
        """
        super().__init__(**kwargs)
        self.op = op

    def add_to(self, function: "OpenGearFunction") -> "OpenGearFunction":
        """Adds the operation to an "open" Gear function.

        Args:
            function (OpenGearFunction):
                The "open" gear function to add the operation to.

        Returns:
            OpenGearFunction:
                The function with the operation added to the end.
        """
        return function.filter(self.op, **self.kwargs)


class Accumulate(Operation):
    """The local Accumulate operation performs many-to-one mapping (N:1) of records.

    It requires one accumulator function.

    Once input records are exhausted its output is a single record consisting of the
    accumulator's value.

    Args:
        op (:data:`redgrease.typing.Accumulator`):
            Accumulation function to run on the input records.
    """

    def __init__(self, op: Accumulator, **kwargs) -> None:
        """Instantiate an Accumulate operation.

        Args:
            op (:data:`redgrease.typing.Accumulator`):
                Function to to apply on the input records.
                The function must take two arguments as input:
                    - the input record, and
                    - An accumulator value.
                It should aggregate the input record into the accumulator variable,
                which stores the state between the function's invocations.
                The function must return the accumulator's updated value.
        """
        super().__init__(**kwargs)
        self.op = op

    def add_to(self, function: "OpenGearFunction") -> "OpenGearFunction":
        """Adds the operation to an "open" Gear function.

        Args:
            function (OpenGearFunction):
                The "open" gear function to add the operation to.

        Returns:
            OpenGearFunction:
                The function with the operation added to the end.
        """
        return function.accumulate(self.op, **self.kwargs)


class LocalGroupBy(Operation):
    """The local LocalGroupBy operation performs many-to-less mapping (N:M) of records.

    The operation requires two functions, an extractor and a reducer.

    The output records consist of the grouping key and its respective reduce value.

    Attributes:
        extractor (:data:`redgrease.typing.Extractor`):
            Function that extracts the key to group by from input records.

        reducer (:data:`redgrease.typing.Reducer`):
            Function that reduces the records in each group to an output record.
    """

    def __init__(
        self,
        extractor: Extractor,
        reducer: Reducer,
        **kwargs,
    ) -> None:
        """Instantiate a LocalGroupBy operator.

        Args:
            extractor (:data:`redgrease.typing.Extractor`):
                Function to apply on the input records, to extact the grouping key.
                The function must take one argument as input (input record) and
                return a string (key).
                The groups are defined by the value of the key.

            reducer (:data:`redgrease.typing.Reducer`):
                Function to apply on the records of each group, to reduce to a single
                value (per group).
                The function must take (a) a key, (b) an input record and (c) a
                variable that's called an accumulator.
                It performs similarly to the accumulator callback, with the difference
                being that it maintains an accumulator per reduced key / group.
        """
        super().__init__(**kwargs)
        self.extractor = extractor
        self.reducer = reducer

    def add_to(self, function: "OpenGearFunction") -> "OpenGearFunction":
        """Adds the operation to an "open" Gear function.

        Args:
            function (OpenGearFunction):
                The "open" gear function to add the operation to.

        Returns:
            OpenGearFunction:
                The function with the operation added to the end.
        """
        return function.localgroupby(self.extractor, self.reducer, **self.kwargs)


class Limit(Operation):
    """The local Limit operation limits the number of records.

    It accepts two numeric arguments: a starting position in the input records "array"
    and a maximal number of output records.

    Attributes:
        start (int):
            Starting index (0-based) of the input record to start from

        length (int):
            The maximum number of records to let through.
    """

    def __init__(self, length: int, start: int = 0, **kwargs) -> None:
        """Instantiate a Limit operation

        Args:
            length (int):
                The maximum number of records.

            start (int, optional):
                The index of the first input record.
                Defaults to 0.
        """
        super().__init__(**kwargs)
        self.length = length
        self.start = start

    def add_to(self, function: "OpenGearFunction") -> "OpenGearFunction":
        """Adds the operation to an "open" Gear function.

        Args:
            function (OpenGearFunction):
                The "open" gear function to add the operation to.

        Returns:
            OpenGearFunction:
                The function with the operation added to the end.
        """
        return function.limit(self.length, self.start, **self.kwargs)


class Collect(Operation):
    """The global Collect operation collects the result records from all of the
    shards to the originating one.
    """

    def __init__(self, **kwargs):
        """Instantiate a Collect operation."""
        super().__init__(**kwargs)

    def add_to(self, function: "OpenGearFunction") -> "OpenGearFunction":
        """Adds the operation to an "open" Gear function.

        Args:
            function (OpenGearFunction):
                The "open" gear function to add the operation to.

        Returns:
            OpenGearFunction:
                The function with the operation added to the end.
        """
        return function.collect(**self.kwargs)


class Repartition(Operation):
    """The global Repartition operation repartitions the records by them shuffling
        between shards.

    It accepts a single key extractor function.
    The extracted key is used for computing the record's new placement in the cluster
    (i.e. hash slot).
    The operation then moves the record from its original shard to the new one.

        Attributes:
            extractor (redgrease.typing.Extractor):
                A function deciding the destination shard of an input record.
    """

    def __init__(self, extractor: Extractor, **kwargs) -> None:
        """Instantiate a Repartition operation

        Args:
            extractor (:data:`redgrease.typing.Extractor`):
                Function that takes a record and calculates a key that is used to
                determine the hash slot, and consequently the shard, that the record
                should migrate to to.
                The function must take one argument as input (input record) and
                return a string (key).
                The hash slot, and consequently the destination shard, is determined by
                hthe value of the key.
        """
        super().__init__(**kwargs)
        self.extractor = extractor

    def add_to(self, function: "OpenGearFunction") -> "OpenGearFunction":
        """Adds the operation to an "open" Gear function.

        Args:
            function (OpenGearFunction):
                The "open" gear function to add the operation to.

        Returns:
            OpenGearFunction:
                The function with the operation added to the end.
        """
        return function.repartition(self.extractor, **self.kwargs)


class Aggregate(Operation):
    """The Aggregate operation performs many-to-one mapping (N:1) of records.

    Aggregate provides an alternative to the local accumulate operation as it takes
    the partitioning of data into consideration.
    Furthermore, because records are aggregated locally before collection,
    its performance is usually superior.

    It requires a zero value and two accumulator functions for computing the local
    and global aggregates.

    The operation is made of these steps::

    1. The local accumulator is executed locally and initialized with the zero value.
    2. A global collect moves all records to the originating engine.
    3. The global accumulator is executed locally by the originating engine.

    Its output is a single record consisting of the accumulator's global value.

    Attributes:
        zero (Any):
            The initial / zero value for the accumulator variable.

        seqOp (:data:`redgrease.typing.Accumulator`):
            A local accumulator function, applied locally on each shard.

        combOp (:data:`redgrease.typing.Accumulator`):
            A global accumulator function, applied on the results of the local
            accumulations.
    """

    def __init__(
        self,
        zero: Any,
        seqOp: Accumulator,
        combOp: Accumulator,
        **kwargs,
    ) -> None:
        """Instantiates an Aggregate operation.

        Args:
            zero (Any):
                The initial / zero value of the accumulator variable.

            seqOp (:data:`redgrease.typing.Accumulator`):
                A function to be applied on each of the input records, locally per
                shard.
                It must take two parameters:
                - an accumulator value, from previous calls
                - an input record
                The function aggregates the input into the accumulator variable,
                which stores the state between the function's invocations.
                The function must return the accumulator's updated value.

            combOp (:data:`redgrease.typing.Accumulator`):
                A function to be applied on each of the aggregated results of the local
                aggregation (i.e. the output of `seqOp`).
                It must take two parameters:
                - an accumulator value, from previous calls
                - an input record
                The function aggregates the input into the accumulator variable,
                which stores the state between the function's invocations.
                The function must return the accumulator's updated value.
        """
        super().__init__(**kwargs)
        self.zero = zero
        self.seqOp = seqOp
        self.combOp = combOp

    def add_to(self, function: "OpenGearFunction") -> "OpenGearFunction":
        """Adds the operation to an "open" Gear function.

        Args:
            function (OpenGearFunction):
                The "open" gear function to add the operation to.

        Returns:
            OpenGearFunction:
                The function with the operation added to the end.
        """
        return function.aggregate(self.zero, self.seqOp, self.combOp, **self.kwargs)


class AggregateBy(Operation):
    """AggregateBy operation performs many-to-less mapping (N:M) of records.

    It is similar to the Aggregate operation but aggregates per key.
    It requires a an extractor callback, a zero value and two reducers callbacks for
    computing the local and global aggregates.

    The operation is made of these steps::

    1. Extraction of the groups using extractor.
    2. The local reducer is executed locally and initialized with the zero value.
    3. A global repartition operation that uses the extractor.
    4. The global reducer is executed on each shard once it is repartitioned with its
        relevant keys.

    Output list of records, one for each key. The output records consist of the
    grouping key and its respective reducer's value.

    Attributes:
        extractor (:data:`redgrease.typing.Extractor`):
            Function that extracts the key to group by from input records.

        zero (Any):
            The initial / zero value for the accumulator variable.

        seqOp (:data:`redgrease.typing.Accumulator`):
            A local accumulator function, applied locally on each shard.

        combOp (:data:`redgrease.typing.Accumulator`):
            A global accumulator function, applied on the results of the local
            accumulations.
    """

    def __init__(
        self,
        extractor: Extractor,
        zero: Any,
        seqOp: Reducer,
        combOp: Reducer,
        **kwargs,
    ) -> None:
        """Instantiate an AggregateBy operation.

        Args:
            extractor (:data:`redgrease.typing.Extractor`):
                Function to apply on the input records, to extact the grouping key.
                The function must take one argument as input (input record) and
                return a string (key).
                The groups are defined by the value of the key.

            zero (Any):
                The initial / zero value of the accumulator variable.

            seqOp (:data:`redgrease.typing.Accumulator`):
                A function to be applied on each of the input records, locally per
                shard and group.
                It must take two parameters:
                - an accumulator value, from previous calls
                - an input record
                The function aggregates the input into the accumulator variable,
                which stores the state between the function's invocations.
                The function must return the accumulator's updated value.

            combOp (:data:`redgrease.typing.Accumulator`):
                A function to be applied on each of the aggregated results of the local
                aggregation (i.e. the output of `seqOp`).
                It must take two parameters:
                - an accumulator value, from previous calls
                - an input record
                The function aggregates the input into the accumulator variable,
                which stores the state between the function's invocations.
                The function must return the accumulator's updated value.
        """
        super().__init__(**kwargs)
        self.extractor = extractor
        self.zero = zero
        self.seqOp = seqOp
        self.combOp = combOp

    def add_to(self, function: "OpenGearFunction") -> "OpenGearFunction":
        """Adds the operation to an "open" Gear function.

        Args:
            function (OpenGearFunction):
                The "open" gear function to add the operation to.

        Returns:
            OpenGearFunction:
                The function with the operation added to the end.
        """
        return function.aggregateby(
            self.extractor, self.zero, self.seqOp, self.combOp, **self.kwargs
        )


class GroupBy(Operation):
    """GroupBy * operation performs a many-to-less (N:M) grouping of records.
    It is similar to AggregateBy but uses only a global reducer.
    It can be used in cases where locally reducing the data isn't possible.

    The operation requires two functions; an extractor a reducer.

    The operation is made of these steps::

        1. A global repartition operation that uses the extractor.
        2. The reducer is locally invoked.

    Output is a locally-reduced list of records, one for each key.
    The output records consist of the grouping key and its respective accumulator's
    value.

    Attributes:
        extractor (:data:`redgrease.typing.Extractor`):
            Function that extracts the key to group by from input records.

        reducer (:data:`redgrease.typing.Reducer`):
            Function that reduces the records of each group to a value
    """

    def __init__(
        self,
        extractor: Extractor,
        reducer: Reducer,
        **kwargs,
    ) -> None:
        """Instantiate a GroupBy operation.

        Args:
            extractor (:data:`redgrease.typing.Extractor`):
                Function to apply on the input records, to extact the grouping key.
                The function must take one argument as input (input record) and
                return a string (key).
                The groups are defined by the value of the key.

            reducer (:data:`redgrease.typing.Reducer`):
                Function to apply on the records of each group, to reduce to a single
                value (per group).
                The function must take (a) a key, (b) an input record and (c) a
                variable that's called an accumulator.
                It performs similarly to the accumulator callback, with the difference
                being that it maintains an accumulator per reduced key / group.
        """
        super().__init__(**kwargs)
        self.extractor = extractor
        self.reducer = reducer

    def add_to(self, function: "OpenGearFunction") -> "OpenGearFunction":
        """Adds the operation to an "open" Gear function.

        Args:
            function (OpenGearFunction):
                The "open" gear function to add the operation to.

        Returns:
            OpenGearFunction:
                The function with the operation added to the end.
        """
        return function.groupby(self.extractor, self.reducer, **self.kwargs)


class BatchGroupBy(Operation):
    """BatchGroupBy operation performs a many-to-less (N:M) grouping of records.

    Prefer the GroupBy Operation
    ----------------------------

        Instead of using BatchGroupBy, prefer using the GroupBy operation as it is more
        efficient and performant. Only use BatchGroupBy when the reducer's logic
        requires the full list of records for each input key.

    The operation requires two functions; an extractor a batch reducer.

    The operation is made of these steps::

        1. A global repartition operation that uses the extractor
        2. A local localgroupby operation that uses the batch reducer

    Once finished, the operation locally outputs a record for each key and its
    respective accumulator value.

    Increased memory consumption
    ----------------------------

        Using this operation may cause a substantial increase in memory usage during
        runtime.

    Attributes:
        extractor (:data:`redgrease.typing.Extractor`):
            Function that extracts the key to group by from input records.

        reducer (:data:`redgrease.typing.Reducer`):
            Function that reduces the records of each group to a value
    """

    def __init__(
        self,
        extractor: Extractor,
        reducer: BatchReducer,
        **kwargs,
    ) -> None:
        """Instantiate a BatchGroupBy operation.

        Args:
            extractor (:data:`redgrease.typing.Extractor`):
                Function to apply on the input records, to extact the grouping key.
                The function must take one argument as input (input record) and
                return a string (key).
                The groups are defined by the value of the key.

            reducer (:data:`redgrease.typing.Reducer`):
                Function to apply on the records of each group, to reduce to a single
                value (per group).
                The function must take (a) a key, (b) an input record and (c) a
                variable that's called an accumulator.
                It performs similarly to the accumulator callback, with the difference
                being that it maintains an accumulator per reduced key / group.
        """
        super().__init__(**kwargs)
        self.extractor = extractor
        self.reducer = reducer

    def add_to(self, function: "OpenGearFunction") -> "OpenGearFunction":
        """Adds the operation to an "open" Gear function.

        Args:
            function (OpenGearFunction):
                The "open" gear function to add the operation to.

        Returns:
            OpenGearFunction:
                The function with the operation added to the end.
        """
        return function.batchgroupby(self.extractor, self.reducer, **self.kwargs)


class Sort(Operation):
    """Sort operation sorts the records.

        It allows to control the sort order.

        The operation is made of the following steps::

            1. A global aggregate operation collects and combines all records.
            2. A local sort is performed on the list.
            3. The list is flatmapped to records.

    Increased memory consumption
    ----------------------------

        Using this operation may cause an increase in memory usage during runtime due
        to the list being copied during the sorting operation.

    Attributes:
        reverse (bool):
            Defines if the sorting order is descending (``True``) or ascending
            (``False``).
    """

    def __init__(self, reverse: bool = True, **kwargs) -> None:
        """Instantiate a Sort operation.

        Args:
            reverse (bool, optional):
                Sort in descending order (higher to lower).
                Defaults to ``True``.
        """
        super().__init__(**kwargs)
        self.reverse = reverse

    def add_to(self, function: "OpenGearFunction") -> "OpenGearFunction":
        """Adds the operation to an "open" Gear function.

        Args:
            function (OpenGearFunction):
                The "open" gear function to add the operation to.

        Returns:
            OpenGearFunction:
                The function with the operation added to the end.
        """
        return function.sort(self.reverse, **self.kwargs)


class Distinct(Operation):
    """The Distinct operation returns distinct records.

    It requires no arguments.

    The operation is made of the following steps::

        1. A aggregate operation locally reduces the records to sets that are then
            collected and unionized globally.
        2. A local flatmap operation turns the set into records.
    """

    def __init__(self, **kwargs):
        """Instantiate a Distinct operation."""
        super().__init__(**kwargs)

    def add_to(self, function: "OpenGearFunction") -> "OpenGearFunction":
        """Adds the operation to an "open" Gear function.

        Args:
            function (OpenGearFunction):
                The "open" gear function to add the operation to.

        Returns:
            OpenGearFunction:
                The function with the operation added to the end.
        """
        return function.distinct(**self.kwargs)


class Count(Operation):
    """The Count operation counts the number of input records.

    It requires no arguments.

    The operation is made of an aggregate operation that uses local counting and
    global summing accumulators.
    """

    def __init__(self, **kwargs):
        """Instantiate a Count operation."""
        super().__init__(**kwargs)

    def add_to(self, function: "OpenGearFunction") -> "OpenGearFunction":
        """Adds the operation to an "open" Gear function.

        Args:
            function (OpenGearFunction):
                The "open" gear function to add the operation to.

        Returns:
            OpenGearFunction:
                The function with the operation added to the end.
        """
        return function.count(**self.kwargs)


class CountBy(Operation):
    """The CountBy operation counts the records grouped by key.

    It requires a single extractor function.

    The operation is made of an aggregateby operation that uses local counting and
    global summing accumulators.

    Attributes:
        extractor (:data:`redgrease.typing.Extractor`):
            Function that extracts the key to group by from input records.
    """

    def __init__(self, extractor: Extractor, **kwargs) -> None:
        """Instantiate a CountBy operation.

        Args:
            extractor (:data:`redgrease.typing.Extractor`):
                Function to apply on the input records, to extact the grouping key.
                The function must take one argument as input (input record) and
                return a string (key).
                The groups are defined by the value of the key.
        """
        super().__init__(**kwargs)
        self.extractor = extractor

    def add_to(self, function: "OpenGearFunction") -> "OpenGearFunction":
        """Adds the operation to an "open" Gear function.

        Args:
            function (OpenGearFunction):
                The "open" gear function to add the operation to.

        Returns:
            OpenGearFunction:
                The function with the operation added to the end.
        """
        return function.countby(self.extractor, **self.kwargs)


class Avg(Operation):
    """The Avg operation returns the arithmetic average of records.

    It has an optional value extractor function.

    The operation is made of the following steps::

        1. A aggregate operation locally reduces the records to tuples of sum and count
            that are globally combined.
        2. A local map operation calculates the average from the global tuple.

    Attributes:
        extractor (:data:`redgrease.typing.Extractor`):
            Function that extracts the key to group by from input records.
    """

    def __init__(self, extractor: Extractor, **kwargs) -> None:
        """Instantiate an Avg operation.

        Args:
            extractor (:data:`redgrease.typing.Extractor`):
                Function to apply on the input records, to extact the grouping key.
                The function must take one argument as input (input record) and
                return a string (key).
                The groups are defined by the value of the key.
        """
        super().__init__(**kwargs)
        self.extractor = extractor

    def add_to(self, function: "OpenGearFunction") -> "OpenGearFunction":
        """Adds the operation to an "open" Gear function.

        Args:
            function (OpenGearFunction):
                The "open" gear function to add the operation to.

        Returns:
            OpenGearFunction:
                The function with the operation added to the end.
        """
        return function.avg(self.extractor, **self.kwargs)


################################################################################
#                               GearFunctions                                  #
################################################################################


class GearFunction(Generic[T]):
    """Abstract base class for both "open" and closed Gear functions.
    The base `GearFunction` class is not intended to be instantiated by API users.

    A GearFunction is a chain of consecutive Operations.

    Attributes:
        operation (Operation):
            The last operation in the functions chain of operations.

        input_function (OpenGearFunction):
            The function (chain of operations) that provides the input records to the
            `operation`. Two different GearFunctions can share the same `input_function`

        requirements (Iterable[str], optional):
            A set of requirements / dependencies (Python packages) that the operation
            requires in order to execute.
    """

    def __init__(
        self,
        operation: Operation,
        input_function: "OpenGearFunction" = None,
        requirements: Optional[Iterable[str]] = None,
    ) -> None:
        """Instantiate a GearFunction

        Args:
            operation (Operation):
                The last operation in the functions chain of operations.

            input_function (OpenGearFunction, optional):
                The function (chain of operations) that provides the input records to
                the `operation`.
                Defaults to ``None``.

            requirements (Optional[Iterable[str]], optional):
                A set of requirements / dependencies (Python packages) that the
                operation requires in order to execute.
                Defaults to ``None``.
        """

        self.operation = operation
        self.input_function = input_function
        self.requirements = set(requirements if requirements else [])

        if input_function:
            self.requirements = self.requirements.union(input_function.requirements)

    @property
    def reader(self) -> Optional[str]:
        """The reader type, generating the initial input records to the GearFunction.

        Returns:
            str:
                Either ``"KeysReader"``, ``"KeysOnlyReader"``, ``"StreamReader"``,
                ``"PythonReader"``, ``"ShardsIDReader"``, ``"CommandReader"`` or
                ``None`` (If no reader is defined).
        """
        if isinstance(self.operation, Reader):
            return self.operation.reader

        if self.input_function:
            return self.input_function.reader

        return None

    @property
    def supports_batch_mode(self) -> bool:
        """Indicates if the function can run in Batch-mode, by closing it with a
        `run` action.

        Returns:
            bool:
                ``True`` if the function supports batch mode, ``False`` if not.
        """
        return self.reader in [
            sugar.ReaderType.KeysReader,
            sugar.ReaderType.KeysOnlyReader,
            sugar.ReaderType.StreamReader,
            sugar.ReaderType.PythonReader,
            sugar.ReaderType.ShardsIDReader,
        ]

    @property
    def supports_event_mode(self) -> bool:
        """Indicates if the function can run in Event-mode, by closing it with a
        `register` action.

        Returns:
            bool:
                ``True`` if the function supports event mode, ``False`` if not.
        """
        return self.reader in [
            sugar.ReaderType.KeysReader,
            sugar.ReaderType.StreamReader,
            sugar.ReaderType.CommandReader,
        ]


class ClosedGearFunction(GearFunction[T]):
    """Closed Gear functions are GearsFunctions that have been "closed" with a
    :ref:`op_action_run` action or a :ref:`op_action_register` action.

    Closed Gear functions cannot add more :ref:`operations`, but can be executed in
    RedisGears.
    """

    def __init__(
        self,
        operation: Operation,
        input_function: "OpenGearFunction" = None,
        requirements: Optional[Iterable[str]] = None,
    ) -> None:
        """ """
        super().__init__(
            operation, input_function=input_function, requirements=requirements
        )

    def on(
        self,
        gears_server,
        unblocking: bool = False,
        requirements: Iterable[str] = None,
        replace: bool = None,
        **kwargs,
    ):
        """Execute the function on a RedisGears.
        This is equivalent to passing the function to `Gears.pyexecute`

        Args:
            gears_server ([type]):
                Redis client / connection object.

            unblocking (bool, optional):
                Execute function unblocking, i.e. asynchronous.
                Defaults to ``False``.

            requirements (Iterable[str], optional):
                Additional requirements / dependency Python packages.
                Defaults to ``None``.

        Returns:
            redgrease.data.ExecutionResult:
                The result of the function, just as `Gears.pyexecute`
        """
        if hasattr(gears_server, "gears"):
            gears_server = gears_server.gears
        if not hasattr(gears_server, "pyexecute"):
            from redgrease.client import Gears

            gears_server = Gears(gears_server)

        try:
            return gears_server.pyexecute(
                self, unblocking=unblocking, requirements=requirements, **kwargs
            )
        except Exception as ex:
            # TODO: This is ugly. just to keep 'redis' from being imported to "gears"
            if ex.__class__.__name__ != "DuplicateTriggerError":
                raise

            # If we get an error because the trigger already is registered,
            # then we check the 'replace' argument for what to do:
            # - `replace is ``None``` : Re-raise the error
            # - `replace is ``False``` : Ignore the error
            # - `replace is ``True``` : Unregister the previous and re-register the new
            if replace is None or "trigger" not in self.operation.kwargs:
                raise

            if replace is False:
                return gears_server._trigger_proxy(self.operation.kwargs["trigger"])

            # Find and replace the registered trigger.
            trigger = self.operation.kwargs["trigger"]
            regs = gears_server.dumpregistrations(trigger=trigger)
            if len(regs) != 1:
                raise
            gears_server.unregister(regs[0].id)

            # Try registering again
            return gears_server.pyexecute(
                self, unblocking=unblocking, requirements=requirements, **kwargs
            )


class OpenGearFunction(GearFunction[InputRecord]):
    """An open Gear function is a Gear function that is not yet "closed" with a
    :ref:`op_action_run` action or a :ref:`op_action_register` action.

    Open Gear functions can be used to create new "open" gear functions by
    applying :ref:`operations`, or it can create a closed Gear function by applying
    either the :ref:`op_action_run` action or a :ref:`op_action_register` action.
    """

    def __init__(
        self,
        operation: Operation,
        input_function: "OpenGearFunction" = None,
        requirements: Optional[Iterable[str]] = None,
    ) -> None:
        """ """
        super().__init__(
            operation, input_function=input_function, requirements=requirements
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
    ) -> ClosedGearFunction[InputRecord]:
        """Create a "closed" function to be :ref:`op_action_run` as in "batch-mode".

        Batch functions are executed once and exits once the data is
        exhausted by its reader.

        Args:
            arg (str, optional):
                An optional argument that's passed to the reader as its defaultArg.
                It means the following:

                - A glob-like pattern for the KeysReader and KeysOnlyReader readers.

                - A key name for the StreamReader reader.

                - A Python generator for the PythonReader reader.

                Defaults to ``None``.

            convertToStr (bool, optional):
                When ``True``, adds a map operation to the flow's end that stringifies
                records.
                Defaults to ``False``.

            collect (bool, optional):
                When ``True`` adds a collect operation to flow's end.
                Defaults to ``False``.

            requirements (Iterable[str], optional):
                Additional requirements / dependency Python packages.
                Defaults to ``None``.

            on (redis.Redis):
                Immediately execute the function on this RedisGears system.

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
        if not self.supports_batch_mode:
            raise TypeError(f"Batch mode (run) is not supported for '{self.reader}'")

        gear_fun: ClosedGearFunction = ClosedGearFunction[InputRecord](
            Run(arg=arg, convertToStr=convertToStr, collect=collect, **kwargs),
            input_function=self,
            requirements=requirements,
        )

        if redgrease.GEARS_RUNTIME:
            return redgrease.runtime.run(gear_fun, redgrease.GearsBuilder)

        if on:
            return gear_fun.on(on)

        return gear_fun

    def register(  # noqa: C901
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
    ) -> ClosedGearFunction[InputRecord]:
        """Create a "closed" function to be :ref:`op_action_register` 'ed as an
        event-triggered function.

        Event functions are executed each time an event arrives.
        Each time it is executed, the function operates on the event's
        data and once done is suspended until its future invocations by
        new events.
        Args:
            prefix (str, optional):
                Key prefix pattern to match on.
                Not relevant for 'CommandReader' readers (see 'trigger').
                Defaults to ``"*"``.

            convertToStr (bool, optional):
                When ``True`` adds a map  operation to the flow's end that stringifies
                records.
                Defaults to ``True``.

            collect (bool, optional):
                When ``True`` adds a collect operation to flow's end.
                Defaults to ``False``.

            mode (str, optional):
                The execution mode of the function.
                Can be one of:

                - ``"async"``:

                    Execution will be asynchronous across the entire cluster.

                - ``"async_local"``:

                    Execution will be asynchronous and restricted to the handling shard.

                - ``"sync"``:

                    Execution will be synchronous and local.

                Defaults to ``"async"``.

            onRegistered (Registrator, optional):
                A function that's called on each shard upon function registration.
                It is a good place to initialize non-serializeable objects such as
                network connections.
                Defaults to ``None``.

            eventTypes (Iterable[str], optional):
                For KeysReader only.
                A whitelist of event types that trigger execution when the KeysReader
                are used. The list may contain one or more:

                    - Any Redis or module command

                    - Any Redis event

                Defaults to ``None``.

            keyTypes (Iterable[str], optional):
                For KeysReader and KeysOnlyReader only.
                A whitelist of key types that trigger execution when using the
                KeysReader or KeysOnlyReader readers.
                The list may contain one or more from the following:

                - Redis core types:

                    ``"string"``, ``"hash"``, ``"list"``, ``"set"``, ``"zset"`` or
                    ``"stream"``

                - Redis module types:

                    ``"module"``

                Defaults to ``None``.

            readValue (bool, optional):
                For KeysReader only.
                When ``False`` the value will not be read, so the 'type' and 'value'
                of the record will be set to ``None``.
                Defaults to ``True``.

            batch (int, optional):
                For StreamReader only.
                The number of new messages that trigger execution.
                Defaults to 1.

            duration  (int, optional):
                For StreamReader only.
                The time to wait before execution is triggered, regardless of the batch
                size (0 for no duration).
                Defaults to 0.

            onFailedPolicy (str, optional):
                For StreamReader only.
                The policy for handling execution failures.
                May be one of:

                - ``"continue"``:

                    Ignores a failure and continues to the next execution.
                    This is the default policy.

                - ``"abort"``:

                    Stops further executions.

                - ``"retry"``:

                    Retries the execution after an interval specified with
                    onFailedRetryInterval (default is one second).

                Defaults to ``"continue"``.

            onFailedRetryInterval (int, optional):
                For StreamReader only.
                The interval (in milliseconds) in which to retry in case onFailedPolicy
                is 'retry'.
                Defaults to 1.

            trimStream (bool, optional):
                For StreamReader only.
                When ``True`` the stream will be trimmed after execution
                Defaults to ``True``.

            trigger (str):
                For 'CommandReader' only, and mandatory.
                The trigger string that will trigger the function.

            requirements (Iterable[str], optional):
                Additional requirements / dependency Python packages.
                Defaults to ``None``.

            on (redis.Redis):
                Immediately execute the function on this RedisGears system.

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

        if mode is not None:
            kwargs["mode"] = mode

        if onRegistered is not None:
            kwargs["onRegistered"] = onRegistered

        if not self.supports_event_mode:
            raise TypeError(f"Event mode (run) is not supported for '{self.reader}'")

        if eventTypes is not None:
            kwargs["eventTypes"] = list(eventTypes)

        if keyTypes is not None:
            kwargs["keyTypes"] = list(keyTypes)

        if readValue is not None:
            kwargs["readValue"] = readValue

        if batch is not None:
            kwargs["batch"] = batch

        if duration is not None:
            kwargs["duration"] = duration

        if onFailedPolicy is not None:
            kwargs["onFailedPolicy"] = onFailedPolicy

        if onFailedRetryInterval is not None:
            kwargs["onFailedRetryInterval"] = onFailedRetryInterval

        if trimStream is not None:
            kwargs["trimStream"] = trimStream

        if trigger is not None:
            kwargs["trigger"] = trigger

        replace = kwargs.pop("replace", None)

        gear_fun = ClosedGearFunction[InputRecord](
            Register(
                prefix=prefix,
                convertToStr=convertToStr,
                collect=collect,
                **kwargs,
            ),
            input_function=self,
            requirements=requirements,
        )

        if redgrease.GEARS_RUNTIME:
            return redgrease.runtime.run(gear_fun, redgrease.GearsBuilder)

        if on:
            return gear_fun.on(on, replace=replace)

        return gear_fun

    def map(
        self,
        op: Mapper[
            InputRecord,
            OutputRecord,
        ],
        # Other Redgrease args
        requirements: Iterable[str] = None,
        # Other Redis Gears args
        **kwargs,
    ) -> "OpenGearFunction[OutputRecord]":
        """Instance-local :ref:`op_map` operation that performs a one-to-one (1:1) mapping of
        records.

        Args:
            op (:data:`redgrease.typing.Mapper`):
                Function to map on the input records.
                The function must take one argument as input (input record) and
                return something as an output (output record).

            requirements (Iterable[str], optional):
                Additional requirements / dependency Python packages.
                Defaults to ``None``.

            **kwargs:
                Additional parameters to the :ref:`op_map` operation.

        Returns:
            OpenGearFunction:
                A new "open" gear function with a :ref:`op_map` operation as last step.
        """
        op = redgrease.utils.passfun(op)
        return OpenGearFunction(
            Map(op=op, **kwargs),
            input_function=self,
            requirements=requirements,
        )

    def flatmap(
        self,
        op: Expander[InputRecord, OutputRecord] = None,
        # Other Redgrease args
        requirements: Iterable[str] = None,
        # Other Redis Gears args
        **kwargs,
    ) -> "OpenGearFunction[Iterable[OutputRecord]]":
        """Instance-local :ref:`op_flatmap` operation that performs one-to-many (1:N) mapping
        of records.

        Args:
            op (:data:`redgrease.typing.Expander`, optional):
                Function to map on the input records.
                The function must take one argument as input (input record) and
                return an iterable as an output (output records).
                Defaults to the 'identity-function', I.e. if input is an iterable will
                be expanded.

            requirements (Iterable[str], optional):
                Additional requirements / dependency Python packages.
                Defaults to ``None``.

            **kwargs:
                Additional parameters to the :ref:`op_flatmap` operation.

        Returns:
            OpenGearFunction:
                A new "open" gear function with a :ref:`op_flatmap` operation as last
                step.
        """
        op = redgrease.utils.passfun(op)
        return OpenGearFunction(
            FlatMap(op=op, **kwargs),
            input_function=self,
            requirements=requirements,
        )

    def foreach(
        self,
        op: Processor[InputRecord],
        # Other Redgrease args
        requirements: Iterable[str] = None,
        # Other Redis Gears args
        **kwargs,
    ) -> "OpenGearFunction[InputRecord]":
        """Instance-local :ref:`op_foreach` operation performs one-to-the-same (1=1) mapping.

        Args:
            op (:data:`redgrease.typing.Processor`):
                Function to run on each of the input records.
                The function must take one argument as input (input record) and
                should not return anything.

            requirements (Iterable[str], optional):
                Additional requirements / dependency Python packages.
                Defaults to ``None``.

            **kwargs:
                Additional parameters to the :ref:`op_foreach` operation.

        Returns:
            OpenGearFunction:
                A new "open" gear function with a :ref:`op_foreach` operation as last
                step.
        """
        op = redgrease.utils.passfun(op)
        return OpenGearFunction(
            ForEach(op=op, **kwargs),
            input_function=self,
            requirements=requirements,
        )

    def filter(
        self,
        op: Filterer[InputRecord] = None,
        # Other Redgrease args
        requirements: Iterable[str] = None,
        # Other Redis Gears args
        **kwargs,
    ) -> "OpenGearFunction[InputRecord]":
        """Instance-local :ref:`op_filter` operation performs one-to-zero-or-one (1:bool)
        filtering of records.

        Args:
            op (:data:`redgrease.typing.Filterer`, optional):
                Function to apply on the input records, to decide which ones to keep.
                The function must take one argument as input (input record) and
                return a bool. The input records evaluated to ``True`` will be kept as
                output records.
                Defaults to the 'identity-function', i.e. records are filtered based on
                their own trueness or falseness.

            requirements (Iterable[str], optional):
                Additional requirements / dependency Python packages.
                Defaults to ``None``.

            **kwargs:
                Additional parameters to the :ref:`op_filter` operation.

        Returns:
            OpenGearFunction:
                A new "open" gear function with a :ref:`op_filter` operation as last
                step.
        """
        op = redgrease.utils.passfun(op)
        return OpenGearFunction(
            Filter(op=op, **kwargs),
            input_function=self,
            requirements=requirements,
        )

    def accumulate(
        self,
        op: Accumulator[T, InputRecord] = None,
        # Other Redgrease args
        requirements: Iterable[str] = None,
        # Other Redis Gears args
        **kwargs,
    ) -> "OpenGearFunction[T]":
        """Instance-local :ref:`op_accumulate` operation performs many-to-one mapping (N:1) of
        records.

        Args:
            op (:data:`redgrease.typing.Accumulator`, optional):
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
                Defaults to ``None``.

            **kwargs:
                Additional parameters to the :ref:`op_accumulate` operation.

        Returns:
            OpenGearFunction:
                A new "open" gear function with :ref:`op_accumulate` operation as last
                step.
        """

        op = redgrease.utils.passfun(op, default=_default_accumulator)
        return OpenGearFunction(
            Accumulate(op=op, **kwargs),
            input_function=self,
            requirements=requirements,
        )

    def localgroupby(
        self,
        extractor: Extractor[InputRecord, Key] = None,
        reducer: Reducer[Key, T, InputRecord] = None,
        # Other Redgrease args
        requirements: Iterable[str] = None,
        # Other Redis Gears args
        **kwargs,
    ) -> "OpenGearFunction[Dict[Key, T]]":
        """Instance-local :ref:`op_localgroupby` operation performs many-to-less mapping (N:M)
        of records.

        Args:
            extractor (:data:`redgrease.typing.Extractor`, optional):
                Function to apply on the input records, to extact the grouping key.
                The function must take one argument as input (input record) and
                return a string (key).
                The groups are defined by the value of the key.
                Defaults to the hash of the input.

            reducer (:data:`redgrease.typing.Reducer`, optional):
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
                Defaults to ``None``.

            **kwargs:
                Additional parameters to the :ref:`op_localgroupby` operation.

        Returns:
            OpenGearFunction:
                A new "open" gear function with a :ref:`op_localgroupby` operation as
                last step.
        """
        extractor = redgrease.utils.passfun(extractor, default=_default_extractor)
        reducer = redgrease.utils.passfun(reducer, default=_default_reducer)
        return OpenGearFunction(
            LocalGroupBy(extractor=extractor, reducer=reducer, **kwargs),
            input_function=self,
            requirements=requirements,
        )

    def limit(
        self,
        length: int,
        start: int = 0,
        # Other Redis Gears args
        **kwargs,
    ) -> "OpenGearFunction[InputRecord]":
        """Instance-local :ref:`op_limit` operation limits the number of records.

        Args:
            length (int):
                The maximum number of records.

            start (int, optional):
                The index of the first input record.
                Defaults to 0.

            requirements (Iterable[str], optional):
                Additional requirements / dependency Python packages.
                Defaults to ``None``.

            **kwargs:
                Additional parameters to the :ref:`op_limit`  operation.

        Returns:
            OpenGearFunction:
                A new "open" gear function with a :ref:`op_limit`  operation as last
                step.
        """
        return OpenGearFunction(
            Limit(length=length, start=start, **kwargs),
            input_function=self,
        )

    def collect(self, **kwargs) -> "OpenGearFunction[InputRecord]":
        """Cluster-global :ref:`op_collect` operation collects the result records.

        Args:
            **kwargs:
                Additional parameters to the :ref:`op_collect` operation.

        Returns:
            OpenGearFunction:
                A new "open" gear function with a :ref:`op_collect` operation as last
                step.
        """
        return OpenGearFunction(
            Collect(**kwargs),
            input_function=self,
        )

    def repartition(
        self,
        extractor: Extractor[InputRecord, Hashable],
        # Other Redgrease args
        requirements: Iterable[str] = None,
        # Other Redis Gears args
        **kwargs,
    ) -> "OpenGearFunction[InputRecord]":
        """Cluster-global :ref:`op_repartition` operation repartitions the records by
        shuffling
        them between shards.

        Args:
            extractor (:data:`Extractor`):
                Function that takes a record and calculates a key that is used to
                determine the hash slot, and consequently the shard, that the record
                should migrate to to.
                The function must take one argument as input (input record) and
                return a string (key).
                The hash slot, and consequently the destination shard, is determined by
                the value of the key.

            requirements (Iterable[str], optional):
                Additional requirements / dependency Python packages.
                Defaults to ``None``.

            **kwargs:
                Additional parameters to the :ref:`op_repartition` operation.

        Returns:
            OpenGearFunction:
                A new "open" gear function with a :ref:`op_repartition` operation as
                last step.
        """
        return OpenGearFunction(
            Repartition(extractor=extractor, **kwargs),
            input_function=self,
            requirements=requirements,
        )

    def aggregate(
        self,
        zero: T = None,
        seqOp: Accumulator[T, InputRecord] = None,
        combOp: Accumulator[T, T] = None,
        # Other Redgrease args
        requirements: Iterable[str] = None,
        # Other Redis Gears args
        **kwargs,
    ) -> "OpenGearFunction[T]":
        """Distributed :ref:`op_aggregate` operation perform an aggregation on local
        data then a global aggregation on the local aggregations.

        Args:
            zero (Any, optional):
                The initial / zero value of the accumulator variable.
                Defaults to an empty list.

            seqOp (:data:`redgrease.typing.Accumulator`, optional):
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

            combOp (:data:`redgrease.typing.Accumulator`, optional):
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
                Defaults to ``None``.

            **kwargs:
                Additional parameters to the ref:`op_aggregate` operation.

        Returns:
            OpenGearFunction:
                A new "open" gear function with a ref:`op_aggregate` operation as last
                step.
        """

        _zero = zero if zero is not None else []

        if not seqOp:
            if isinstance(_zero, numbers.Number):
                seqOp = operator.add
            elif isinstance(_zero, list):
                seqOp = _default_accumulator
                combOp = combOp or operator.add
            else:
                raise ValueError(
                    "No operatod provided, and unable to deduce a reasonable default."
                )

        seqOp = redgrease.utils.passfun(seqOp)
        combOp = redgrease.utils.passfun(combOp, default=seqOp)

        return OpenGearFunction(
            Aggregate(zero=_zero, seqOp=seqOp, combOp=combOp, **kwargs),
            input_function=self,
            requirements=requirements,
        )

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
    ) -> "OpenGearFunction[Dict[Key, T]]":
        """Distributed :ref:`op_aggregateby` operation, behaves like aggregate, but
        separated on each key, extracted using the extractor.

        Args:
            extractor (:data:`redgrease.typing.Extractor`, optional):
                Function to apply on the input records, to extact the grouping key.
                The function must take one argument as input (input record) and
                return a string (key).
                The groups are defined by the value of the key.
                Defaults to the hash of the input.

            zero (Any, optional):
                The initial / zero value of the accumulator variable.
                Defaults to an empty list.

            seqOp (:data:`redgrease.typing.Accumulator`, optional):
                A function to be applied on each of the input records, locally per
                shard and group.
                It must take two parameters:
                - an accumulator value, from previous calls
                - an input record
                The function aggregates the input into the accumulator variable,
                which stores the state between the function's invocations.
                The function must return the accumulator's updated value.
                Defaults to a list reducer.

            combOp (:data:`redgrease.typing.Accumulator`):
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
                Defaults to ``None``.

            **kwargs:
                Additional parameters to the :ref:`op_aggregateby` operation.

        Returns:
            OpenGearFunction:
                A new "open" gear function with a :ref:`op_aggregateby` operation as
                last step.
        """

        _zero = zero if zero is not None else []

        extractor = redgrease.utils.passfun(extractor, default=_default_extractor)
        seqOp = redgrease.utils.passfun(seqOp, _default_reducer)
        combOp = redgrease.utils.passfun(combOp, seqOp)

        return OpenGearFunction(
            AggregateBy(
                extractor=extractor, zero=_zero, seqOp=seqOp, combOp=combOp, **kwargs
            ),
            input_function=self,
            requirements=requirements,
        )

    def groupby(
        self,
        extractor: Extractor[InputRecord, Key] = None,
        reducer: Reducer[Key, T, InputRecord] = None,
        # Other Redgrease args
        requirements: Iterable[str] = None,
        # Other Redis Gears args
        **kwargs,
    ) -> "OpenGearFunction[Dict[Key, T]]":
        """Cluster-local :ref:`op_groupby` operation performing a many-to-less (N:M)
        grouping of records.

        Args:
            extractor (:data:`redgrease.typing.Extractor`, optional):
                Function to apply on the input records, to extact the grouping key.
                The function must take one argument as input (input record) and
                return a string (key).
                The groups are defined by the value of the key.
                Defaults to the hash of the input.

            reducer (:data:`redgrease.typing.Reducer`, optional):
                Function to apply on the records of each group, to reduce to a single
                value (per group).
                The function must take (a) a key, (b) an input record and (c) a
                variable that's called an accumulator.
                It performs similarly to the accumulator callback, with the difference
                being that it maintains an accumulator per reduced key / group.
                Defaults to a list reducer.

            requirements (Iterable[str], optional):
                Additional requirements / dependency Python packages.
                Defaults to ``None``.

            **kwargs:
                Additional parameters to the :ref:`op_groupby` operation.

        Returns:
            OpenGearFunction:
                A new "open" gear function with a :ref:`op_groupby` operation as last
                step.
        """
        extractor = redgrease.utils.passfun(extractor, default=_default_extractor)
        reducer = redgrease.utils.passfun(reducer, default=_default_reducer)

        return OpenGearFunction(
            GroupBy(extractor=extractor, reducer=reducer, **kwargs),
            input_function=self,
            requirements=requirements,
        )

    def batchgroupby(
        self,
        extractor: Extractor[InputRecord, Key] = None,
        reducer: BatchReducer[Key, T, InputRecord] = None,
        # Other Redgrease args
        requirements: Iterable[str] = None,
        # Other Redis Gears args
        **kwargs,
    ) -> "OpenGearFunction[Dict[Key, T]]":
        """Cluster-local :ref:`op_groupby` operation, performing a many-to-less (N:M)
        grouping of records.

            Note: Using this operation may cause a substantial increase in memory usage
                during runtime. Consider using the GroupBy

        Args:
            extractor (:data:`redgrease.typing.Extractor`, optional):
                Function to apply on the input records, to extact the grouping key.
                The function must take one argument as input (input record) and
                return a string (key).
                The groups are defined by the value of the key.
                Defaults to the hash of the input.

            reducer (:data:`redgrease.typing.Reducer`):
                Function to apply on the records of each group, to reduce to a single
                value (per group).
                The function must take (a) a key, (b) an input record and (c) a
                variable that's called an accumulator.
                It performs similarly to the accumulator callback, with the difference
                being that it maintains an accumulator per reduced key / group.
                Default is the length (`len`) of the input.

            **kwargs:
                Additional parameters to the :ref:`op_groupby` operation.

        Returns:
            OpenGearFunction:
                A new "open" gear function with a :ref:`op_groupby` operation as last
                step.
        """
        extractor = redgrease.utils.passfun(extractor, default=_default_extractor)
        reducer = redgrease.utils.passfun(reducer, default=_default_batch_reducer)
        return OpenGearFunction(
            BatchGroupBy(extractor=extractor, reducer=reducer, **kwargs),
            input_function=self,
            requirements=requirements,
        )

    def sort(
        self,
        reverse: bool = True,
        # Other Redgrease args
        requirements: Iterable[str] = None,
        # Other Redis Gears args
        **kwargs,
    ) -> "OpenGearFunction[InputRecord]":
        """:ref:`op_sort` the records

        Args:
            reverse (bool, optional):
                Sort in descending order (higher to lower).
                Defaults to ``True``.

            requirements (Iterable[str], optional):
                Additional requirements / dependency Python packages.
                Defaults to ``None``.

            **kwargs:
                Additional parameters to the :ref:`op_sort` operation.

        Returns:
            OpenGearFunction:
                A new "open" gear function with a :ref:`op_sort` operation as last
                step.
        """
        return OpenGearFunction(
            Sort(reverse=reverse, **kwargs),
            input_function=self,
            requirements=requirements,
        )

    def distinct(self, **kwargs) -> "OpenGearFunction[InputRecord]":
        """Keep only the :ref:`op_distinct` values in the data.

        Args:
            **kwargs:
                Additional parameters to the :ref:`op_distinct` operation.

        Returns:
            OpenGearFunction:
                A new "open" gear function with a :ref:`op_distinct` operation as last
                step.
        """
        return OpenGearFunction(
            Distinct(**kwargs),
            input_function=self,
        )

    def count(self, **kwargs) -> "OpenGearFunction[int]":
        """:ref:`op_count` the number of records in the execution.

        Args:
            **kwargs:
                Additional parameters to the :ref:`op_count` operation.

        Returns:
            OpenGearFunction:
                A new "open" gear function with a :ref:`op_count` operation as last
                step.
        """
        return OpenGearFunction(
            Count(**kwargs),
            input_function=self,
        )

    def countby(
        self,
        extractor: Extractor[InputRecord, Hashable] = lambda x: str(x),
        # Other Redgrease args
        requirements: Iterable[str] = None,
        # Other Redis Gears args
        **kwargs,
    ) -> "OpenGearFunction[Dict[Hashable, int]]":
        """Distributed :ref:`op_countby` operation counting the records grouped by key.

        Args:
            extractor (:data:`redgrease.typing.Extractor`):
                Function to apply on the input records, to extact the grouping key.
                The function must take one argument as input (input record) and
                return a string (key).
                The groups are defined by the value of the key.
                Defaults to ``lambda x: str(x)``.

            requirements (Iterable[str], optional):
                Additional requirements / dependency Python packages.
                Defaults to ``None``.

            **kwargs:
                Additional parameters to the :ref:`op_countby` operation.

        Returns:
            OpenGearFunction:
                A new "open" gear function with a :ref:`op_countby` operation as last
                step.
        """

        return OpenGearFunction(
            CountBy(extractor=extractor, **kwargs),
            input_function=self,
            requirements=requirements,
        )

    def avg(
        self,
        extractor: Extractor[InputRecord, float] = lambda x: float(
            x if isinstance(x, (int, float, str)) else str(x)
        ),
        # Other Redgrease args
        requirements: Iterable[str] = None,
        # Other Redis Gears args
        **kwargs,
    ) -> "OpenGearFunction[float]":
        """Distributed :ref:`op_avg` operation, calculating arithmetic average
        of the records.

        Args:
            extractor (:data:`redgrease.typing.Extractor`):
                Function to apply on the input records, to extact the grouping key.
                The function must take one argument as input (input record) and
                return a string (key).
                The groups are defined by the value of the key.
                Defaults to ``lambda x: float(x)``.

            requirements (Iterable[str], optional):
                Additional requirements / dependency Python packages.
                Defaults to ``None``.

            **kwargs:
                Additional parameters to the :ref:`op_avg`  operation.

        Returns:
            OpenGearFunction:
                A new "open" gear function with an :ref:`op_avg`  operation as last
                step.
        """
        return OpenGearFunction(
            Avg(extractor=extractor, **kwargs),
            input_function=self,
            requirements=requirements,
        )
