import logging
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Generic,
    Hashable,
    Iterable,
    Optional,
    Type,
    TypeVar,
)

#
# import redgrease.operation as gearop
import redgrease.sugar as sugar

if TYPE_CHECKING:
    import redgrease.typing as optype

T = TypeVar("T")

logger = logging.getLogger(__name__)


class Operation:
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def add_to(self, function):
        raise NotImplementedError(f"Cannot add {self.__class__.__name__} to {function}")


class Reader(Operation):
    def __init__(
        self,
        reader: str,
        defaultArg: str,
        desc: Optional[str],
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.reader = reader
        self.defaultArg = defaultArg
        self.desc = desc

    def add_to(self, builder: Type):
        return builder(self.reader, self.defaultArg, self.desc, **self.kwargs)


class Run(Operation):
    def __init__(
        self,
        arg: Optional[str] = None,
        convertToStr: bool = True,
        collect: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.arg = arg
        self.convertToStr = convertToStr
        self.collect = collect

    def add_to(self, function: "PartialGearFunction"):
        import cloudpickle

        return function.map(lambda x: cloudpickle.dumps(x, protocol=4)).run(
            self.arg, False, self.collect, **self.kwargs
        )


class Register(Operation):
    def __init__(
        self,
        prefix: str = "*",
        convertToStr: bool = True,
        collect: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.prefix = prefix
        self.convertToStr = convertToStr
        self.collect = collect

    def add_to(self, function: "PartialGearFunction"):
        return function.register(
            self.prefix, self.convertToStr, self.collect, **self.kwargs
        )


class Map(Operation):
    def __init__(self, op: "optype.Mapper", **kwargs) -> None:
        super().__init__(**kwargs)
        self.op = op

    def add_to(self, function: "PartialGearFunction"):
        return function.map(self.op, **self.kwargs)


class FlatMap(Operation):
    def __init__(self, op: "optype.Expander", **kwargs) -> None:
        super().__init__(**kwargs)
        self.op = op

    def add_to(self, function: "PartialGearFunction"):
        return function.flatmap(self.op, **self.kwargs)


class ForEach(Operation):
    def __init__(self, op: "optype.Processor", **kwargs) -> None:
        super().__init__(**kwargs)
        self.op = op

    def add_to(self, function: "PartialGearFunction"):
        return function.foreach(self.op, **self.kwargs)


class Filter(Operation):
    def __init__(self, op: "optype.Filterer", **kwargs) -> None:
        super().__init__(**kwargs)
        self.op = op

    def add_to(self, function: "PartialGearFunction"):
        return function.filter(self.op, **self.kwargs)


class Accumulate(Operation):
    def __init__(self, op: "optype.Accumulator", **kwargs) -> None:
        super().__init__(**kwargs)
        self.op = op

    def add_to(self, function: "PartialGearFunction"):
        return function.accumulate(self.op, **self.kwargs)


class LocalGroupBy(Operation):
    def __init__(
        self, extractor: "optype.Extractor", reducer: "optype.Reducer", **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.extractor = extractor
        self.reducer = reducer

    def add_to(self, function: "PartialGearFunction"):
        return function.localgroupby(self.extractor, self.reducer, **self.kwargs)


class Limit(Operation):
    def __init__(self, length: int, start: int, **kwargs) -> None:
        super().__init__(**kwargs)
        self.length = length
        self.start = start

    def add_to(self, function: "PartialGearFunction"):
        return function.limit(self.length, self.start, **self.kwargs)


class Collect(Operation):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def add_to(self, function: "PartialGearFunction"):
        return function.collect(**self.kwargs)


class Repartition(Operation):
    def __init__(self, extractor: "optype.Extractor", **kwargs) -> None:
        super().__init__(**kwargs)
        self.extractor = extractor

    def add_to(self, function: "PartialGearFunction"):
        return function.repartition(self.extractor, **self.kwargs)


class Aggregate(Operation):
    def __init__(
        self,
        zero: Any,
        seqOp: "optype.Accumulator",
        combOp: "optype.Accumulator",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.zero = zero
        self.seqOp = seqOp
        self.combOp = combOp

    def add_to(self, function: "PartialGearFunction"):
        return function.aggregate(self.zero, self.seqOp, self.combOp, **self.kwargs)


class AggregateBy(Operation):
    def __init__(
        self,
        extractor: "optype.Extractor",
        zero: Any,
        seqOp: "optype.Reducer",
        combOp: "optype.Reducer",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.extractor = extractor
        self.zero = zero
        self.seqOp = seqOp
        self.combOp = combOp

    def add_to(self, function: "PartialGearFunction"):
        return function.aggregateby(
            self.extractor, self.zero, self.seqOp, self.combOp, **self.kwargs
        )


class GroupBy(Operation):
    def __init__(
        self, extractor: "optype.Extractor", reducer: "optype.Reducer", **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.extractor = extractor
        self.reducer = reducer

    def add_to(self, function: "PartialGearFunction"):
        return function.groupby(self.extractor, self.reducer, **self.kwargs)


class BatchGroupBy(Operation):
    def __init__(
        self, extractor: "optype.Extractor", reducer: "optype.BatchReducer", **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.extractor = extractor
        self.reducer = reducer

    def add_to(self, function: "PartialGearFunction"):
        return function.batchgroupby(self.extractor, self.reducer, **self.kwargs)


class Sort(Operation):
    def __init__(self, reverse: bool = True, **kwargs) -> None:
        super().__init__(**kwargs)
        self.reverse = reverse

    def add_to(self, function: "PartialGearFunction"):
        return function.sort(self.reverse, **self.kwargs)


class Distinct(Operation):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def add_to(self, function: "PartialGearFunction"):
        return function.distinct(**self.kwargs)


class Count(Operation):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def add_to(self, function: "PartialGearFunction"):
        return function.count(**self.kwargs)


class CountBy(Operation):
    def __init__(self, extractor: "optype.Extractor", **kwargs) -> None:
        super().__init__(**kwargs)
        self.extractor = extractor

    def add_to(self, function: "PartialGearFunction"):
        return function.countby(self.extractor, **self.kwargs)


class Avg(Operation):
    def __init__(self, extractor: "optype.Extractor", **kwargs) -> None:
        super().__init__(**kwargs)
        self.extractor = extractor

    def add_to(self, function: "PartialGearFunction"):
        return function.avg(self.extractor, **self.kwargs)


class GearFunction(Generic[T]):
    def __init__(self, operation: Operation, input_function: "GearFunction" = None):
        self.input_function: Optional[GearFunction] = input_function
        self.operation = operation

    @property
    def reader(self):
        if isinstance(self.operation, Reader):
            return self.operation.reader

        if self.input_function:
            return self.input_function.reader

        return None

    @property
    def supports_batch_mode(self):
        return self.reader in [
            sugar.Reader.KeysReader,
            sugar.Reader.KeysOnlyReader,
            sugar.Reader.StreamReader,
            sugar.Reader.PythonReader,
            sugar.Reader.ShardsIDReader,
        ]

    @property
    def supports_event_mode(self):
        return self.reader in [
            sugar.Reader.KeysReader,
            sugar.Reader.StreamReader,
            sugar.Reader.CommandReader,
        ]


class ClosedGearFunction(GearFunction[T]):
    def on(
        self,
        gears_server,
        unblocking: bool = False,
        requirements: Iterable[str] = None,
    ):
        if hasattr(gears_server, "gears"):
            gears_server = gears_server.gears
        if not hasattr(gears_server, "pyexecute"):
            from redgrease.client import Gears

            gears_server = Gears(gears_server)

        return gears_server.pyexecute(
            self, unblocking=unblocking, requirements=requirements
        )


class PartialGearFunction(GearFunction["optype.InputRecord"]):
    def run(
        self,
        arg: str = None,  # TODO: This can also be a Python generator
        convertToStr: bool = False,
        collect: bool = False,
        # Helpers, all must be None
        # Other args
        **kwargs,
        # TODO: Add all the Reader specific args here
    ) -> ClosedGearFunction["optype.InputRecord"]:
        """Runs a gear function as a batch.
        The function is executed once and exits once the data is
        exhausted by its reader.
        Args:
            arg (str, optional): An optional argument that's passed to
                the reader as its defaultArg.
                It means the following:
                - A glob-like pattern for the KeysReader
                and KeysOnlyReader readers.
                - A key name for the StreamReader reader.
                - A Python generator for the PythonReader reader.
                Defaults to None.

            convertToStr (bool, optional): When `True` adds a map
                operation to the flow's end that stringifies records.
                Defaults to False.

            collect (bool, optional): When `True` adds a collect operation
                to flow's end.
                Defaults to False.
        Returns:s
            [type]: [description]
        """
        if not self.supports_batch_mode:
            raise TypeError(f"Batch mode (run) is not supporterd for '{self.reader}'")

        return ClosedGearFunction(
            Run(arg=arg, convertToStr=convertToStr, collect=collect, **kwargs),
            input_function=self,
        )

    def register(
        self,
        prefix: str = "*",
        convertToStr: bool = False,
        collect: bool = False,
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
        # Other args
        **kwargs,
        # TODO: Add all the Reader specific args here
    ) -> ClosedGearFunction["optype.OutputRecord"]:
        """Runs a Gear function as an event handler.
        The function is executed each time an event arrives.
        Each time it is executed, the function operates on the event's
        data and once done is suspended until its future invocations by
        new events.
        Args:
            prefix (str, optional): Key prefix pattern to match on.
                Not relevant for 'CommandReader' readers (see 'trigger').
                Defaults to '*'.

            convertToStr (bool, optional): When `True` adds a map
                operation to the flow's end that stringifies records.
                Defaults to True.
                collect (bool, optional): When True adds a collect operation
                to flow's end.
                Defaults to False.

            mode (str, optional): The execution mode of the function.
                Can be one of:
                - 'async': Execution will be asynchronous across the entire
                cluster.
                - 'async_local': Execution will be asynchronous and restricted
                to the handling shard.
                - 'sync': Execution will be synchronous and local.
                Defaults to 'async'.

            onRegistered (Callback, optional): A function callback that's
                called on each shard upon function registration.
                It is a good place to initialize non-serializable objects
                such as network connections.
                Defaults to None.

            trigger (str, optional): For 'CommandReader' only.
                The trigger string that will trigger the function.
                Defaults to None.
        Returns:
            [type]: [description]
        """

        if mode is not None:
            kwargs["mode"] = mode

        if onRegistered is not None:
            kwargs["onRegistered"] = onRegistered

        if not self.supports_event_mode:
            raise TypeError(f"Event mode (run) is not supporterd for '{self.reader}'")

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

        if onFailedPolcy is not None:
            kwargs["onFailedPolicy"] = onFailedPolcy

        if onFailedRetryInterval is not None:
            kwargs["onFailedRetryInterval"] = onFailedRetryInterval

        if trimStream is not None:
            kwargs["trimStream"] = trimStream

        if trigger is not None:
            kwargs["trigger"] = trigger

        return ClosedGearFunction(
            Register(
                prefix=prefix,
                convertToStr=convertToStr,
                collect=collect,
                **kwargs,
            ),
            input_function=self,
        )

    def map(
        self,
        op: "optype.Mapper[optype.InputRecord, optype.OutputRecord]",
        **kwargs,
    ) -> "PartialGearFunction[optype.OutputRecord]":
        """Instance-local Map operation that performs a one-to-one
        (1:1) mapping of records.
        Args:
            op (Mapper): Mapper function (1:1)
        Returns:
            A gear flow outputting the transformed records
        """
        return PartialGearFunction(
            Map(op=op, **kwargs),
            input_function=self,
        )

    def flatmap(
        self,
        op: "optype.Expander[optype.InputRecord, optype.OutputRecord]",
        **kwargs,
    ) -> "PartialGearFunction[Iterable[optype.OutputRecord]]":
        """Instance-local FlatMap operation that performs one-to-many
        (1:N) mapping of records.
        It requires one expander callback that maps a single input record
        to one or more output records.
        FlatMap is nearly identical to the Map operation.
        Unlike regular mapping, however, when FlatMap returns a list,
        each element in the list is turned into a separate output record.
        Args:
            op (Expander): Expader function (1:N)
        Returns:
            A gear flow outputting the expanded records
        """
        return PartialGearFunction(
            FlatMap(op=op, **kwargs),
            input_function=self,
        )

    def foreach(
        self, op: "optype.Processor[optype.InputRecord]", **kwargs
    ) -> "PartialGearFunction[optype.InputRecord]":
        """Instance-local ForEach operation performs one-to-the-same
        (1=1) mapping.
        It requires one processor callback to perform some work that's
        related to the input record.
        Its output record is a copy of the input, which means anything
        the callback returns is discarded
        Args:
            op (Processor): Processor functon to be used on each record.
        Returns:
            A gear flow outputting the **input** records unmodified
        """
        return PartialGearFunction(
            ForEach(op=op, **kwargs),
            input_function=self,
        )

    def filter(
        self, op: "optype.Filterer[optype.InputRecord]", **kwargs
    ) -> "PartialGearFunction[optype.InputRecord]":
        """Instance-local Filter operation performs one-to-zero-or-one
        (1:bool) filtering of records.
        It requires a filterer function callback.
        An input record that yields a falsehood will be discarded and
        only truthful ones will be output.
        Args:
            op (Filterer): Filteriong / predicate function
        Returns:
            A gear flow outputting only the records tha pass the
            filter (i.e evaluates to 'True')
        """
        return PartialGearFunction(
            Filter(op=op, **kwargs),
            input_function=self,
        )

    def accumulate(
        self, op: "optype.Accumulator[T, optype.InputRecord]", **kwargs
    ) -> "PartialGearFunction[T]":
        """Instance-local Accumulate operation performs many-to-one
        mapping (N:1) of records.
        It requires one accumulator callback.
        Once input records are exhausted its output is a single record
        consisting of the accumulator's value.
        Args:
            op (Accumulator): Accumulator function
        Returns:
            A gear flow outputting the final accumulated value
        """
        return PartialGearFunction(
            Accumulate(op=op, **kwargs),
            input_function=self,
        )

    def localgroupby(
        self,
        extractor: "optype.Extractor[optype.InputRecord, optype.Key]",
        reducer: "optype.Reducer[optype.Key, T, optype.InputRecord]",
        **kwargs,
    ) -> "PartialGearFunction[Dict[optype.Key, T]]":
        """Instance-local LocalGroupBy operation performs many-to-less
        mapping (N:M) of records.
        The operation requires two callbacks: an extractor and a reducer.
        The output records consist of the grouping key and its respective
        accumulator's value.
        Args:
            extractor (Extractor): Extractor funtction that extracts
            the key that will be used to group by, from each record.
            reducer (Reducer): Reducer function that reduce the records of
            each 'group' into a single value for the group.
        Returns:
            A gear flow outputting each group and its reduced value
        """
        return PartialGearFunction(
            LocalGroupBy(extractor=extractor, reducer=reducer, **kwargs),
            input_function=self,
        )

    def limit(
        self, length: int, start: int = 0, **kwargs
    ) -> "PartialGearFunction[optype.InputRecord]":
        """Instance-local Limit operation limits the number of records.
        It accepts two numeric arguments: a starting position in the input
        records "array" and a maximal number of output records.
        Args:
            length (int): Maximum number of outputted records.
            start (int, optional): 0-based offset / index of the first
            record that output should start from.
            Defaults to 0.
        Returns:
            A gear flow outputting the records between the start
            record index (inclusive) and start+length record index
            (exclusive).
        """
        return PartialGearFunction(
            Limit(length=length, start=start, **kwargs),
            input_function=self,
        )

    def collect(self, **kwargs) -> "PartialGearFunction[optype.InputRecord]":
        """Cluster-global Collect operation collects the result records
        It has no arguments.
        Returns:
            A gear flow outputting the input records across all shards.
        """
        return PartialGearFunction(
            Collect(**kwargs),
            input_function=self,
        )

    def repartition(
        self, extractor: "optype.Extractor[optype.InputRecord, Hashable]", **kwargs
    ) -> "PartialGearFunction[optype.InputRecord]":
        """Cluster-global Repartition operation repartitions the records
        by them shuffling between shards.
        It accepts a single key extractor function callback.
        The extracted key is used for computing the record's new placement
        in the cluster (i.e. hash slot).
        The operation then moves the record from its original shard
        to the new one.
        Args:
            extractor (Extractor): [description]
        Returns:
            A gear flow outputting the input records, but each record
            is repartitioned to the shards as per the extractor function.
        """
        return PartialGearFunction(
            Repartition(extractor=extractor, **kwargs),
            input_function=self,
        )

    def aggregate(
        self,
        zero: T,
        seqOp: "optype.Accumulator[T, optype.InputRecord]",
        combOp: "optype.Accumulator[T, T]",
        **kwargs,
    ) -> "PartialGearFunction[T]":
        """Perform aggregation on all the execution data.
        Args:
            zero (T): The first value that will pass to the aggregation
            function.
            seqOp (Accumulator[T, InputRecord]): The local aggregate function
            (will be performed on each shard)
            combOp (Accumulator[T, T]): The global aggregate function
            (will be performed on the results of seqOp from each shard)
        Returns:
            A gear flow with the output of the
            'combOp' aggregation as output.
        """
        return PartialGearFunction(
            Aggregate(zero=zero, seqOp=seqOp, combOp=combOp, **kwargs),
            input_function=self,
        )

    def aggregateby(
        self,
        extractor: "optype.Extractor[optype.InputRecord, optype.Key]",
        zero: T,
        seqOp: "optype.Reducer[optype.Key, T, optype.InputRecord]",
        combOp: "optype.Reducer[optype.Key, T, T]",
        **kwargs,
    ) -> "PartialGearFunction[Dict[optype.Key, T]]":
        """Like aggregate, but on each key, the key is extracted using the extractor.
        Args:
            extractor (Extractor[Key]): A function that get as input the
            record and return the aggregated key.
            zero (T): The first value that will pass to the aggregation
            function.
            seqOp (Reducer[T]): The local aggregate function
            (will be performed on each shard)
            combOp (Reducer[T]): The global aggregate function
            (will be performed on the results of seqOp from each shard)
        Returns:
            [T]: The gear flow with the output of the
            'combOp' aggregation as output.
        """
        return PartialGearFunction(
            AggregateBy(
                extractor=extractor, zero=zero, seqOp=seqOp, combOp=combOp, **kwargs
            ),
            input_function=self,
        )

    def groupby(
        self,
        extractor: "optype.Extractor[optype.InputRecord, optype.Key]",
        reducer: "optype.Reducer[optype.Key, T, optype.InputRecord]",
        **kwargs,
    ) -> "PartialGearFunction[Dict[optype.Key, T]]":
        """performs a many-to-less (N:M) grouping of records.
        It is similar to AggregateBy but uses only a global reducer.
        It can be used in cases where locally reducing the data isn't
        possible.
        The operation requires two callbacks: an extractor a reducer .
        The operation is made of these steps:
        1. A global repartition operation that uses the extractor
        2. The reducer is locally invoked
        Args:
            extractor (Extractor[Key]): Key extraction function
            reducer (Reducer[T]): Reducer function
        Returns:
            A gear flow outputting a locally-reduced list of records,
            one for each key.
            The output records consist of the grouping key and its
            respective accumulator's value.
        """
        return PartialGearFunction(
            GroupBy(extractor=extractor, reducer=reducer, **kwargs),
            input_function=self,
        )

    def batchgroupby(
        self,
        extractor: "optype.Extractor[optype.InputRecord, optype.Key]",
        reducer: "optype.BatchReducer[optype.Key, T, optype.InputRecord]",
        **kwargs,
    ) -> "PartialGearFunction[Dict[optype.Key, T]]":
        """Many-to-less (N:M) grouping of records.
        Note: Using this operation may cause a substantial increase in
        memory usage during runtime.
        Instead of using BatchGroupBy, prefer using the GroupBy
        operation as it is more efficient and performant.
        Only use BatchGroupBy when the reducer's logic requires the full
        list of records for each input key.
        The operation requires two callbacks: an extractor a batch reducer.
        The operation is made of these steps:
        1. A global repartition operation that uses the extractor
        2. A local localgroupby operation that uses the batch reducer
        Once finished, the operation locally outputs a record for each key
        and its respective accumulator value.
        Args:
            extractor (Extractor[Key]): Key extractor function.
            reducer (BatchReducer[T]): Batch reducer function.
        Returns:
            A gear flow outputting a record for each key
        and its respective accumulator value.
        """
        return PartialGearFunction(
            BatchGroupBy(extractor=extractor, reducer=reducer, **kwargs),
            input_function=self,
        )

    def sort(
        self, reverse: bool = True, **kwargs
    ) -> "PartialGearFunction[optype.InputRecord]":
        """Sorting the records
        Note: Using this operation may cause an increase in memory usage
        during runtime due to the list being copied during the sorting
        operation.
        It accepts a single Boolean argument that determines the order.
        The operation is made of the following steps:
        1. A global aggregate operation collects and combines all records
        2. A local sort is performed on the list
        3. The list is flatmapped to records
        Args:
            reverse (bool, optional): Sort in descending order.
            Defaults to True.
        Returns:
            A gear flow outputting the input recods in sorted order.
        """
        return PartialGearFunction(
            Sort(reverse=reverse, **kwargs),
            input_function=self,
        )

    def distinct(self, **kwargs) -> "PartialGearFunction[optype.InputRecord]":
        """Keep only the distinct values in the data.
        It requires no arguments.
        The operation is made of the following steps:
        1. A aggregate operation locally reduces the records to sets that
        are then collected and unionized globally
        2. A local flatmap operation turns the set into records
        Returns:
            A gear flow outputting the unique records, discaring duplicates
        """
        return PartialGearFunction(
            Distinct(**kwargs),
            input_function=self,
        )

    def count(self, **kwargs) -> "PartialGearFunction[int]":
        """Count the number of records in the execution
        It requires no arguments.
        The operation is made of an aggregate operation that uses local
        counting and global summing accumulators.
        Returns:
            A gear flow outputting the number of input recods (int)
        """
        return PartialGearFunction(
            Count(**kwargs),
            input_function=self,
        )

    def countby(
        self,
        extractor: "optype.Extractor[optype.InputRecord, Hashable]" = lambda x: str(x),
        **kwargs,
    ) -> "PartialGearFunction[Dict[Hashable, int]]":
        """Counts the records grouped by key.
        It requires a single extractor function callback.
        The operation is made of an aggregateby operation that uses
        local counting and global summing accumulators.
        Args:
            extractor (Extractor[Key], optional): A function that
            get as input the record and return the key by which to
            performthe counting.
            Defaults to identity function  'lambda x: x'.
        Returns:
            A Gear flow outputting the number of items
            per extracted value.
        """
        return PartialGearFunction(
            CountBy(extractor=extractor, **kwargs),
            input_function=self,
        )

    def avg(
        self,
        extractor: "optype.Extractor[optype.InputRecord, float]" = lambda x: float(x),
        **kwargs,
    ) -> "PartialGearFunction[float]":
        """Calculating arithmetic average of the records
        It accepts an optional value extractor function callback.
        The operation is made of the following steps:
        1. A aggregate operation locally reduces the records to
        tuples of sum and count that are globally combined.
        2. A local map operation calculates the average
        Args:
            extractor (Extractor[Key], optional): A function that
            gets the record and return the value by which to
            calculate the average.
            Defaults to identity function  'lambda x: float(x)'.
        Returns:
            [float]: A gear flow outputting the average
            of the extracted values
        """
        return PartialGearFunction(
            Avg(extractor=extractor, **kwargs),
            input_function=self,
        )
