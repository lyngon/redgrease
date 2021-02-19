import logging
from typing import TYPE_CHECKING, Dict, Generic, Iterable, Optional

import redgrease.operation as gearop
import redgrease.sugar as sugar

if TYPE_CHECKING:
    import redgrease.runtime as runtime
    import redgrease.typing as optype

from redgrease.typing import Key, T

logger = logging.getLogger(__name__)


class GearFunction(Generic[T]):
    def __init__(
        self, operation: gearop.Operation, input_function: "GearFunction" = None
    ):
        self.input_function: Optional[GearFunction] = input_function
        self.operation = operation

    @property
    def reader(self):
        if isinstance(self.operation, gearop.Reader):
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


def runtime_build(function: GearFunction, builder):

    if function.input_function:
        input_builder = runtime_build(function.input_function, builder)
    else:
        input_builder = builder

    return function.operation.add_to(input_builder)


class ClosedGearFunction(GearFunction[T]):
    def compile(self, builder: "runtime.GearsBuilder"):
        return runtime_build(self, builder)

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

        gears_server.pyexecute(self, unblocking, requirements)


class PartialGearFunction(GearFunction["optype.InputRecord"]):
    def run(
        self,
        arg: str = None,  # TODO: This can also be a Python generator
        convertToStr: bool = False,
        collect: bool = False,
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
            gearop.Run(
                arg=arg, convertToStr=convertToStr, collect=collect, kwargs=kwargs
            ),
            input_function=self,
        )

    def register(
        self,
        prefix: str = "*",  # Reader Specific: ...
        convertToStr: bool = False,
        collect: bool = False,
        mode: str = sugar.TriggerMode.Async,
        onRegistered: "optype.Callback" = None,
        trigger: str = None,  # Reader Specific: CommandReader
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

        if not self.supports_event_mode:
            raise TypeError(f"Event mode (run) is not supporterd for '{self.reader}'")

        return ClosedGearFunction(
            gearop.Register(
                prefix=prefix,
                convertToStr=convertToStr,
                collect=collect,
                mode=mode,
                onRegistered=onRegistered,
                trigger=trigger,
                kwargs=kwargs,
            ),
            input_function=self,
        )

    def map(
        self, op: "optype.Mapper[optype.InputRecord, optype.OutputRecord]"
    ) -> "PartialGearFunction[optype.OutputRecord]":
        """Instance-local Map operation that performs a one-to-one
        (1:1) mapping of records.
        Args:
            op (Mapper): Mapper function (1:1)
        Returns:
            A gear flow outputting the transformed records
        """
        return PartialGearFunction(gearop.Map(op=op), input_function=self)

    def flatmap(
        self, op: "optype.Expander[optype.InputRecord, optype.OutputRecord]"
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
        return PartialGearFunction(gearop.FlatMap(op=op), input_function=self)

    def foreach(
        self, op: "optype.Processor[optype.InputRecord]"
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
        return PartialGearFunction(gearop.ForEach(op=op), input_function=self)

    def filter(
        self, op: "optype.Filterer[optype.InputRecord]"
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
        return PartialGearFunction(gearop.Filter(op=op), input_function=self)

    def accumulate(
        self, op: "optype.Accumulator[T, optype.InputRecord]"
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
        return PartialGearFunction(gearop.Accumulate(op=op), input_function=self)

    def localgroupby(
        self,
        extractor: "optype.Extractor[Key]",
        reducer: "optype.Reducer[Key, T, optype.InputRecord]",
    ) -> "PartialGearFunction[Dict[Key, T]]":
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
            gearop.LocalGroupBy(extractor=extractor, reducer=reducer),
            input_function=self,
        )

    def limit(
        self, length: int, start: int = 0
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
            gearop.Limit(length=length, start=start), input_function=self
        )

    def collect(self) -> "PartialGearFunction[optype.InputRecord]":
        """Cluster-global Collect operation collects the result records
        It has no arguments.
        Returns:
            A gear flow outputting the input records across all shards.
        """
        return PartialGearFunction(gearop.Collect(), input_function=self)

    def repartition(
        self, extractor: "optype.Extractor[optype.InputRecord]"
    ) -> "PartialGearFunction[str]":
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
            gearop.Repartition(extractor=extractor), input_function=self
        )

    def aggregate(
        self,
        zero: T,
        seqOp: "optype.Accumulator[T, optype.InputRecord]",
        combOp: "optype.Accumulator[T, T]",
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
            gearop.Aggregate(zero=zero, seqOp=seqOp, combOp=combOp),
            input_function=self,
        )

    def aggregateby(
        self,
        extractor: "optype.Extractor[Key]",
        zero: T,
        seqOp: "optype.Reducer[Key, T, optype.InputRecord]",
        combOp: "optype.Reducer[Key, T, T]",
    ) -> "PartialGearFunction[Dict[Key, T]]":
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
            gearop.AggregateBy(
                extractor=extractor, zero=zero, seqOp=seqOp, combOp=combOp
            ),
            input_function=self,
        )

    def groupby(
        self,
        extractor: "optype.Extractor[Key]",
        reducer: "optype.Reducer[Key, T, optype.InputRecord]",
    ) -> "PartialGearFunction[Dict[Key, T]]":
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
            gearop.GroupBy(extractor=extractor, reducer=reducer), input_function=self
        )

    def batchgroupby(
        self,
        extractor: "optype.Extractor[Key]",
        reducer: "optype.BatchReducer[Key, T, optype.InputRecord]",
    ) -> "PartialGearFunction[Dict[Key, T]]":
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
            gearop.BatchGroupBy(extractor=extractor, reducer=reducer),
            input_function=self,
        )

    def sort(self, reverse: bool = True) -> "PartialGearFunction[optype.InputRecord]":
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
        return PartialGearFunction(gearop.Sort(reverse=reverse), input_function=self)

    def distinct(self) -> "PartialGearFunction[optype.InputRecord]":
        """Keep only the distinct values in the data.
        It requires no arguments.
        The operation is made of the following steps:
        1. A aggregate operation locally reduces the records to sets that
        are then collected and unionized globally
        2. A local flatmap operation turns the set into records
        Returns:
            A gear flow outputting the unique records, discaring duplicates
        """
        return PartialGearFunction(gearop.Distinct(), input_function=self)

    def count(self) -> "PartialGearFunction[int]":
        """Count the number of records in the execution
        It requires no arguments.
        The operation is made of an aggregate operation that uses local
        counting and global summing accumulators.
        Returns:
            A gear flow outputting the number of input recods (int)
        """
        return PartialGearFunction(gearop.Count(), input_function=self)

    def countby(
        self, extractor: "optype.Extractor[optype.InputRecord]" = lambda x: str(x)
    ) -> "PartialGearFunction[Dict[str, int]]":
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
            gearop.CountBy(extractor=extractor), input_function=self
        )

    def avg(
        self, extractor: "optype.Extractor[optype.InputRecord]" = lambda x: str(x)
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
        return PartialGearFunction(gearop.Avg(extractor=extractor), input_function=self)
