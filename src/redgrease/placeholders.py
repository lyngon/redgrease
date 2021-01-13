import logging
import redgrease.operations as gearop
from redgrease.sugar import Reader, TriggerMode, LogLevel
from redgrease.typing import (
    T, Key, Callback, Processor, Filterer, Accumulator,
    Reducer, BatchReducer, Extractor, Mapper, Expander
)


logger = logging.getLogger(__name__)


class atomic:
    def __enter__(self):
        pass

    def __exit__(self, ex_type, ex_value, ex_traceback):
        pass


def execute(command: str, *args) -> str:
    '''execute redis command'''
    # TODO: Call some default Redis Server?
    pass


def hashtag() -> str:
    pass


def log(message: str, level: str = LogLevel.Notice):
    logger.log(level=LogLevel.to_logging_level(level), msg=message)


def configGet(key: str):
    pass


def gearsConfigGet(key: str, default=None):
    pass


class GearsBuilder:
    def __init__(
        self,
        reader: str = Reader.KeysReader,
        defaultArgs: str = '*',
        desc: str = None,
    ):
        """Gear process

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

            defaultArgs (str, optional): Additional arguments
            to the reader.
            These are usually a key's name, prefix, glob-like
            or a regular expression.
            Its use depends on the function's reader type and action.
            Defaults to '*'.

            desc (str, optional): An optional description.
            Defaults to None.
        """
        self.reader = reader
        self.defaultArgs = defaultArgs
        self.desc = desc
        self.operations = []

    def run(
        self,
        arg: str = None,
        convertToStr: bool = True,
        collect: bool = True,
        **kargs
    ):
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
            Defaults to True.

            collect (bool, optional): When `True` adds a collect operation
            to flow's end.
            Defaults to True.

        Returns:
            [type]: [description]
        """
        self.operations.append(
            gearop.Run(
                arg=arg,
                convertToStr=convertToStr,
                collect=collect,
                kargs=kargs
            )
        )
        return self

    def register(
        self,
        prefix: str = '*',
        convertToStr: bool = True,
        collect: bool = True,
        mode: str = TriggerMode.Async,
        onRegistered: Callback = None,
        trigger: str = None,
        **kargs
    ):
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
            Defaults to True.

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
        self.operations.append(
            gearop.Register(
                prefix=prefix,
                convertToStr=convertToStr,
                collect=collect,
                mode=mode,
                onRegistered=onRegistered,
                trigger=trigger,
                kargs=kargs
            )
        )
        return self

    def map(self, op: Mapper):
        """Instance-local Map operation that performs a one-to-one
        (1:1) mapping of records.

        Args:
            op (Mapper): Mapper function (1:1)

        Returns:
            A gear flow outputting the transformed records
        """
        self.operations.append(
            gearop.Map(op=op)
        )
        return self

    def flatmap(self, op: Expander):
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
        self.operations.append(
            gearop.FlatMap(op=op)
        )
        return self

    def foreach(self, op: Processor):
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
        self.operations.append(
            gearop.ForEach(op=op)
        )
        return self

    def filter(self, op: Filterer):
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
        self.operations.append(
            gearop.Filter(op=op)
        )
        return self

    def accumulate(self, op: Accumulator):
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
        self.operations.append(
            gearop.Accumulate(op=op)
        )
        return self

    def localgroupby(self, extractor: Extractor[Key], reducer: Reducer):
        """Instance-local LocalGroupBy operation performs many-to-less
        mapping (N:M) of records.

        The operation requires two callbacks: an extractor and a reducer.

        The output records consist of the grouping key and its respective
        accumulator's value.

        Args:
            extractor (Extractor[Key]): Extractor funtction that extracts
            the key that will be used to group by, from each record.

            reducer (Reducer): Reducer function that reduce the records of
            each 'group' into a single value for the group.

        Returns:
            A gear flow outputting each group and its reduced value
        """
        self.operations.append(
            gearop.LocalGroupBy(extractor=extractor, reducer=reducer)
        )
        return self

    def limit(self, length: int, start: int = 0):
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
        self.operations.append(
            gearop.Limit(length=length, start=start)
        )
        return self

    def collect(self):
        """Cluster-global Collect operation collects the result records
        from all of the shards to the originating one.

        It has no arguments.

        Returns:
            A gear flow outputting the input records across all shards.
        """
        self.operations.append(
            gearop.Collect()
        )
        return self

    def repartition(self, extractor: Extractor[Key]):
        """Cluster-global Repartition operation repartitions the records
        by them shuffling between shards.

        It accepts a single key extractor function callback.
        The extracted key is used for computing the record's new placement
        in the cluster (i.e. hash slot).
        The operation then moves the record from its original shard
        to the new one.

        Args:
            extractor (Extractor[Key]): [description]

        Returns:
            A gear flow outputting the input records, but each record
            is repartitioned to the shards as per the extractor function.
        """
        self.operations.append(
            gearop.Repartition(extractor=extractor)
        )
        return self

    def aggregate(
        self,
        zero: T,
        seqOp: Accumulator[T],
        combOp: Accumulator[T]
    ):
        """Perform aggregation on all the execution data.

        Args:
            zero (T): The first value that will pass to the aggregation
            function.

            seqOp (Accumulator[T]): The local aggregate function
            (will be performed on each shard)

            combOp (Accumulator[T]): The global aggregate function
            (will be performed on the results of seqOp from each shard)

        Returns:
            A gear flow with the output of the
            'combOp' aggregation as output.
        """
        self.operations.append(
            gearop.Aggregate(zero=zero, seqOp=seqOp, combOp=combOp)
        )
        return self

    def aggregateby(
        self,
        extractor: Extractor[Key],
        zero: T,
        seqOp: Reducer[T],
        combOp: Reducer[T]
    ):
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
        self.operations.append(
            gearop.AggregateBy(
                extractor=extractor,
                zero=zero,
                seqOp=seqOp,
                combOp=combOp
            )
        )
        return self

    def groupby(self, extractor: Extractor[Key], reducer: Reducer[T]):
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
        self.operations.append(
            gearop.GroupBy(extractor=extractor, reducer=reducer)
        )
        return self

    def batchgroupby(
        self,
        extractor: Extractor[Key],
        reducer: BatchReducer[T]
    ):
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
        self.operations.append(
            gearop.BatchGroupBy(extractor=extractor, reducer=reducer)
        )
        return self

    def sort(self, reverse: bool = True):
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
        self.operations.append(
            gearop.Sort(reverse=reverse)
        )
        return self

    def distinct(self):
        """Keep only the distinct values in the data.

        It requires no arguments.

        The operation is made of the following steps:
        1. A aggregate operation locally reduces the records to sets that
        are then collected and unionized globally
        2. A local flatmap operation turns the set into records

        Returns:
            A gear flow outputting the unique records, discaring duplicates
        """
        self.operations.append(
            gearop.Distinct()
        )
        return self

    def count(self):
        """Count the number of records in the execution

        It requires no arguments.

        The operation is made of an aggregate operation that uses local
        counting and global summing accumulators.

        Returns:
            A gear flow outputting the number of input recods (int)
        """
        self.operations.append(
            gearop.Count()
        )
        return self

    def countby(self, extractor: Extractor[Key] = lambda x: x):
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
        self.operations.append(
            gearop.CountBy(extractor=extractor)
        )
        return self

    def avg(self, extractor: Extractor[Key] = lambda x: float(x)):
        """Calculating arithmetic average of the records

        It accepts an optional value extractor function callback.

        The operation is made of the following steps:
        1. A aggregate operation locally reduces the records to
        tuples of sum and count that are globally combined.
        2. A local map operation calculates the average
        from the global tuple

        Args:
            extractor (Extractor[Key], optional): A function that
            gets the record and return the value by which to
            calculate the average.
            Defaults to identity function  'lambda x: float(x)'.

        Returns:
            [float]: A gear flow outputting the average
            of the extracted values
        """
        self.operations.append(
            gearop.Avg(extractor=extractor)
        )
        return self


GB = GearsBuilder
