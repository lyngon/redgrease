from dataclasses import dataclass, field
from redgrease.sugar import TriggerMode
from redgrease.typing import T, Key
import redgrease.typing as optype


# Redis Gears Operations
# Should maybe be in a separate sub-package 'redgrease.operations'
@dataclass
class Operation:
    pass


@dataclass
class Run(Operation):
    arg: str = None
    convertToStr: bool = True
    collect: bool = True
    kargs: dict = field(default_factory=dict)


@dataclass
class Register(Operation):
    prefix: str = '*'
    convertToStr: bool = True
    collect: bool = True
    mode: str = TriggerMode.Async
    onRegistered: optype.Callback = None
    trigger: str = None
    kargs: dict = field(default_factory=dict)


@dataclass
class Map(Operation):
    op: optype.Mapper


@dataclass
class FlatMap(Operation):
    op: optype.Expander


@dataclass
class ForEach(Operation):
    op: optype.Processor


@dataclass
class Filter(Operation):
    op: optype.Filterer


@dataclass
class Accumulate(Operation):
    op: optype.Accumulator


@dataclass
class LocalGroupBy(Operation):
    extractor: optype.Extractor[Key]
    reducer: optype.Reducer


@dataclass
class Limit(Operation):
    length: int
    start: int


@dataclass
class Collect(Operation):
    pass


@dataclass
class Repartition(Operation):
    extractor: optype.Extractor[Key]


@dataclass
class Aggregate(Operation):
    zero: T
    seqOp: optype.Accumulator[T]
    combOp: optype.Accumulator[T]


@dataclass
class AggregateBy(Operation):
    extractor: optype.Extractor[Key]
    zero: T
    seqOp: optype.Reducer[T]
    combOp: optype.Reducer[T]


@dataclass
class GroupBy(Operation):
    extractor: optype.Extractor[Key]
    reducer: optype.Reducer[T]


@dataclass
class BatchGroupBy(Operation):
    extractor: optype.Extractor[Key]
    reducer: optype.BatchReducer[T]


@dataclass
class Sort(Operation):
    reverse: bool = True


@dataclass
class Distinct(Operation):
    pass


@dataclass
class Count(Operation):
    pass


@dataclass
class CountBy(Operation):
    extractor: optype.Extractor[Key]


@dataclass
class Avg(Operation):
    extractor: optype.Extractor[Key]
