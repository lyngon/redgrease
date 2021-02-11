from dataclasses import dataclass, field
from typing import Any, Dict, Optional

import redgrease.typing as optype
from redgrease.sugar import TriggerMode


# Redis Gears Operations
@dataclass
class Operation:
    pass


@dataclass
class Run(Operation):
    arg: Optional[str] = None
    convertToStr: bool = True
    collect: bool = True
    kargs: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Register(Operation):
    prefix: str = "*"
    convertToStr: bool = True
    collect: bool = True
    mode: str = TriggerMode.Async
    onRegistered: Optional[optype.Callback] = None
    trigger: Optional[str] = None
    kargs: Dict[str, Any] = field(default_factory=dict)


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
    op: optype.Accumulator[Any]


@dataclass
class LocalGroupBy(Operation):
    extractor: optype.Extractor
    reducer: optype.Reducer[Any]


@dataclass
class Limit(Operation):
    length: int
    start: int


@dataclass
class Collect(Operation):
    pass


@dataclass
class Repartition(Operation):
    extractor: optype.Extractor


@dataclass
class Aggregate(Operation):
    zero: Any
    seqOp: optype.Accumulator[Any]
    combOp: optype.Accumulator[Any]


@dataclass
class AggregateBy(Operation):
    extractor: optype.Extractor
    zero: Any
    seqOp: optype.Reducer[Any]
    combOp: optype.Reducer[Any]


@dataclass
class GroupBy(Operation):
    extractor: optype.Extractor
    reducer: optype.Reducer[Any]


@dataclass
class BatchGroupBy(Operation):
    extractor: optype.Extractor
    reducer: optype.BatchReducer[Any]


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
    extractor: optype.Extractor


@dataclass
class Avg(Operation):
    extractor: optype.Extractor
