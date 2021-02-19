from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, Optional, Type

if TYPE_CHECKING:
    from redgrease.gears import PartialGearFunction
    import redgrease.typing as optype

from redgrease.sugar import TriggerMode


@dataclass
class Operation:
    pass

    def add_to(self, function):
        raise NotImplementedError("Cannot add genric operation to Gear function")


@dataclass
class Reader(Operation):
    reader: str
    defaultArg: str
    desc: Optional[str]

    def add_to(self, builder: Type):
        return builder(self.reader, self.defaultArg, self.desc)


@dataclass
class Run(Operation):
    arg: Optional[str] = None
    convertToStr: bool = True
    collect: bool = True
    kwargs: Dict[str, Any] = field(default_factory=dict)

    def add_to(self, function: "PartialGearFunction"):
        import cloudpickle

        return function.map(lambda x: cloudpickle.dumps(x, protocol=4)).run(
            self.arg, False, self.collect, **self.kwargs
        )


@dataclass
class Register(Operation):
    prefix: str = "*"
    convertToStr: bool = True
    collect: bool = True
    mode: str = TriggerMode.Async
    onRegistered: Optional["optype.Callback"] = None
    trigger: Optional[str] = None
    kwargs: Dict[str, Any] = field(default_factory=dict)

    def add_to(self, function: "PartialGearFunction"):
        return function.register(
            self.prefix,
            self.convertToStr,
            self.collect,
            self.mode,
            self.onRegistered,
            self.trigger,
            **self.kwargs
        )


@dataclass
class Map(Operation):
    op: "optype.Mapper"

    def add_to(self, function: "PartialGearFunction"):
        return function.map(self.op)


@dataclass
class FlatMap(Operation):
    op: "optype.Expander"

    def add_to(self, function: "PartialGearFunction"):
        return function.flatmap(self.op)


@dataclass
class ForEach(Operation):
    op: "optype.Processor"

    def add_to(self, function: "PartialGearFunction"):
        return function.foreach(self.op)


@dataclass
class Filter(Operation):
    op: "optype.Filterer"

    def add_to(self, function: "PartialGearFunction"):
        return function.filter(self.op)


@dataclass
class Accumulate(Operation):
    op: "optype.Accumulator"

    def add_to(self, function: "PartialGearFunction"):
        return function.accumulate(self.op)


@dataclass
class LocalGroupBy(Operation):
    extractor: "optype.Extractor"
    reducer: "optype.Reducer"

    def add_to(self, function: "PartialGearFunction"):
        return function.localgroupby(self.extractor, self.reducer)


@dataclass
class Limit(Operation):
    length: int
    start: int

    def add_to(self, function: "PartialGearFunction"):
        return function.limit(self.length, self.start)


@dataclass
class Collect(Operation):
    pass

    def add_to(self, function: "PartialGearFunction"):
        return function.collect()


@dataclass
class Repartition(Operation):
    extractor: "optype.Extractor"

    def add_to(self, function: "PartialGearFunction"):
        return function.repartition(self.extractor)


@dataclass
class Aggregate(Operation):
    zero: Any
    seqOp: "optype.Accumulator"
    combOp: "optype.Accumulator"

    def add_to(self, function: "PartialGearFunction"):
        return function.aggregate(self.zero, self.seqOp, self.combOp)


@dataclass
class AggregateBy(Operation):
    extractor: "optype.Extractor"
    zero: Any
    seqOp: "optype.Reducer"
    combOp: "optype.Reducer"

    def add_to(self, function: "PartialGearFunction"):
        return function.aggregateby(self.extractor, self.zero, self.seqOp, self.combOp)


@dataclass
class GroupBy(Operation):
    extractor: "optype.Extractor"
    reducer: "optype.Reducer"

    def add_to(self, function: "PartialGearFunction"):
        return function.groupby(self.extractor, self.reducer)


@dataclass
class BatchGroupBy(Operation):
    extractor: "optype.Extractor"
    reducer: "optype.BatchReducer"

    def add_to(self, function: "PartialGearFunction"):
        return function.batchgroupby(self.extractor, self.reducer)


@dataclass
class Sort(Operation):
    reverse: bool = True

    def add_to(self, function: "PartialGearFunction"):
        return function.sort(self.reverse)


@dataclass
class Distinct(Operation):
    pass

    def add_to(self, function: "PartialGearFunction"):
        return function.distinct()


@dataclass
class Count(Operation):
    pass

    def add_to(self, function: "PartialGearFunction"):
        return function.count()


@dataclass
class CountBy(Operation):
    extractor: "optype.Extractor"

    def add_to(self, function: "PartialGearFunction"):
        return function.countby(self.extractor)


@dataclass
class Avg(Operation):
    extractor: "optype.Extractor"

    def add_to(self, function: "PartialGearFunction"):
        return function.avg(self.extractor)
