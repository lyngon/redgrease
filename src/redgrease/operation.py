from typing import TYPE_CHECKING, Any, Optional, Type

if TYPE_CHECKING:
    import redgrease.typing as optype
    from redgrease.gears import PartialGearFunction


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
