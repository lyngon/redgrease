from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    Tuple,
    Type,
    TypeVar,
    Union,
)

if TYPE_CHECKING:
    try:
        from typing import Protocol
    except ImportError:
        from typing_extensions import Protocol  # type: ignore
else:
    Protocol = Tuple  # Hack 2000

# Type aliases for type hints

T = TypeVar("T")

InputRecord = TypeVar("InputRecord", contravariant=True)
OutputRecord = TypeVar("OutputRecord", covariant=True)

Key = TypeVar("Key", contravariant=True)
Val = TypeVar("Val")

Constructor = Union[Type[T], Callable[[Any], T]]

RedisType = Union[bytes, int, float]
SafeType = Union[bytes, int, float, str]
SupportedType = Union[bool, str, bytes, int, float]

RedisKey = Union[str, bytes]

Record = Dict


class Callback(Protocol[InputRecord]):
    def __call__(self, record: InputRecord) -> Any:
        ...


class Extractor(Protocol[InputRecord, OutputRecord]):
    def __call__(self, record: InputRecord) -> OutputRecord:
        ...


class Mapper(Protocol[InputRecord, OutputRecord]):
    def __call__(self, record: InputRecord) -> OutputRecord:
        ...


class Expander(Protocol[InputRecord, OutputRecord]):
    def __call__(self, record: InputRecord) -> Iterable[OutputRecord]:
        ...


class Processor(Protocol[InputRecord]):
    def __call__(self, record: InputRecord) -> None:
        ...


class Filterer(Protocol[InputRecord]):
    def __call__(self, record: InputRecord) -> bool:
        ...


class Accumulator(Protocol[T, InputRecord]):
    def __call__(self, accumulator: T, record: InputRecord) -> T:
        ...


class Reducer(Protocol[Key, T, InputRecord]):
    def __call__(self, key: Key, accumulator: T, record: InputRecord) -> T:
        ...


class BatchReducer(Protocol[Key, OutputRecord, InputRecord]):
    def __call__(self, key: Key, records: Iterable[InputRecord]) -> OutputRecord:
        ...
