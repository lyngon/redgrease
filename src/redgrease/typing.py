from typing import Any, Callable, Dict, Iterable, Type, TypeVar, Union

# Type aliases for type hints

T = TypeVar("T")
Key = TypeVar("Key")
Val = TypeVar("Val")

Constructor = Union[Type[T], Callable[[Any], T]]

RedisType = Union[bytes, int, float]
SafeType = Union[bytes, int, float, str]
SupportedType = Union[bool, str, bytes, int, float]

RedisKey = Union[str, bytes]

Record = dict
Callback = Callable[[Record], Any]
Extractor = Callable[[Record], T]
Mapper = Callable[[Record], Record]
Expander = Callable[[Record], Iterable[Dict[str, Any]]]
Processor = Callable[[Record], None]
Filterer = Callable[[Record], bool]
Accumulator = Callable[[T, Record], T]
Reducer = Callable[[RedisKey, T, Record], T]
BatchReducer = Callable[[RedisKey, Iterable[Dict[str, Any]]], T]
