from typing import Any, AnyStr, Callable, Dict, Iterable, TypeVar

# Type aliases for type hints

T = TypeVar("T")
Key = str
Record = dict
Callback = Callable[[Record], Any]
Extractor = Callable[[Record], T]
Mapper = Callable[[Record], Record]
Expander = Callable[[Record], Iterable[Dict[str, Any]]]
Processor = Callable[[Record], None]
Filterer = Callable[[Record], bool]
Accumulator = Callable[[T, Record], T]
Reducer = Callable[[Key, T, Record], T]
BatchReducer = Callable[[Key, Iterable[Dict[str, Any]]], T]
