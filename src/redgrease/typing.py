# -*- coding: utf-8 -*-
"""


Todo:
    * Use config as much as possible

"""
__author__ = "Anders Åström"
__contact__ = "anders@lyngon.com"
__copyright__ = "2021, Lyngon Pte. Ltd."
__licence__ = """The MIT License
Copyright © 2021 Lyngon Pte. Ltd.

Permission is hereby granted, free of charge, to any person obtaining a copy of this
 software and associated documentation files (the “Software”), to deal in the Software
 without restriction, including without limitation the rights to use, copy, modify,
 merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 permit persons to whom the Software is furnished to do so, subject to the following
 conditions:

The above copyright notice and this permission notice shall be included in all copies
 or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF
 CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE
 OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""


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
