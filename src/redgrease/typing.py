# -*- coding: utf-8 -*-
"""
Type variables, Type aliases and Protocol Types.
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
"""Type variable for the input value of a GearFunction step / operation."""

OutputRecord = TypeVar("OutputRecord", covariant=True)
"""Type variable for the output value of a GearFunction step / operation."""

Key = TypeVar("Key", contravariant=True)
"""Type variable for a Keys
used in extractor functions in GroupBy operations and similar.
"""

Val = TypeVar("Val")
"""Type variable for intermediate values inside a step / operation."""

Constructor = Union[Type[T], Callable[[Any], T]]
"""Joint type for primitive Types and (single argument) object constructors"""

RedisType = Union[bytes, int, float]
"""Types native to Redis"""

SafeType = Union[bytes, int, float, str]
"""Types that Redis happily accepts as input without any manipulation."""

SupportedType = Union[bool, str, bytes, int, float]
"""Types that RedGrease supports"""

RedisKey = Union[str, bytes]
"""Accepted types for Redis Keys"""

Record = Dict
"""The type of a record from KeysReader and others."""


Callback = Callable[[InputRecord], Any]
""""Type for General Callbacks.

    An function of Callback type can be called with a single argument:

        - A record, of type InputRecord

        The function a value, of any type.
"""

Extractor = Callable[[InputRecord], OutputRecord]
"""Type for Extractor functions.

    An function of Extractor type can be called with a single argument:

        - A record, of type InputRecord

    The function a value, of type OutputRecord.
"""


Mapper = Callable[[InputRecord], OutputRecord]
"""Type for Mapper functions.

    An function of Mapper type can be called with a single argument:

        - A record, of type InputRecord

    The function returns a value, of type OutputRecord.
"""


Expander = Callable[[InputRecord], Iterable[OutputRecord]]
"""Type for Expander functions.

    An function of Expander type can be called with a single argument:

        - A record, of type InputRecord

        The function returns an **Iterable** of any type, OutputRecord.
"""


Processor = Callable[[InputRecord], None]
"""Type for Processor functions.

    An function of Processor type can be called with a single argument:

        - A record, of type InputRecord

    The function does not return anything.
"""


Filterer = Callable[[InputRecord], bool]
"""Type for Filterer functions.

    An function of Filterer type can be called with a single argument:

        - A record, of type InputRecord

    The function returns a bool.
"""


Accumulator = Callable[[T, InputRecord], T]
"""Type for Accumulator functions.

    An function of Accumulator type can be called with two arguments:

        - An accumulator value, of type T
        - A record, of type InputRecord

    The function returns a value of the same type as the accumulator value, T.
"""


Reducer = Callable[[Key, T, InputRecord], T]
"""Type for Reducer functions.

    An function of Reducer type can be called with three arguments:

        - A key value, of type Key
        - An accumulator value, of type T
        - A record, of type InputRecord

    The function returns a value of the same type as the accumulator value, T.
"""


BatchReducer = Callable[[Key, Iterable[InputRecord]], OutputRecord]
"""Type for BatchReducer functions.

    An function of BatchReducer type can be called with two arguments:

        - A key value, of type Key
        - An Iterable collection of records, of type InputRecord

    The function returns a value, T, reduced from the iterable.
"""
