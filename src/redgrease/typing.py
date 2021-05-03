# -*- coding: utf-8 -*-
# from __future__ import annotations

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


from typing import Any, Callable, Dict, Hashable, Iterable, Type, TypeVar, Union

# Type aliases for type hints

T = TypeVar("T")

InputRecord = TypeVar("InputRecord", contravariant=True)
"""Type variable for the input value of a GearFunction step / operation."""

OutputRecord = TypeVar("OutputRecord", covariant=True)
"""Type variable for the output value of a GearFunction step / operation."""

Key = TypeVar("Key", bound=Hashable, contravariant=True)
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


Registrator = Callable[[], None]
""""Type definition for Registrator functions.

    I.e. callback functions that may be called on each shard upon function registration.
    Such functions provide a good place to initialize non-serializable objects such as
    network connections.

    An function of Registrator type shoud take no arguments, nor return any value.
"""

Extractor = Callable[[InputRecord], Key]
"""Type definition for Extractor functions.

    Extractor functions are used in the following :ref:`operations`:

    - :ref:`op_localgroupby`
    - :ref:`op_repartition`
    - :ref:`op_aggregateby`
    - :ref:`op_groupby`
    - :ref:`op_batchgroupby`
    - :ref:`op_countby`
    - :ref:`op_avg`


    Extractor functions extracts or calculates the value that should be used as
    (grouping) key, from an input record of the operation.

    :Parameters: (InputRecord) - A single input-record, of the same type as the
    operations' input type.

    :Returns: A any 'Hashable' value.

    :Return type: Key

    Example - Count users per supervisor::

        # Function of "Extractor" type
        # Extracts the "supervisor" for a user,
        # If the user has no supervisor, then the user is considered its own supervisor.
        def supervisor(user)
            return user.get("supervisor", user["id"])

        KeysReader("user:*").values().countby(supervisor).run()

"""


Mapper = Callable[[InputRecord], OutputRecord]
"""Type definition for Mapper functions.

    Mapper functions are used in the following :ref:`operations`:

    - :ref:`op_map`

    Mapper functions transforms a value from the operations input to some new value.

    :Parameters: (InputRecord) - A single input-record, of the same type as the
    operations' input type.

    :Returns: A any value.

    :Return type: OutputRecord
"""


Expander = Callable[[InputRecord], Iterable[OutputRecord]]
"""Type definition forExpander functions.

    Expander functions are used in the following :ref:`operations`:

    - :ref:`op_flatmap`

    Expander functions transforms a value from the operations input into several new
    values.

    :Parameters: (InputRecord) - A single input-record, of the same type as the
    operations' input type.

    :Returns: An iterable sequence of values, for example a list, each of which becomes
        an input to the next operation.

    :Return type: Iterable[OutputRecord]
"""


Processor = Callable[[InputRecord], None]
"""Type definition forProcessor functions.

    Processor functions are used in the following :ref:`operations`:

    - :ref:`op_foreach`

    Processor functions performs some side effect using a value from the operations
    input.

    :Parameters: (InputRecord) - A single input-record, of the same type as the
    operations' input type.

    :Returns: Nothing.

    :Return type: None
"""


Filterer = Callable[[InputRecord], bool]
"""Type definition forFilterer functions.

    Filterer functions are used in the following :ref:`operations`:

    - :ref:`op_filter`

    Filter functions evaluates a value from the operations input to either ``True``
    or ``False``.

    :Parameters: (InputRecord) - A single input-record, of the same type as the
    operations' input type.

    :Returns: Either ``True`` or ``False``.

    :Return type: bool
"""


Accumulator = Callable[[T, InputRecord], T]
"""Type definition forAccumulator functions.

    Accumulator functions are used in the following :ref:`operations`:

    - :ref:`op_accumulate`
    - :ref:`op_aggregate`

    Accumulator functions takes a variable that's also called an accumulator, as well
    as an input record. It aggregates inputs into the accumulator variable, which
    stores the  state between the function's invocations.
    The function must return the accumulator's updated value after each call.

    :Parameters:

    * ( T ) - An accumulator value.

    * (InputRecord) - A single input-record, of the same type as the operations' input
    type.

    :Returns: The updated accumulator value.

    :Return type: T
"""


Reducer = Callable[[Key, T, InputRecord], T]
"""Type definition forReducer functions.

    Reducer functions are used in the following :ref:`operations`:

    - :ref:`op_localgroupby`
    - :ref:`op_aggregateby`
    - :ref:`op_groupby`


    Reducer functions receives a key, a variable that's called an accumulator and an an
    input. It performs similarly to the :data:`redgrease.typing.Accumulator callback`,
    with the difference being that it maintains an accumulator per reduced key.

    :Parameters:

    * (Key) - A key value for the group.

    * ( T ) - An accumulator value.

    * (InputRecord) - A single input-record, of the same type as the operations' input
    type.

    :Returns: The updated accumulator value.

    :Return type: T
"""


BatchReducer = Callable[[Key, Iterable[InputRecord]], OutputRecord]
"""Type definition forBatchReducer functions.

    BatchReducer functions are used in the following :ref:`operations`:

    - :ref:`op_batchgroupby`

    BatchReducer functions receives a key and a list of input records. It performs
    similarly to the :data:`redgrease.typing.Reducer callback`, with the difference
    being that it is input with a list of records instead of a single one.
    It is expected to return an accumulator value for these records

    :Parameters:

    * (Key) - A key value for the group.

    * (Iterable[InputRecord]) - A collection of input-record, of the same type as the
    operations' input type.

    :Returns: A reduced output value.

    :Return type: OutputRecord
"""
