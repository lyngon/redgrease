# -*- coding: utf-8 -*-
"""
Utility and boilerplate functions, such as parsers, value transformers etc.
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

import ast
import functools
from enum import Enum
from typing import (
    Any,
    Callable,
    Container,
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
    Type,
    Union,
)

from redgrease.typing import Constructor, Filterer, Key, RedisType, T, Val


# Not a parser
class REnum(Enum):
    """Base Class for Redis-compatible enum values"""

    def __str__(self):
        return safe_str(self.value)

    def __bytes__(self):
        return to_redis_type(self.value)

    def __repr__(self):
        return f"{self.__class__.__name__}({self.value})"

    def __eq__(self, other):
        return str(self) == safe_str(other)

    def __hash__(self):
        return hash(str(self))


# Not a parser
def iteritems(d: Dict[Key, Val]) -> Iterable[Tuple[Key, Val]]:
    """Iterate items in a dict
    Just to le able to use CaseInsensitiveDict without modifications

    Args:
        d (Dict[Key, Val]):
            Dict to iterate

    Returns:
        Iterable[Tuple[Key, Val]]:
            Iterable of key-value tuples
    """
    return iter(d.items())


# Not a parser
class CaseInsensitiveDict(dict):
    """Case insensitive dict implementation.
    Assumes string keys only.
    Heavily influenced from redis.client.
    """

    def __init__(self, data):
        for k, v in iteritems(data):
            self[safe_str_upper(k)] = v

    def __contains__(self, k):
        return super(CaseInsensitiveDict, self).__contains__(safe_str_upper(k))

    def __delitem__(self, k):
        super(CaseInsensitiveDict, self).__delitem__(safe_str_upper(k))

    def __getitem__(self, k):
        return super(CaseInsensitiveDict, self).__getitem__(safe_str_upper(k))

    def get(self, k, default=None):
        return super(CaseInsensitiveDict, self).get(safe_str_upper(k), default)

    def __setitem__(self, k, v):
        super(CaseInsensitiveDict, self).__setitem__(safe_str_upper(k), v)

    def update(self, data):
        data = CaseInsensitiveDict(data)
        super(CaseInsensitiveDict, self).update(data)


def as_is(value: T) -> T:
    """Passthrough parser / identity function

    Args:
        value (T):
            Input value

    Returns:
        T:
            The value, unmodified.
    """
    return value


def str_if_bytes(value: T) -> Union[T, str]:
    """Parses byte values into a string, non-byte values passthrough unchanged
    Slightly modified from redis.utils, as it is not exported

    Args:
        value (T):
            Any serialized Redis value

    Returns:
        Union[T, str]:
            Either a string or the input unchanged
    """
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


# Copied as is from redis.utils, as it is not exported
def safe_str(value: Any) -> str:
    """Parse anything to a string

    Args:
        value (Any):
            Input value

    Returns:
        str:
            String
    """
    if value is None:
        return ""
    return str(str_if_bytes(value))


def safe_str_upper(value: Any) -> str:
    """Parse anything to an uppercase string

    Args:
        value (Any):
            Input value

    Returns:
        str:
            Parsed uppercase string
    """
    return safe_str(value).upper()


def bool_ok(value: Any) -> bool:
    """Parse redis response as bool, such that:
    'Ok' => True
    Anything else => False
    Same name as in redis.client but slightly different implementation.
    Should be better for long non-Ok replies, e.g. images, erroneously passed to it

    Args:
        value (Any):
            Input value

    Returns:
        bool:
            Parsed boolean
    """
    if isinstance(value, (bytes, str)):
        return safe_str_upper(value[:2]) == "OK"
    else:
        return False


def optional(constructor: Constructor[T]) -> Constructor[Optional[T]]:
    """Create parser that accepts `None` values, but otherwise behaves like the
    provided parser.

    Args:
        constructor (Constructor[T]):
            constructor to apply, unless the value is None.
    """

    def parser(value):
        return None if value is None else constructor(value)

    return parser


def safe_bool(input: Any) -> bool:
    """Parse a bool, slightly more accepting
    allowing for literal "True"/"False", integer 0/1,
    as well as "Ok" and "yes"/"no" values

    Args:
        input (Any):
            Input value

    Returns:
        bool:
            Parsed boolean
    """
    if isinstance(input, (bool, int)):
        return bool(input)
    if isinstance(input, (bytes, str)):
        return any(
            [
                safe_str_upper(input[:4]).startswith(true_val)
                for true_val in ["TRUE", "OK", "1", "Y", "YES"]
            ]
        )
    return False


# Not a parser
def to_int_if_bool(value: Any) -> Union[int, Any]:
    """Transforms any boolean into an integer
    As booleans are not natively supported as a separate datatype in Redis
    True => 1
    False => 0

    Args:
        value (Union[bool,Any]):
            A boolean value

    Returns:
        Union[int, Any]:
            Integer reprepresentataion of the bool
    """
    return int(value) if isinstance(value, bool) else value


# Not a parser
def to_redis_type(value: Any) -> RedisType:
    """Attempts to serialize a value to a Redis-native type.
    I.e. either: bytes, int or float
    It will serialze most primitive types (str, bool, int, float),
    as well as any complex type that implemens __bytes__ method

    Args:
        value (Any):
            Value to serialize for Redis

    Returns:
        RedisType:
            A serialized version
    """
    if value is None:
        return bytes()

    if isinstance(value, bool):
        return to_int_if_bool(value)

    if isinstance(value, str):
        return value.encode()

    if hasattr(value, "__bytes__"):
        value.__bytes__()

    if isinstance(value, (bytes, int, float)):
        return value

    return to_redis_type(str(value))


def to_bytes(value: Any) -> bytes:
    compat_val = to_redis_type(value)
    if isinstance(compat_val, bytes):
        return compat_val
    else:
        return str(compat_val).encode()


# Not a parser
def to_list(
    mapping: Optional[Dict[Key, Val]],
    key_transform: Callable[[Key], Any] = str_if_bytes,
    val_transform: Callable[[Val], Any] = to_redis_type,
) -> List:
    """Flattens a Dict to a list consisting of the of keys and values altertnating.
    This is useful generating the arguments for Redis commands.

    Args:
        mapping (Dict[Key, Val]):
            The dict to flatten.

        key_transform (Callable[[Key], Any], optional):
            Transformation function for the keys.
            Defaults to 'str_if_bytes'

        val_transform (Callable[[Val], Any], optional):
            Transformation function for the values.
            Defaults to 'to_redis_type'.

    Returns:
        List:
            Flattened list of the transformed keys and values
    """
    if mapping is None:
        return []
    return list(
        [
            item
            for key, value in mapping.items()
            for item in (key_transform(key), val_transform(value))
        ]
    )


def transform(
    value: Any,
    constuctor: Union[Constructor[T], Dict[Key, Constructor[T]]],
    key: Key = None,
) -> T:
    """Applies a transformation to a value.
    The tranformation fuction could either be passed directly or in a dictionary along
    with a key to look it up.
    This is mostly only useful as a helper function for constructors of composite types,
    where the value may need to be transformed differently depending on the field/key.

    Args:
        value (Any):
            Value to transform

        constuctor (Union[Constructor[T], Dict[Any, Constructor[T]]]):
            Transformation function or a dict of transformation functions.
            If a dict is used, the key argument is used to look up the transformation.
            If the key is not present in the dict, but there is a 'None'-key mapping
            to a functsion, this function will be used as a default.
            Otherwise, the value will be returned untransformed.

        key (Key):
            key to use to look up the appropriate transforpation

    Returns:
        T: Transformed value
    """
    if constuctor is None:
        return value  # type: ignore

    try:
        if isinstance(constuctor, dict):
            if key in constuctor:
                return constuctor[key](value)  # type: ignore
            elif None in constuctor:
                return constuctor[None](value)  # type: ignore
            else:
                return value
        else:
            return constuctor(value)  # type: ignore
    except (TypeError, ValueError):
        # if for some reason the value can't be coerced, just use
        # the raw value
        return value


def to_dict(
    items: Iterable,
    keys: Iterable = None,
    key_transform: Union[Constructor[Key], Dict[Any, Constructor[Key]]] = None,
    val_transform: Union[Constructor[Val], Dict[Key, Constructor[Val]]] = None,
) -> Dict[Key, Val]:
    """Folds an iterable of values into a dict.
    This is useful for parsing Redis' list responseses into a more manageable structure.
    It can be used on lists of following different structures:

    - Alternating unnamed Key and values, i.e:
    [key_1, value_1, key_2, value_2, ... ]
        eg:
        - to_dict(["foo", 42, 13, 37]) == {"foo": 42, 13: 37}
        - to_dict(["foo", 42, 13, 37], key_transform=str) == {"foo": 42, "13": 37}
        - to_dict(["foo", 42, 13, 37], val_transform=str) == {"foo": "42", 13: "37"}
        - to_dict(["foo", 42, 13, 37], val_transform={"foo":int, 13:float})
            == {"foo": 42, 13: 37.0}
        - to_dict(
            ["foo", 42, 13, 37],
            key_transform=str,
            val_transform={"foo":int, "13":float}
        ) == {"foo": 42, "13": 37.0}


    Args:
        items (Iterable):
            Iterable to "fold" into a dict

        key_transform (Union[Constructor[Key], Dict[Any, Constructor[Key]]], optional):
            Transformation function / type / constructor to apply to keys.
            It can either be a callable, which is then applied to all keys.
            Altertnatively, it can be a mapping from 'raw' key to a specific transform
            for that key
            Defaults to None (No key transformation).

        val_transform (Union[Constructor[Val], Dict[Key, Constructor[Val]]], optional):
            Transformation function / type / constructor to apply to values.
            It can either be a callable, which is then applied to all values.
            Altertnatively, it can be a mapping from (transformed) key to a specific
            transform for the value of that key
            Defaults to None (No value transformation).

    Returns:
        Dict[Key, Val]:
            Folded dictionary
    """

    if items is None:
        return {}  # type: ignore

    if key_transform is None:
        key_transform = as_is

    if val_transform is None:
        val_transform = as_is

    if keys:
        kv_pairs = zip(keys, items)
    else:
        it = iter(items)
        kv_pairs = zip(it, it)

    result = {}
    for key, value in kv_pairs:
        key = transform(key, key_transform, key)
        value = transform(value, val_transform, key)  # type: ignore
        result[key] = value

    return result


def to_kwargs(items: Iterable) -> Dict[str, Any]:
    """Folds an iterable of values into a 'kwargs-compatible' dict.
    This is useful for cunstucting objects from  Redis' list responseses, by means
    of an intermediate kwargs dict that can be passed to for example a constructor.
    It behaves exactly as 'to_dict' but enforces keys to be parsed to strings.

    - Alternating unnamed Key and values, i.e:
    [key_1, value_1, key_2, value_2, ... ]
        eg:
            input:  ["foo", 42, 13, 37]
            output: {"foo": 42, "13": 37}

    Args:
        items (Iterable):
            Iterable to "fold" into a dict

    Returns:
        Dict[str, Any]:
            Folded dictionary
    """
    return to_dict(items, key_transform=safe_str)


# TODO: Should maybe be renamed to list_of or parse_list or sometiong
def list_parser(item_parser: Constructor[T]) -> Callable[[Iterable], List[T]]:
    """Creates a list parser for lists of objects created with a given constructor.
    E.g:

    parser = list_parser(bool)
    parser(['', 1, None])

    => [False, True, False]

    Args:
        item_parser (Constructor[T]):
            The constructor to apply to each element.

    Returns:
        Callable[[Iterable[Any]], List[T]]:
            Function that takes maps the constructor on to the iterable and returns
            the result as a list.
    """

    def parser(input_list):
        if isinstance(input_list, bytes):
            input_list = safe_str(input_list)
        if isinstance(input_list, str):
            input_list = list(ast.literal_eval(input_list))

        return list(map(item_parser, input_list))

    return parser


# is this used anywhere?
list_of_str = list_parser(safe_str)


def dict_of(
    constructors: Dict[Key, Constructor[Any]]
) -> Callable[[Iterable, Iterable[Key]], Dict[Key, Any]]:
    """Creates a parser that parses a list of values to a dict,
    according to a dict of named constructors.

    The generated parser takes both the iterable of values to parse,
    as well as, an equally long, iterable of names/keys to to use to lookup the
    corresponding parser/constructor in the constructor lookup dict.

    The parser for the Nth value is using the parser found by looking up the Nth
    name/key in the key list in the lookup dict.
    This key is also used as the key in the resulting dict.

    E.g:
    parser = dict_of({"b":bool, "i":int, "s":str, "f":float})
    parser([0,1,0,1], ["b","f","s","i"])

    => {"b":False, "f":1.0, "i":1, "s":"0"}

    Args:
        constructors (Dict[str, Constructor[Any]]):
            Dict of named constructors

    Returns:
        Callable[..., Dict[str, Any]]:
            Dict parser
    """

    def parser(values: Iterable, keys: Iterable[Key]):
        return {
            key: transform(value, constructors, key)
            for (key, value) in zip(keys, values)
        }

    return parser


class Record:
    """Class repredenting a Redis Record, as generated by KeysReader.

    Attributes:
        key (str):
            The name of the Redis key.

        value (Any):
            The value corresponting to the key. `None` if deleted.

        type (str):
            The core Redis type. Either 'string', 'hash', 'list', 'set', 'zset' or
            'stream'.

        event (str):
            The event that triggered the execution.
            (`None` if the execution was created via the run function.)
    """

    def __init__(
        self,
        key: str,
        value: Any = None,
        type: str = None,
        event: str = None,
        **kwargs,
    ):
        if not key:
            raise ValueError(f"Record cannot be created with empty key '{key}'.")

        self.key = safe_str(key)
        self.value = value
        self.type = safe_str(type)
        self.event = safe_str(event)
        self.kwargs = kwargs


class StreamRecord:
    """Class repredenting a Redis Record, as generated by KeysReader.

    Attributes:
        key (str):
            The name of the Redis key.

        value (Any):
            The value corresponting to the key. `None` if deleted.

        type (str):
            The core Redis type. Either 'string', 'hash', 'list', 'set', 'zset' or
            'stream'.

        event (str):
            The event that triggered the execution.
            (`None` if the execution was created via the run function.)
    """

    def __init__(
        self,
        key: str,
        id: str = None,
        value: Any = None,
        **kwargs,
    ):
        if not key:
            raise ValueError(f"Record cannot be created with empty key '{key}'.")

        self.key = safe_str(key)
        self.id = safe_str(id) if id else None
        self.value = value
        self.kwargs = kwargs


def record(rec: Any) -> Record:
    """Create a Record

    Args:
        rec (Any):
            the value to parse. Either a string (key only) or a dict with at minimum
            the key `key` present and optionally any of the keys `value`, `type`
            and/or `event`.

    Returns:
        Record:
            Parsed Record object.
    """
    if isinstance(rec, Record):
        return rec

    if isinstance(rec, dict):
        return Record(**rec)

    return Record(rec)


def stream_record(rec: Any) -> StreamRecord:
    """Create a Record

    Args:
        rec (Any):
            the value to parse. Either a string (key only) or a dict with at minimum
            the key `key` present and optionally any of the keys `value`, `type`
            and/or `event`.

    Returns:
        Record:
            Parsed Record object.
    """
    if isinstance(rec, StreamRecord):
        return rec

    if isinstance(rec, dict):
        return StreamRecord(**rec)

    return StreamRecord(rec)


def compose(*function: Callable) -> Callable:
    """Compose functions.
    I.e::

        ``lambda x: f(g(x))``
        can be written:
        ``compose(f, g)``

    Args:
        *function (Callable):
            Any number of functions to compose together.
            Output type of function N must be the input type of function N+1.

    Returns:
        Callable:
            A composed function with input type same as the firs function, and output
            type same as the last function.
    """
    return functools.reduce(lambda f, g: lambda x: f(g(x)), function, lambda x: x)


def dict_filter(**kwargs) -> Filterer[Dict[str, Any]]:
    """Create a dictionary matching predicate function.

    This function takes any number of keyword arguments, and returns a predicate
    function that takes a single `dict` as argument and returns a `bool`.

    The predicate function returns `True` if, and only if, for all keyword arguments:

    1. The keyword argument name is a key in the dict, and

    2. Depending on the value, V, of the keyword argument, either:

        - V is Container type (excluding `str`) -> The dict value for the key is
            present in V.

        - V is a Type (e.g. `bool`) -> The dict value for the key is of type V.

        - V is any value, except `...` (Ellipsis) -> The dict value for the key
            equals V.

        - V is `...` (Ellipsis) -> The dict value can be any value.
    """

    def predicate(record):
        for k, v in kwargs.items():
            if k not in record:
                return False

            if v == ...:
                continue
            elif isinstance(v, Container) and not isinstance(v, str):
                if record[k] not in v:
                    return False
            elif isinstance(v, Type):
                if not isinstance(record[k], v):
                    return False
            elif v != record[k]:
                return False
        return True

    return predicate


def passfun(fun: Optional[T] = None, default: Optional[T] = None) -> T:
    """Create a Python 'function' object from any 'Callable', or constant.

    RedisGears operator callbacks ony accept proper 'function's and not every type of
    'Callable', such as for example 'method's (e.g. `redgrease.cmd.incr`) or
    'method-desriptor's (e.g. `str.split`), wich forces users to write seemingly
    "unecceary" lambda-functions to wrap these.

    This function ensures that the argument is a proper 'function', and thus will be
    accepted as RedisGears operator callback (asuming the type signature is correct).

    It can also be used to create 'constant'-functions, if passing a non-callable,
    or to create the 'identity-function`, if called with no arguments.


    Args:
        fun (Optional[T], optional):
            Callable to turn into a 'function`
            Alternatively a constant, to use to create a constant function,
            i.e. a function that alway return the same thing, regarding of input.
            If None, and no default, the 'identity-function' is returned.
            Defaults to None.

        default (Optional[T], optional):
            Default Callable to use as fallback if the 'fun' argument isn't a callable.
            Defaults to None.

    Returns:
        T:
            [description]
    """
    if fun is None:
        return passfun(default) if default else lambda x: x  # type: ignore

    if type(fun).__name__ == "function":
        return fun

    if callable(fun):
        return lambda *a, **kw: fun(*a, **kw)  # type: ignore

    return passfun(default) if default else lambda *__, **_: fun  # type: ignore
