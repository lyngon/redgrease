from enum import Enum
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union

from redgrease.typing import Constructor, Key, RedisType, T, Val


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
        d (Dict[Key, Val]): Dict to iterate

    Returns:
        Iterable[Tuple[Key, Val]]: Iterable of key-value tuples
    """
    return iter(d.items())


# Not a parser
class CaseInsensitiveDict(dict):
    """Case insensitive dict implementation. Assumes string keys only.
    Copied as-is, except for this docstring from redis.client, as it is not exported
    """

    def __init__(self, data):
        for k, v in iteritems(data):
            self[k.upper()] = v

    def __contains__(self, k):
        return super(CaseInsensitiveDict, self).__contains__(k.upper())

    def __delitem__(self, k):
        super(CaseInsensitiveDict, self).__delitem__(k.upper())

    def __getitem__(self, k):
        return super(CaseInsensitiveDict, self).__getitem__(k.upper())

    def get(self, k, default=None):
        return super(CaseInsensitiveDict, self).get(k.upper(), default)

    def __setitem__(self, k, v):
        super(CaseInsensitiveDict, self).__setitem__(k.upper(), v)

    def update(self, data):
        data = CaseInsensitiveDict(data)
        super(CaseInsensitiveDict, self).update(data)


def as_is(value: T) -> T:
    """Passthrough parser / identity function

    Args:
        value (T): Input value

    Returns:
        [T]: The input value, unchanged
    """
    return value


def str_if_bytes(value: T) -> Union[T, str]:
    """Parses byte values into a string, non-byte values passthrough unchanged
    Slightly modified from redis.utils, as it is not exported

    Args:
        value (T): Any serialized Redis value

    Returns:
        Union[T, str]: Either a string or the input unchanged
    """
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


# Copied as is from redis.utils, as it is not exported
def safe_str(value: Any) -> str:
    """Parse anything to a string

    Args:
        value (Any): Input value

    Returns:
        str: String
    """
    if value is None:
        return ""
    return str(str_if_bytes(value))


def safe_str_upper(value: Any) -> str:
    """Parse anything to an uppercase string

    Args:
        value (Any): Input value

    Returns:
        str: parsed uppercase string
    """
    return safe_str(value).upper()


def bool_ok(value: Any) -> bool:
    """Parse redis response as bool, such that:
    'Ok' => True
    Anything else => False
    Same name as in redis.client but slightly different implementation.
    Should be better for long non-Ok replies, e.g. images, erroneously passed to it

    Args:
        value (Any): Input value

    Returns:
        bool: Parsed boolean
    """
    if isinstance(value, (bytes, str)):
        return safe_str_upper(value[:2]) == "OK"
    else:
        return False


def safe_bool(input: Any) -> bool:
    """Parse a bool, slightly more accepting
    allowing for literal "True"/"False", integer 0/1,
    as well as "Ok" and "yes"/"no" values

    Args:
        input (Any): Input value

    Returns:
        bool: Parsed boolean
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
        value (Union[bool,Any]): A boolean value

    Returns:
        Union[int, Any]: Integer reprepresentataion of the bool
    """
    return int(value) if isinstance(value, bool) else value


# Not a parser
def to_redis_type(value: Any) -> RedisType:
    """Attempts to serialize a value to a Redis-native type.
    I.e. either: bytes, int or float
    It will serialze most primitive types (str, bool, int, float),
    as well as any complex type that implemens __bytes__ method

    Args:
        value (Any): Value to serialize for Redis

    Returns:
        RedisType: A serialized version
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
        List: Flattened list of the transformed keys and values
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
    constuctor: Union[Constructor[T], Dict[Any, Constructor[T]]],
    key: Key = None,
) -> T:
    """Applies a transformation to a value. The tranformatoin fuction could either be
    passed directly or in a dictionary along with a key to look it up.
    This is mostly only useful as a helper functoin for constructors of composite types,
    where the value may need to be transformed differently depending on the field/key.

    Args:
        value (Any): value to transform

        constuctor (Union[Constructor[T], Dict[Any, Constructor[T]]]):
            Transformation function or a dict of transformation functions.
            If a dict is used, the key argument is used to look up the transformation.
            If the key is not present in the dict, but there is a 'None'-key mapping
            to a functsion, this function will be used as a default.
            Otherwise, the value will be returned untransformed.

        key (Key): key to use to look up the appropriate transforpation

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
    except Exception:
        # if for some reason the value can't be coerced, just use
        # the raw value
        return value


def to_dict(
    items: Iterable,
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
        items (Iterable): Iterable to "fold" into a dict

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
        Dict[Key, Val]: Folded dictionary
    """

    if items is None:
        return {}  # type: ignore

    if key_transform is None:
        key_transform = as_is

    if val_transform is None:
        val_transform = as_is

    it = iter(items)
    result = {}
    for key, value in zip(it, it):
        key = transform(key, key_transform, key)
        value = transform(value, val_transform, key)  # type: ignore
        result[key] = value

    return result


def to_kwargs(items: Iterable) -> Dict[str, Any]:
    """Folds an iterable of values into a 'kwargs-compatible' dict.
    This is useful for cunstucting objects from  Redis' list responseses,
    by means of an intermediate kwargs dict that can be passed to for example
    a constructor
    It behaves exactly as 'to_dict' but enforces keys to be parsed to strings

    - Alternating unnamed Key and values, i.e:
    [key_1, value_1, key_2, value_2, ... ]
        eg:
            input:  ["foo", 42, 13, 37]
            output: {"foo": 42, "13": 37}

    Args:
        items (Iterable): Iterable to "fold" into a dict

    Returns:
        Dict[str, Any]: Folded dictionary
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
        item_parser (Constructor[T]): The constructor to apply to each element

    Returns:
        Callable[[Iterable[Any]], List[T]]: Function that takes maps the constructor
        on to the iterable and returns the result as a list
    """

    def parser(input_list):
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
        constructors (Dict[str, Constructor[Any]]): Dict of named constructors

    Returns:
        Callable[..., Dict[str, Any]]: Dict parser
    """

    def parser(values: Iterable, keys: Iterable[Key]):
        return {
            key: transform(value, constructors, key)
            for (key, value) in zip(keys, values)
        }

    return parser
