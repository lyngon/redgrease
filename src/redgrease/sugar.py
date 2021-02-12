import logging

import redgrease
from redgrease.typing import Callback

# Various "sugar" constructs, such as:
# - Helper enums, instead of "magic" strings.
# - Function decorators for boilerplate gears constructs


class Reader:
    """Redis Gears Reader Types"""

    KeysReader = "KeysReader"
    KeysOnlyReader = "KeysOnlyReader"
    StreamReader = "StreamReader"
    PythonReader = "PythonReader"
    ShardsIDReader = "ShardsIDReader"
    CommandReader = "CommandReader"


class TriggerMode:
    """Redis Geears Trigger modes for registered actions"""

    Async = "async"
    AsyncLocal = "async_local"
    Sync = "sync"


class KeyType:
    """Redis Key Types"""

    String = "string"
    Hash = "hash"
    List = "list"
    Set = "set"
    ZSet = "zset"
    Stream = "stream"
    Module = "module"

    _constructors = {
        "string": str,
        "hash": dict,
        "list": list,
        "set": set,
    }

    @staticmethod
    def of(python_type):
        if python_type is str or isinstance(python_type, str):
            return KeyType.String
        elif python_type is dict or isinstance(python_type, dict):
            return KeyType.Hash
        elif python_type is list or isinstance(python_type, list):
            return KeyType.List
        elif python_type is set or isinstance(python_type, set):
            return KeyType.Set
        else:
            raise ValueError(
                f"No corresponding Redis Key-Type for python type {python_type}"
            )

    @staticmethod
    def constructor(key_type: str):
        return KeyType._constructors[key_type]


class LogLevel:
    """Redis Gears log levels"""

    Debug = "debug"
    Verbose = "verbose"
    Notice = "notice"
    Warning = "warninig"

    @staticmethod
    def to_logging_level(rg_log_level):
        if rg_log_level == LogLevel.Debug:
            return logging.DEBUG
        elif rg_log_level == LogLevel.Verbose:
            return logging.INFO
        elif rg_log_level == LogLevel.Notice:
            return logging.WARNING
        elif rg_log_level == LogLevel.Warning:
            return logging.ERROR  # Maybe a little unintuitive ;)
        else:
            return logging.INFO


def trigger(
    trigger: str,
    prefix: str = "*",
    convertToStr: bool = True,
    collect: bool = True,
    mode: str = TriggerMode.Async,
    onRegistered: Callback = None,
    **kargs,
):
    def gear(func):
        redgrease.GearsBuilder("CommandReader").map(
            lambda params: func(params[1:])
        ).register(
            prefix=prefix,
            convertToStr=convertToStr,
            collect=collect,
            mode=mode,
            onRegistered=onRegistered,
            trigger=trigger,
            **kargs,
        )

    return gear
