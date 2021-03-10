# -*- coding: utf-8 -*-
"""
Basic syntactic sugar.
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
import logging
from typing import Any


class ReaderType:
    """Redis Gears Reader Types"""

    KeysReader = "KeysReader"
    """KeysReader"""

    KeysOnlyReader = "KeysOnlyReader"
    """KeysOnlyReader"""

    StreamReader = "StreamReader"
    """StreamReader"""

    PythonReader = "PythonReader"
    """PythonReader"""

    ShardsIDReader = "ShardsIDReader"
    """ShardsIDReader"""

    CommandReader = "CommandReader"
    """CommandReader"""


class TriggerMode:
    """Redis Geears Trigger modes for registered actions"""

    Async = "async"
    """Async"""

    AsyncLocal = "async_local"
    """AsyncLocal"""

    Sync = "sync"
    """Sync"""


class KeyType:
    """Redis Key Types"""

    String = "string"
    """String"""

    Hash = "hash"
    """Hash"""

    List = "list"
    """List"""

    Set = "set"
    """Set"""

    ZSet = "zset"
    """ZSet"""

    Stream = "stream"
    """Stream"""

    Module = "module"
    """Module"""

    _constructors = {
        "string": str,
        "hash": dict,
        "list": list,
        "set": set,
    }

    @staticmethod
    def of(python_type: Any) -> str:
        """Get the correspondig Redis key type string for a given Python type or value,
        if it exists.
        The only valid types are 'str', 'dict', 'list' and 'set'.

        Args:
            python_type (Any):
                A python type or any value value.

        Raises:
            ValueError:
                If there is no valid Redis key type for the Python type.

        Returns:
            str:
                The string representing the Redis key type.
        """
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
        """Get the corresponting Python type / constructor, for a given Redis key type.

        Args:
            key_type (str):
                A Redis key type as str.

        Returns:
            Type:
                The Python constructor / type corresponding to the key type.
        """
        return KeyType._constructors[key_type]


class FailurePolicy:
    """Redis event handler failure policy"""

    Continue = "continue"
    """Continue"""

    Abort = "abort"
    """Abort"""

    Retry = "retry"
    """Retry"""


class LogLevel:
    """Redis Gears log levels"""

    Debug = "debug"
    """Debug"""

    Verbose = "verbose"
    """Verbose"""

    Notice = "notice"
    """Notice"""

    Warning = "warninig"
    """Warning"""

    @staticmethod
    def to_logging_level(rg_log_level):
        """Get the 'logging' log level corresponding to a Redis Gears log level.

        Args:
            rg_log_level ([type]):
                Reis Gears log level

        Returns:
            int:
                The corresponding default 'logging' log level.
        """
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
