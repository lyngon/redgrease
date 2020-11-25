from enum import Enum
import logging

# Various Helper enums, instead of "magic" strings.


class StrEnum(Enum):
    """
    Enumeration for which its string representatin is the enumeration value
    """
    def __str__(self):
        return str(self.value)

    def __eq__(self, other):
        return str(self) == str(other)


class Reader(StrEnum):
    """Redis Gears Reader Types"""
    KeysReader = "KeysReader"
    KeysOnlyReader = "KeysOnlyReader"
    StreamReader = "StreamReader"
    PythonReader = "PythonReader"
    ShardsIDReader = "ShardsIDReader"
    CommandReader = "CommandReader"


class TriggerMode(StrEnum):
    """Redis Geears Trigger modes for registered actions"""
    Async = 'async'
    AsyncLocal = 'async_local'
    Sync = 'sync'


class LogLevel(StrEnum):
    """Redis Gears log levels"""
    Debug = 'debug'
    Verbose = 'verbose'
    Notice = 'notice'
    Warning = 'warninig'

    def __int__(self):
        if self == LogLevel.Debug:
            return logging.DEBUG
        if self == LogLevel.Verbose:
            return logging.INFO
        if self == LogLevel.Notice:
            return logging.WARNING
        if self == LogLevel.Warning:
            return logging.ERROR  # Maybe a little unintuitive ;)
