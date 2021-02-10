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
