import sys

from .func import trigger
from .gears import ClosedGearFunction, GearFunction, PartialGearFunction
from .reader import (
    CommandReader,
    GearReader,
    KeysOnlyReader,
    KeysReader,
    PythonReader,
    ShardsIDReader,
    StreamReader,
)
from .sugar import FailurePolicy, KeyType, LogLevel, ReaderType, TriggerMode

__all__ = [
    "trigger",
    "ClosedGearFunction",
    "GearFunction",
    "PartialGearFunction",
    "FailurePolicy",
    "KeyType",
    "LogLevel",
    "ReaderType",
    "TriggerMode",
    "utils",
    "CommandReader",
    "GearReader",
    "KeysOnlyReader",
    "KeysReader",
    "PythonReader",
    "ShardsIDReader",
    "StreamReader",
]

# Dynamic / Conditional imports below. (depends on installed packa)

try:
    # This will fail if redis package is not installed
    from .client import Gears, Redis, RedisGears

    __all__ += ["Gears", "Redis", "RedisGears"]
except ModuleNotFoundError:
    pass

try:
    # This will fail if redis package is not installed
    from .command import redis as cmd

    __all__ += ["cmd"]
except ModuleNotFoundError:
    pass


try:
    # this will fail if either redis, watchdog or pyaml packages are not installed
    from .loader import GearsLoader

    __all__ += ["GearsLoader"]
except ModuleNotFoundError:
    pass

try:
    # this will fail if packaging packages is not installed
    from .requirements import (
        Requirement,
        Version,
        read_requirements,
        resolve_requirements,
    )

    __all__ += [
        "Requirement",
        "Version",
        "read_requirements",
        "resolve_requirements",
    ]
except ModuleNotFoundError:
    pass

# Use either the real or mock (placeholder) implementations of the
# Redis Gears Python environment top level builtin funvtions
# Depending on if the module is loaded in a 'redisgears' environment
# or not (e.g. dev or client)
if "redisgears" in sys.modules:
    # Server Gears runtime environment
    # Import the default functions and classes
    # pyright: reportMissingImports=false
    from __main__ import GB as GB
    from __main__ import GearsBuilder as GearsBuilder
    from __main__ import configGet as configGet
    from __main__ import gearsConfigGet as gearsConfigGet
    from redisgears import atomicCtx as atomic
    from redisgears import executeCommand as execute
    from redisgears import getMyHashTag as hashtag
    from redisgears import log as log
else:
    # Dev or Client environment
    # Import placeholder functions and
    from .runtime import (
        GB,
        GearsBuilder,
        atomic,
        configGet,
        execute,
        gearsConfigGet,
        hashtag,
        log,
    )

__all__ += [
    "GB",
    "GearsBuilder",
    "atomic",
    "configGet",
    "execute",
    "gearsConfigGet",
    "hashtag",
    "log",
]
