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

import sys

from .func import command, trigger
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
    "command",
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
    from .client import Gears, Redis, RedisCluster, RedisGears

    __all__ += ["Gears", "Redis", "RedisCluster", "RedisGears"]

except ModuleNotFoundError:
    pass


try:
    # This will fail if redis package is not installed
    from .runtime_client import cmd

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
# Redis Gears Python environment top level builtin functions
# Depending on if the module is loaded in a 'redisgears' environment
# or not (e.g. dev or client)
if "redisgears" in sys.modules:
    # Server Gears runtime environment
    GEARS_RUNTIME = True
    # Import the default functions and classes
    # pyright: reportMissingImports=false
    from __main__ import GB as GB
    from __main__ import GearsBuilder as GearsBuilder
    from __main__ import configGet as configGet
    from __main__ import gearsConfigGet as gearsConfigGet
    from redisgears import atomicCtx as atomic
    from redisgears import executeCommand as execute
    from redisgears import getMyHashTag as hashtag
#     from redisgears import log as log

else:
    # Dev or Client environment
    GEARS_RUNTIME = False
    # Import placeholder functions and
    from .runtime import (
        GB,
        GearsBuilder,
        atomic,
        configGet,
        execute,
        gearsConfigGet,
        hashtag,
    )

from .runtime import hashtag3, log

__all__ += [
    "GB",
    "GearsBuilder",
    "atomic",
    "configGet",
    "execute",
    "gearsConfigGet",
    "hashtag",
    "hashtag3",
    "log",
    "GEARS_RUNTIME",
]
