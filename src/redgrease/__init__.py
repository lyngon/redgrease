__all__ = []

import sys

from .sugar import KeyType
from .sugar import LogLevel as LogLevel
from .sugar import Reader as Reader
from .sugar import TriggerMode as TriggerMode

__all__ += ["LogLevel", "Reader", "TriggerMode"]

# Note: the form "from ... import x as x" is used not to trigger the mypy error
# "implicit reexport", as described here:
# https://mypy.readthedocs.io/en/stable/config_file.html#confval-implicit_reexport

try:
    # This will fail if redis package is not installed
    from .command import redis as cmd

    __all__ += ["cmd"]
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
    from .runtime import GB as GB
    from .runtime import GearsBuilder as GearsBuilder
    from .runtime import atomic as atomic
    from .runtime import configGet as configGet
    from .runtime import execute as execute
    from .runtime import gearsConfigGet as gearsConfigGet
    from .runtime import hashtag as hashtag
    from .runtime import log as log

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
