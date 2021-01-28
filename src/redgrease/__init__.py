import sys

from .sugar import LogLevel as LogLevel
from .sugar import Reader as Reader
from .sugar import TriggerMode as TriggerMode

# Note: the form "from ... import x as x" is used not to trigger the mypy error
# "implicit reexport", as described here:
# https://mypy.readthedocs.io/en/stable/config_file.html#confval-implicit_reexport


# Use either the real or mock (placeholder) implementations of the
# Redis Gears Python environment top level builtin funvtions
# Depending on if the module is loaded in a 'redisgears' environment
# or not (e.g. dev or client)
if "redisgears" in sys.modules:
    # Server Gears runtime environment
    # Import the default functions and classes
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
    # Import placeholder functions and classes
    from .placeholders import GB as GB
    from .placeholders import GearsBuilder as GearsBuilder
    from .placeholders import atomic as atomic
    from .placeholders import configGet as configGet
    from .placeholders import execute as execute
    from .placeholders import gearsConfigGet as gearsConfigGet
    from .placeholders import hashtag as hashtag
    from .placeholders import log as log
