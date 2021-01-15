import sys
from .sugar import TriggerMode, Reader, LogLevel  # noqa: F401

# Use either the real or mock (placeholder) implementations of the
# Redis Gears Python environment top level builtin funvtions
# Depending on if the module is loaded in a 'redisgears' environment
# or not (e.g. dev or client)
if "redisgears" in sys.modules:
    # Server Gears runtime environment
    # Import the default functions and classes
    from redisgears import atomicCtx as atomic  # noqa: F401
    from redisgears import executeCommand as execute  # noqa: F401
    from redisgears import getMyHashTag as hashtag  # noqa: F401
    from redisgears import log  # noqa: F401
    from __main__ import gearsConfigGet  # noqa: F401
    from __main__ import GearsBuilder  # noqa: F401
    from __main__ import GB  # noqa: F401
else:
    # Dev or Client environment
    # Import placeholder functions and classes
    from .placeholders import (  # noqa: F401
        atomic,
        execute,
        hashtag,
        log,
        configGet,
        gearsConfigGet,
        GearsBuilder,
        GB,
    )
