import sys

if 'redisgears' in sys.modules:
    # Import the default functions and classes
    from redisgears import atomicCtx as atomic  # noqa: F401
    from redisgears import executeCommand as execute  # noqa: F401
    from redisgears import getMyHashTag as hashtag  # noqa: F401
    from redisgears import log  # noqa: F401
    from __main__ import gearsConfigGet  # noqa: F401
    from __main__ import GearsBuilder  # noqa: F401
    from __main__ import GB  # noqa: F401
else:
    # Import placeholder functions and classes
    from .placeholders import (   # noqa: F401
        atomic,
        execute,
        hashtag,
        log,
        configGet,
        gearsConfigGet,
        GearsBuilder,
        GB,
    )
