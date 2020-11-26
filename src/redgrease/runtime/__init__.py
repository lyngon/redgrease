import sys

if 'redisgears' not in sys.modules:
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
else:
    from redisgears import atomicCtx as atomic  # noqa: F401
    from redisgears import executeCommand as execute  # noqa: F401
    from redisgears import getMyHashTag as hashtag  # noqa: F401
    from redisgears import log  # noqa: F401
    from __main__ import gearsConfigGet  # noqa: F401
    from __main__ import GearsBuilder  # noqa: F401
    from __main__ import GB  # noqa: F401

    # atomic = atomic
    # execute = execute
    # hashtag = hashtag
    # configGet = configGet
    # gearsConfigGet = gearsConfigGet
    # log = log
    # GearsBuilder = GearsBuilder
    # GB = GB
