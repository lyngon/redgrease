import redgrease
from redgrease.runtime import log, GB


def foo(x):
    log(f"foo({x})={x}")
    return x


def bar(x):
    log(f"bar({x})={x}")
    return x


log(str(redgrease.Reader.PythonReader), level='warning')


try:
    rg = GB("CommandReader")
    rg.map(bar).map(foo).map(bar)
    rg.register(trigger='HOO')
    log("Done")
except Exception as ex:
    log(str(ex))
