
from redgrease.runtime import log, GearsBuilder


def foo(x):
    log(f"foo({x})={x}")
    return x


def bar(x):
    log(f"bar({x})={x}")
    return x


log("hello!")

try:
    rg = GearsBuilder('CommandReader')
    rg.map(bar).map(foo)
    rg.register(trigger='HOO')
except Exception as ex:
    print(ex)
