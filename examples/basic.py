from redgrease.runtime import log, GearsBuilder


def foo(x):
    log(f"foo({x})={x}")
    return x


def bar(x):
    log(f"bar({x})={x}")
    return x


try:
    rg = GearsBuilder('CommandReader')
    rg.map(bar).map(foo)
    rg.register(trigger='async')
except Exception as ex:
    print(ex)
