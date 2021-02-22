import sys

import pytest
import redis.exceptions

from redgrease import GearsBuilder, execute, hashtag
from redgrease.client import RedisGears

counter = 0


redis_py_ver = (3, 7, 2, "final", 0)


@pytest.mark.xfail(
    sys.version_info[:2] != redis_py_ver[:2],
    reason="Incompatible Python versions",
    raises=redis.exceptions.ResponseError,
)
def test_map(rg):
    rg.set("x", "1")
    rg.set("y", "2")
    rg.set("z", "3")

    gear_fun = GearsBuilder().map(lambda x: x["value"]).sort().run()
    res = rg.gears.pyexecute(gear_fun, requirements=["redgrease[runtime]"])
    assert res.results == ["1", "2", "3"]
    assert res.errors == []


@pytest.mark.xfail(
    sys.version_info[:2] != redis_py_ver[:2],
    reason="Incompatible Python versions",
    raises=redis.exceptions.ResponseError,
)
def test_filter(rg: RedisGears):
    rg.set("x", "1")
    rg.set("y", "2")
    rg.set("z", "3")
    gear_fun = GearsBuilder().map(lambda x: x["value"]).filter(lambda x: x == "1").run()
    res = rg.gears.pyexecute(gear_fun)
    assert res.results == ["1"]
    assert res.errors == []


@pytest.mark.xfail(
    sys.version_info[:2] != redis_py_ver[:2],
    reason="Incompatible Python versions",
    raises=redis.exceptions.ResponseError,
)
def test_foreach(rg: RedisGears):
    rg.set("x", "1")
    rg.set("y", "2")
    rg.set("z", "3")

    def increase(_):
        global counter
        counter += 1

    # important to notice, the counte will increased
    # on the server size and not on client side!!
    gear_fun = GearsBuilder().foreach(increase).map(lambda _: counter).run()

    res = rg.gears.pyexecute(gear_fun)
    assert res.results == [1, 2, 3]
    assert res.errors == []


@pytest.mark.xfail(
    sys.version_info[:2] != redis_py_ver[:2],
    reason="Incompatible Python versions",
    raises=redis.exceptions.ResponseError,
)
def test_flatmap(rg: RedisGears):
    rg.lpush("l", "1", "2", "3")

    gear_fun = (
        GearsBuilder("KeysOnlyReader")
        .map(lambda x: execute("lrange", x, "0", "-1"))
        .flatmap(lambda x: x)
        .run()
    )
    res = rg.gears.pyexecute(gear_fun)
    assert res.results == ["1", "2", "3"]
    assert res.errors == []


@pytest.mark.xfail(
    sys.version_info[:2] != redis_py_ver[:2],
    reason="Incompatible Python versions",
    raises=redis.exceptions.ResponseError,
)
def test_countby(rg: RedisGears):
    rg.set("x", "1")
    rg.set("y", "1")
    rg.set("z", "2")
    rg.set("t", "2")
    gear_fun = (
        GearsBuilder()
        .map(lambda x: x["value"])
        .countby()
        .map(lambda x: (x["key"], x["value"]))
        .sort()
        .run()
    )
    res = rg.gears.pyexecute(gear_fun)
    assert res.results == [("1", 2), ("2", 2)]
    assert res.errors == []


@pytest.mark.xfail(
    sys.version_info[:2] != redis_py_ver[:2],
    reason="Incompatible Python versions",
    raises=redis.exceptions.ResponseError,
)
def test_avg(rg: RedisGears):
    rg.set("x", "1")
    rg.set("y", "1")
    rg.set("z", "2")
    rg.set("t", "2")
    gear_fun = GearsBuilder().map(lambda x: x["value"]).avg().run()
    res = rg.gears.pyexecute(gear_fun)
    assert res.results == [1.5]
    assert res.errors == []


@pytest.mark.xfail(
    sys.version_info[:2] != redis_py_ver[:2],
    reason="Incompatible Python versions",
    raises=redis.exceptions.ResponseError,
)
def test_count(rg: RedisGears):
    rg.set("x", "1")
    rg.set("y", "1")
    rg.set("z", "2")
    rg.set("t", "2")
    gear_fun = GearsBuilder().count().run()
    res = rg.gears.pyexecute(gear_fun)
    assert res.results == [4]
    assert res.errors == []


@pytest.mark.xfail(
    sys.version_info[:2] != redis_py_ver[:2],
    reason="Incompatible Python versions",
    raises=redis.exceptions.ResponseError,
)
def test_distinct(rg: RedisGears):
    rg.set("x", "1")
    rg.set("y", "1")
    rg.set("z", "2")
    rg.set("t", "2")
    gear_fun = GearsBuilder().map(lambda x: x["value"]).distinct().count().run()
    res = rg.gears.pyexecute(gear_fun)
    assert res.results == [2]
    assert res.errors == []


@pytest.mark.xfail(
    sys.version_info[:2] != redis_py_ver[:2],
    reason="Incompatible Python versions",
    raises=redis.exceptions.ResponseError,
)
def test_aggregate(rg: RedisGears):
    rg.set("x", "1")
    rg.set("y", "1")
    rg.set("z", "2")
    rg.set("t", "2")
    gear_fun = (
        GearsBuilder()
        .map(lambda x: x["value"])
        .aggregate(0, lambda a, r: a + int(r), lambda a, r: a + r)
        .run()
    )
    res = rg.gears.pyexecute(gear_fun)
    assert res.results == [6]
    assert res.errors == []


@pytest.mark.xfail(
    sys.version_info[:2] != redis_py_ver[:2],
    reason="Incompatible Python versions",
    raises=redis.exceptions.ResponseError,
)
def test_aggregateby(rg: RedisGears):
    rg.set("x", "1")
    rg.set("y", "1")
    rg.set("z", "2")
    rg.set("t", "2")
    gear_fun = (
        GearsBuilder()
        .map(lambda x: x["value"])
        .aggregateby(lambda x: x, 0, lambda _, a, r: a + int(r), lambda _, a, r: a + r)
        .map(lambda x: (x["key"], x["value"]))
        .sort()
        .run()
    )
    res = rg.gears.pyexecute(gear_fun)
    assert res.results == [("1", 2), ("2", 4)]
    assert res.errors == []


@pytest.mark.xfail(
    sys.version_info[:2] != redis_py_ver[:2],
    reason="Incompatible Python versions",
    raises=redis.exceptions.ResponseError,
)
def test_limit(rg: RedisGears):
    rg.set("x", "1")
    rg.set("y", "1")
    rg.set("z", "2")
    rg.set("t", "2")
    gear_fun = GearsBuilder().map(lambda x: x["value"]).sort().limit(1).run()
    res = rg.gears.pyexecute(gear_fun)
    assert res.results == ["1"]
    assert res.errors == []


@pytest.mark.xfail(
    sys.version_info[:2] != redis_py_ver[:2],
    reason="Incompatible Python versions",
    raises=redis.exceptions.ResponseError,
)
def test_sort(rg: RedisGears):
    rg.set("x", "1")
    rg.set("y", "1")
    rg.set("z", "2")
    rg.set("t", "2")
    gear_fun = GearsBuilder().map(lambda x: x["key"]).sort().run()
    res = rg.gears.pyexecute(gear_fun)
    assert res.results == ["t", "x", "y", "z"]
    assert res.errors == []


@pytest.mark.xfail(
    sys.version_info[:2] != redis_py_ver[:2],
    reason="Incompatible Python versions",
    raises=redis.exceptions.ResponseError,
)
def test_hashtag(rg: RedisGears):
    gear_fun = GearsBuilder("ShardsIDReader").map(lambda _: hashtag()).run()
    res = rg.gears.pyexecute(gear_fun)
    assert res.results == ["06S"]
    assert res.errors == []


@pytest.mark.xfail(
    sys.version_info[:2] != redis_py_ver[:2],
    reason="Incompatible Python versions",
    raises=redis.exceptions.ResponseError,
)
def test_register(rg: RedisGears):
    gear_fun = GearsBuilder("CommandReader").register(trigger="test")
    res = rg.gears.pyexecute(gear_fun)
    assert res is True
    assert rg.gears.trigger("test", "this", "is", "a", "test") == [
        b"['test', 'this', 'is', 'a', 'test']"
    ]
