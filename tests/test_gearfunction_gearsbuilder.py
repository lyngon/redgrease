# -*- coding: utf-8 -*-
"""
Tests for GearFunctions, aka 'Programmatic / Dynamic / Remote Gears'
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

import pytest
import redis.exceptions

from redgrease import GearsBuilder, RedisGears, execute, hashtag

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
    res = rg.gears.pyexecute(gear_fun)
    assert res == ["1", "2", "3"]
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
    assert res == ["1"]
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
    assert res == [1, 2, 3]
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
    assert res == ["1", "2", "3"]
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
    assert res == [("1", 2), ("2", 2)]
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
    assert res == 1.5
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
    assert res == 4
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
    assert res == 2
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
    assert res == 6
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
    assert res == [("1", 2), ("2", 4)]
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
    assert res == "1"
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
    assert res == ["t", "x", "y", "z"]
    assert res.errors == []


@pytest.mark.xfail(
    sys.version_info[:2] != redis_py_ver[:2],
    reason="Incompatible Python versions",
    raises=redis.exceptions.ResponseError,
)
def test_hashtag(rg: RedisGears):
    gear_fun = GearsBuilder("ShardsIDReader").map(lambda _: hashtag()).run()
    res = rg.gears.pyexecute(gear_fun)
    assert res == "06S"
    assert res.errors == []


@pytest.mark.xfail(
    sys.version_info[:2] != redis_py_ver[:2],
    reason="Incompatible Python versions",
    raises=redis.exceptions.ResponseError,
)
def test_register(rg: RedisGears):
    gear_fun = GearsBuilder("CommandReader").register(
        trigger="test", convertToStr=False
    )
    res = rg.gears.pyexecute(gear_fun)
    assert res
    res = rg.gears.trigger("test", "this", "is", "a", "test")
    assert res
    # Todo: Is this the desired output?
    assert res == [b"test", b"this", b"is", b"a", b"test"]
