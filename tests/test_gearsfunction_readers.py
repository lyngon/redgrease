# -*- coding: utf-8 -*-
"""
Tests for the various Reader shorthand GearFunctions, aka 'Programmatic / Dynamic /
Remote Gears'
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

from redgrease import (
    CommandReader,
    KeysOnlyReader,
    KeysReader,
    PythonReader,
    RedisGears,
    ShardsIDReader,
    StreamReader,
    execute,
    hashtag,
)

counter = 0


redis_py_ver = (3, 7, 2, "final", 0)


@pytest.mark.xfail(
    sys.version_info[:2] != redis_py_ver[:2],
    reason="Incompatible Python versions",
    raises=redis.exceptions.ResponseError,
)
def test_keysreader(rg: RedisGears):
    rg.set("x", "1")
    rg.set("y", "2")
    rg.set("z", "3")
    gear_fun = KeysReader().map(lambda x: x["value"]).filter(lambda x: x == "1").run()
    res = rg.gears.pyexecute(gear_fun)
    assert res == ["1"]
    assert res.errors == []


@pytest.mark.xfail(
    sys.version_info[:2] != redis_py_ver[:2],
    reason="Incompatible Python versions",
    raises=redis.exceptions.ResponseError,
)
def test_keysonlyreader(rg: RedisGears):
    rg.lpush("l", "1", "2", "3")

    gear_fun = (
        KeysOnlyReader()
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
def test_shardsidreader(rg: RedisGears):
    gear_fun = ShardsIDReader().map(lambda _: hashtag()).run()
    res = rg.gears.pyexecute(gear_fun)
    assert res == "06S"
    assert res.errors == []


@pytest.mark.xfail(
    sys.version_info[:2] != redis_py_ver[:2],
    reason="Incompatible Python versions",
    raises=redis.exceptions.ResponseError,
)
def test_command_reader(rg: RedisGears):
    gear_fun = CommandReader().register(trigger="test")
    res = rg.gears.pyexecute(gear_fun)
    assert res
    res = rg.gears.trigger("test", "this", "is", "a", "test")
    assert res
    # Todo: Is this the desired output?
    assert res == [b"test", b"this", b"is", b"a", b"test"]


@pytest.mark.xfail(reason="Testcase not implemented")
@pytest.mark.xfail(
    sys.version_info[:2] != redis_py_ver[:2],
    reason="Incompatible Python versions",
    raises=redis.exceptions.ResponseError,
)
def test_pythonreader():
    gearfun = PythonReader()
    raise NotImplementedError(str(gearfun))


@pytest.mark.xfail(reason="Testcase not implemented")
@pytest.mark.xfail(
    sys.version_info[:2] != redis_py_ver[:2],
    reason="Incompatible Python versions",
    raises=redis.exceptions.ResponseError,
)
def test_streamreader():
    gearfun = StreamReader()
    raise NotImplementedError(str(gearfun))
