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

import redgrease.utils
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
def test_keysreader_keys(rg: RedisGears):
    data = [("x", "1"), ("y", "2"), ("z", "3")]
    for d in data:
        rg.set(*d)

    gear_fun = KeysReader().keys().run()
    res = rg.gears.pyexecute(gear_fun)

    for d in data:
        assert d[0] in res
    assert res.errors == []


@pytest.mark.xfail(
    sys.version_info[:2] != redis_py_ver[:2],
    reason="Incompatible Python versions",
    raises=redis.exceptions.ResponseError,
)
def test_keysreader_values(rg: RedisGears):
    data = [("x", "1"), ("y", "2"), ("z", "3")]
    for d in data:
        rg.set(*d)

    gear_fun = KeysReader().values().run()
    res = rg.gears.pyexecute(gear_fun)

    for d in data:
        assert d[1] in res
    assert res.errors == []


@pytest.mark.xfail(
    sys.version_info[:2] != redis_py_ver[:2],
    reason="Incompatible Python versions",
    raises=redis.exceptions.ResponseError,
)
def test_keysreader_records(rg: RedisGears):
    keys = ["x", "y", "Z"]
    values = [1, 2, 3]
    data = zip(keys, values)

    for d in data:
        rg.set(*d)

    gear_fun = KeysReader().records().run()
    res = rg.gears.pyexecute(gear_fun)

    assert len(res) == len(list(data))
    for r in res:
        assert isinstance(r, redgrease.utils.Record)
        assert r.type == "string"
        assert r.value in values
        assert r.key in keys
        assert r.type is None

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
    gear_fun = CommandReader().register(trigger="test", convertToStr=False)
    res = rg.gears.pyexecute(gear_fun)
    assert res
    res = rg.gears.trigger("test", "this", "is", "a", "test")
    assert res
    assert res == ["test", "this", "is", "a", "test"]


@pytest.mark.xfail(
    sys.version_info[:2] != redis_py_ver[:2],
    reason="Incompatible Python versions",
    raises=redis.exceptions.ResponseError,
)
def test_command_reader_args(rg: RedisGears):
    gear_fun = CommandReader().args().register(trigger="test", convertToStr=False)

    res = rg.gears.pyexecute(gear_fun)
    assert res
    res = rg.gears.trigger("test", "this", "is", "a", "test")
    assert res
    assert res == ["this", "is", "a", "test"]


@pytest.mark.xfail(
    sys.version_info[:2] != redis_py_ver[:2],
    reason="Incompatible Python versions",
    raises=redis.exceptions.ResponseError,
)
def test_command_reader_args_2(rg: RedisGears):
    gear_fun = CommandReader().args(2).register(trigger="test", convertToStr=False)

    res = rg.gears.pyexecute(gear_fun)
    assert res
    res = rg.gears.trigger("test", "this", "is", "a", "test")
    assert res
    assert res == ["this", "is"]


@pytest.mark.xfail(
    sys.version_info[:2] != redis_py_ver[:2],
    reason="Incompatible Python versions",
    raises=redis.exceptions.ResponseError,
)
def test_command_reader_apply(rg: RedisGears):
    def sum_args_len(*args):
        return sum([len(a) for a in args])

    gear_fun = (
        CommandReader().apply(sum_args_len).register(trigger="test", convertToStr=False)
    )

    res = rg.gears.pyexecute(gear_fun)
    assert res
    res = rg.gears.trigger("test", "this", "is", "a", "test")
    assert res
    assert res == 11


@pytest.mark.xfail(
    sys.version_info[:2] != redis_py_ver[:2],
    reason="Incompatible Python versions",
    raises=redis.exceptions.ResponseError,
)
def test_pythonreader(rg: RedisGears):
    def generator():
        for x in range(6379):
            yield x

    gearfun = PythonReader().run(generator)

    res = rg.gears.pyexecute(gearfun)

    vals = list(generator())

    assert len(res) == len(vals)

    assert all([r == g for r, g in zip(res, vals)])


@pytest.mark.xfail(
    sys.version_info[:2] != redis_py_ver[:2],
    reason="Incompatible Python versions",
    raises=redis.exceptions.ResponseError,
)
@pytest.mark.xfail(reason="RedisGears Bug?")
def test_streamreader_implicit(rg: RedisGears):
    stream_name = "MyStream"

    data = [
        {"nr": 1, "msg": "Hello!"},
        {"nr": 2, "msg": "Hi!"},
        {"nr": 3, "msg": "Good day!"},
    ]

    for d in data:
        rg.xadd(stream_name, d)

    gearfun = StreamReader().run()

    res = rg.gears.pyexecute(gearfun)
    assert len(res) == len(data)

    for r in res:
        assert r["value"] in data


@pytest.mark.xfail(
    sys.version_info[:2] != redis_py_ver[:2],
    reason="Incompatible Python versions",
    raises=redis.exceptions.ResponseError,
)
def test_streamreader_explicit(rg: RedisGears):
    stream_name = "MyStream"

    messages = [
        {"cnt": 1, "say": "Hello!"},
        {"cnt": 2, "say": "Hi!"},
        {"cnt": 3, "say": "Good day!"},
    ]

    for msg in messages:
        rg.xadd(stream_name, msg)

    gearfun = StreamReader(stream_name).run()

    res = rg.gears.pyexecute(gearfun)

    assert len(res) == len(messages)

    for r in res:
        assert r["key"] == stream_name
        assert isinstance(r["id"], str)
        assert r["id"]
        assert any(
            [all([r["value"][k] == str(v) for k, v in msg.items()]) for msg in messages]
        )


@pytest.mark.xfail(
    sys.version_info[:2] != redis_py_ver[:2],
    reason="Incompatible Python versions",
    raises=redis.exceptions.ResponseError,
)
def test_streamreader_explicit_values(rg: RedisGears):
    stream_name = "MyStream"

    messages = [
        {"cnt": 1, "say": "Hello!"},
        {"cnt": 2, "say": "Hi!"},
        {"cnt": 3, "say": "Good day!"},
    ]

    for msg in messages:
        rg.xadd(stream_name, msg)

    gearfun = StreamReader(stream_name).values().run()

    res = rg.gears.pyexecute(gearfun)

    assert len(res) == len(messages)

    for r in res:
        assert any([all([r[k] == str(v) for k, v in msg.items()]) for msg in messages])


@pytest.mark.xfail(
    sys.version_info[:2] != redis_py_ver[:2],
    reason="Incompatible Python versions",
    raises=redis.exceptions.ResponseError,
)
def test_streamreader_explicit_keys(rg: RedisGears):
    stream_name = "MyStream"

    messages = [
        {"cnt": 1, "say": "Hello!"},
        {"cnt": 2, "say": "Hi!"},
        {"cnt": 3, "say": "Good day!"},
    ]

    for msg in messages:
        rg.xadd(stream_name, msg)

    gearfun = StreamReader(stream_name).keys().run()

    res = rg.gears.pyexecute(gearfun)

    assert len(res) == len(messages)

    for r in res:
        assert r == stream_name


@pytest.mark.xfail(
    sys.version_info[:2] != redis_py_ver[:2],
    reason="Incompatible Python versions",
    raises=redis.exceptions.ResponseError,
)
def test_streamreader_explicit_records(rg: RedisGears):
    stream_name = "MyStream"

    messages = [
        {"cnt": 1, "say": "Hello!"},
        {"cnt": 2, "say": "Hi!"},
        {"cnt": 3, "say": "Good day!"},
    ]

    for msg in messages:
        rg.xadd(stream_name, msg)

    gearfun = StreamReader(stream_name).records().run()

    res = rg.gears.pyexecute(gearfun)

    assert len(res) == len(messages)

    for r in res:
        assert isinstance(r, redgrease.utils.StreamRecord)
        assert r.key == stream_name
        assert isinstance(r.key, str)
        assert r.key
        assert any(
            [all([r.value[k] == str(v) for k, v in msg.items()]) for msg in messages]
        )
