# -*- coding: utf-8 -*-
"""
Tests for Redgrease extension fuctons to the  vanilla runtime.
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

import redgrease
from redgrease.utils import safe_str

redis_py_ver = (3, 7, 2, "final", 0)


@pytest.mark.xfail(
    sys.version_info[:2] != redis_py_ver[:2],
    reason="Incompatible Python versions",
    raises=redis.exceptions.ResponseError,
)
def test_hastag3(rg: redgrease.RedisGears):
    def once():
        yield 1

    redgrease.PythonReader().map(
        lambda _: redgrease.cmd.set("hashtag", redgrease.hashtag())
    ).run(once, on=rg)

    redgrease.PythonReader().map(
        lambda _: redgrease.cmd.set("hashtag3", redgrease.hashtag3())
    ).run(once, on=rg)

    ht = rg.get("hashtag")
    ht3 = rg.get("hashtag3")

    assert safe_str(ht3) == "{" + safe_str(ht) + "}"
    assert f"somethting {safe_str(ht3)} something"
