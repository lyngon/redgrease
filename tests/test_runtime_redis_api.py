# -*- coding: utf-8 -*-
"""
Tests for the serverside redis commands.s
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

import os
from pathlib import Path

import pytest

from redgrease import RedisGears
from redgrease.utils import safe_str

scripts_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), "gear_scripts")


def gear_script(file):
    return os.path.join(scripts_dir, file)


def gear_scripts(file_pattern):
    directory = Path(str(scripts_dir))
    return directory.rglob(file_pattern)


def read(file_pattern):
    directory = Path(str(scripts_dir))
    files_contents = []
    for file_path in directory.rglob(file_pattern):
        with open(file_path, "r") as file:
            files_contents.append((file_path.name, file.read()))
    return files_contents


@pytest.mark.parametrize(
    "script", read("redgrease_runtime_redis_api_*.py"), ids=lambda x: x[0][-10:]
)
def test_basic(rg: RedisGears, script):
    script_name, script_contents = script
    # First set no keys
    res_0 = rg.gears.pyexecute(script_contents)
    assert res_0 is not None
    assert res_0 == []

    orig_val = 13
    key = "NUM"

    assert rg.set(key, orig_val)

    res_1 = rg.gears.pyexecute(script_contents)
    assert res_1 is not None
    assert res_1
    assert res_1.errors == []

    assert float(safe_str(rg.get(key))) == orig_val * 2
