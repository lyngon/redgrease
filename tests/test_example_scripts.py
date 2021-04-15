# -*- coding: utf-8 -*-
"""
Tests for Redgrease overloads of the vanilla runtime functions that are loaded per
default at top level in ter Redis Gears Python runtime environment.
Such as `GearsBuilder`, `execute`, `log`, `atomic` etc.
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
import re
from pathlib import Path
from typing import Dict

import importlib_metadata
import pytest

from redgrease import RedisGears

redgrease_version = importlib_metadata.version("redgrease")

scripts_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), "gear_scripts")

explicit_redgrease_import = re.compile(r"from redgrease[^\n]+")


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


# ################################# #
# Tests                             #
# ################################# #
@pytest.mark.parametrize("enforce_redgrease", [True])
@pytest.mark.parametrize(
    "script_file",
    [
        gear_script("redislabs_example_wordcount.py"),
        gear_script("redislabs_mod_example_wordcount.py"),
    ],
)
def test_example_wordcount(rg: RedisGears, script_file: str, enforce_redgrease):
    lines = [
        "this is the first sentence of a lot of nonsense",
        "the lines don`t mean a thing at all",
        "the phrases are just words to count",
        "enough of this nonsense lines of words",
    ]
    reference_word_counts: Dict[str, int] = {}
    for i, line in enumerate(lines):
        rg.set(f"line:{i}", line)
        for word in line.split():
            reference_word_counts[word] = reference_word_counts.get(word, 0) + 1

        res = rg.gears.pyexecute(script_file, enforce_redgrease=enforce_redgrease)

        assert res
        assert isinstance(res.value, list)
        assert len(res.value) == len(reference_word_counts)
