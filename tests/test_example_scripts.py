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
import ast
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Dict

import importlib_metadata
import pytest

from redgrease import RedisGears
from redgrease.utils import safe_str

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
    "script_file,vanilla",
    [
        (gear_script("redislabs_example_wordcount.py"), True),
        (gear_script("redislabs_mod_example_wordcount.py"), False),
    ],
    ids=lambda arg: Path(arg).name
    if isinstance(arg, str) and arg.endswith(".py")
    else None,
)
def test_example_wordcount(
    rg: RedisGears, script_file: str, vanilla: bool, enforce_redgrease
):
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
        for word_count in res.value:
            if vanilla:
                word_count = ast.literal_eval(safe_str(word_count))

            assert word_count["value"] == reference_word_counts[word_count["key"]]


@pytest.mark.parametrize("enforce_redgrease", [True])
@pytest.mark.parametrize(
    "script_file,vanilla",
    [
        (gear_script("redislabs_example_deletebykeyprefix.py"), True),
        (gear_script("redislabs_mod_example_deletebykeyprefix.py"), False),
    ],
    ids=lambda arg: Path(arg).name
    if isinstance(arg, str) and arg.endswith(".py")
    else None,
)
def test_example_deletebykeprefix(
    rg: RedisGears, script_file: str, vanilla: bool, enforce_redgrease
):

    rg.set("delete_me:unimportant", "Merble!")
    rg.set("keep_me:important", "Gerbil!")
    rg.hset("delete_me:data", mapping={"a": 1, "b": 2})
    rg.xadd("dont_delete_me:", {"some": "interesting", "data": "here"})

    assert len(rg.keys()) == 4

    res = rg.gears.pyexecute(script_file, enforce_redgrease=enforce_redgrease)

    assert res.value
    assert len(rg.keys()) == 2


@pytest.mark.parametrize("enforce_redgrease", [True])
@pytest.mark.parametrize(
    "script_file,vanilla",
    [
        (gear_script("redislabs_example_basicredisstreamprocessing.py"), True),
        (gear_script("redislabs_mod_example_basicredisstreamprocessing.py"), False),
    ],
    ids=lambda arg: Path(arg).name
    if isinstance(arg, str) and arg.endswith(".py")
    else None,
)
def test_example_basicredisstreamprocessing(
    rg: RedisGears, script_file: str, vanilla: bool, enforce_redgrease
):
    res = rg.gears.pyexecute(script_file, enforce_redgrease=enforce_redgrease)

    assert res.value is True

    original_message = {"a": 1, "b": 2}
    msgid = rg.xadd("mystream", original_message)
    time.sleep(0.1)
    message_envelope = rg.hgetall(msgid)

    assert message_envelope

    stored_message = message_envelope[b"value"]

    stored_message = ast.literal_eval(safe_str(stored_message))

    assert message_envelope[b"key"] == b"mystream"
    assert message_envelope[b"id"] == msgid

    assert len(original_message) == len(stored_message)
    print(f"MESSAGE: {stored_message}")
    for k, v in stored_message.items():
        assert safe_str(original_message[safe_str(k)]) == safe_str(v)


@pytest.mark.parametrize("enforce_redgrease", [True])
@pytest.mark.parametrize(
    "script_file,vanilla",
    [
        (gear_script("redislabs_example_automaticexpiry.py"), True),
        (gear_script("redislabs_mod_example_automaticexpiry.py"), False),
    ],
    ids=lambda arg: Path(arg).name
    if isinstance(arg, str) and arg.endswith(".py")
    else None,
)
def test_example_automaticexpiry(
    rg: RedisGears, script_file: str, vanilla: bool, enforce_redgrease
):
    res = rg.gears.pyexecute(script_file, enforce_redgrease=enforce_redgrease)
    assert res.value is True

    rg.set("Foo", "Bar")
    time.sleep(0.1)
    ttl = rg.ttl("Foo")
    assert int(ttl) <= 3600
    assert int(ttl) > 3590


@pytest.mark.parametrize("enforce_redgrease", [True])
@pytest.mark.parametrize(
    "script_file,vanilla",
    [
        (gear_script("redislabs_example_keyspacenotificationprocessing.py"), True),
        (gear_script("redislabs_mod_example_keyspacenotificationprocessing.py"), False),
    ],
    ids=lambda arg: Path(arg).name
    if isinstance(arg, str) and arg.endswith(".py")
    else None,
)
def test_example_keyspacenotificationprocessing(
    redgrease_runtime_container,
    rg: RedisGears,
    script_file: str,
    vanilla: bool,
    enforce_redgrease,
):
    res = rg.gears.pyexecute(script_file, enforce_redgrease=enforce_redgrease)
    assert res.value is True

    rg.set("Foo", "Bar", ex=1)
    expected_log_entry = "Key 'Foo' expired at "
    logs = redgrease_runtime_container.logs()
    assert expected_log_entry not in logs
    time.sleep(1.5)
    logs = redgrease_runtime_container.logs()
    assert expected_log_entry in logs


@pytest.mark.parametrize("enforce_redgrease", [True])
@pytest.mark.parametrize(
    "script_file,vanilla",
    [
        (gear_script("redislabs_example_reliablekeyspacenotification.py"), True),
        (gear_script("redislabs_mod_example_reliablekeyspacenotification.py"), False),
    ],
    ids=lambda arg: Path(arg).name
    if isinstance(arg, str) and arg.endswith(".py")
    else None,
)
def test_example_reliablekeyspacenotification(
    rg: RedisGears, script_file: str, vanilla: bool, enforce_redgrease
):
    # All hash records matching "person:*" are written
    # to the stream "notifications-stream"
    res = rg.gears.pyexecute(script_file, enforce_redgrease=enforce_redgrease)
    assert res.value is True

    rg.set("person:ignore", "Igno Re")  # should be ignored because it is not a hash
    rg.hset(
        "dog:Fido", mapping={"gender": "boy", "behavior": "good"}
    )  # should be ignored because of key name

    assert not rg.exists("notifications-stream")

    msg = {"gender": "male", "behavior": "naugty"}
    rg.hset("person:anders", mapping=msg)

    time.sleep(0.5)

    assert rg.exists("notifications-stream")
    events = rg.xrange("notifications-stream")

    assert len(events) == 1
    _, msg_envelope = events[0]
    assert safe_str(msg_envelope[b"key"]) == "person:anders"
    assert safe_str(msg_envelope[b"value"]) == safe_str(msg)


@pytest.mark.parametrize("enforce_redgrease", [True])
@pytest.mark.parametrize(
    "script_file,vanilla",
    [
        (gear_script("redislabs_example_gearsFuture.py"), True),
        (gear_script("redislabs_mod_example_gearsFuture.py"), False),
    ],
    ids=lambda arg: Path(arg).name
    if isinstance(arg, str) and arg.endswith(".py")
    else None,
)
@pytest.mark.xfail(
    True,
    reason="RedisGears Version??",
    raises=Exception,
)
def test_example_gearsFuture(
    rg: RedisGears, script_file: str, vanilla: bool, enforce_redgrease
):
    ttl = 3
    res = rg.gears.pyexecute(script_file, enforce_redgrease=enforce_redgrease)
    assert res.value is True

    start_time = datetime.now()
    rg.set("Foo", "bar")
    rg.expire("Foo", ttl)
    res = rg.gears.trigger("WaitForKeyExpiration", "Foo")
    duration = datetime.now() - start_time
    assert res
    assert res == "Foo expired"
    assert duration.seconds >= ttl
