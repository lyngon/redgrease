# -*- coding: utf-8 -*-
"""


Todo:
    * Use config as much as possible

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

import pytest

import redgrease.data
import redgrease.utils as utils


class test_REnum:
    pass


class test_CaseInsensitiveDict:
    pass


@pytest.mark.parametrize(
    "input",
    [None, 0, 1, 2, 3.1415, object(), [], [1, 2, 3], {}, {"a": 1, "b": 2}, int, ...],
)
def test_as_is(input):
    assert utils.as_is(input) is input


@pytest.mark.parametrize(
    "input,output",
    [
        (b"", ""),
        (b"a", "a"),
        (b"Lyngon", "Lyngon"),
        ("", ""),
        ("a", "a"),
        ("Lyngon", "Lyngon"),
        (None, None),
        (42, 42),
        (3.14, 3.14),
        (..., ...),
    ],
)
def test_str_if_bytes(input, output):
    assert utils.str_if_bytes(input) == output


@pytest.mark.parametrize(
    "input,output",
    [
        (None, ""),
        ("", ""),
        ("a", "a"),
        ("Lyngon", "Lyngon"),
        (b"", ""),
        (b"a", "a"),
        (b"Lyngon", "Lyngon"),
        (42, "42"),
        (3.14, "3.14"),
        (..., "Ellipsis"),
    ],
)
def test_safe_str(input, output):
    assert utils.safe_str(input) == output


@pytest.mark.parametrize(
    "input,output",
    [
        (None, ""),
        ("", ""),
        ("a", "A"),
        ("aaa", "AAA"),
        (b"", ""),
        (b"a", "A"),
        (b"Lyngon", "LYNGON"),
        (42, "42"),
        (3.14, "3.14"),
        (..., "ELLIPSIS"),
    ],
)
def test_safe_str_upper(input, output):
    assert utils.safe_str_upper(input) == output


@pytest.mark.parametrize(
    "input,output",
    [
        (None, False),
        (0, False),
        (False, False),
        ([], False),
        ([1, 2, 3], False),
        ({"a": 1}, False),
        (object(), False),
        (..., False),
        ("", False),
        ("a", False),
        (" ", False),
        ("nok", False),
        ("Nok", False),
        ("NOK", False),
        ("n", False),
        ("N", False),
        ("no", False),
        ("No", False),
        ("NO", False),
        ("None", False),
        ("Nose", False),
        ("false", False),
        ("False", False),
        ("FALSE", False),
        (b"", False),
        (b"a", False),
        (b" ", False),
        (b"nok", False),
        (b"Nok", False),
        (b"NOK", False),
        (b"n", False),
        (b"N", False),
        (b"no", False),
        (b"No", False),
        (b"NO", False),
        (b"None", False),
        (b"Nose", False),
        (b"false", False),
        (b"False", False),
        (b"FALSE", False),
        ("ok", True),
        ("Ok", True),
        ("OK", True),
        ("y", False),
        ("Y", False),
        ("Yes", False),
        ("YES", False),
        ("true", False),
        ("True", False),
        ("TRUE", False),
        (b"ok", True),
        (b"Ok", True),
        (b"OK", True),
        (b"y", False),
        (b"Y", False),
        (b"Yes", False),
        (b"YES", False),
        (b"true", False),
        (b"True", False),
        (b"TRUE", False),
        (1, False),
        (42, False),
        (True, False),
    ],
)
def test_bool_ok(input, output):
    assert utils.bool_ok(input) == output


@pytest.mark.parametrize(
    "input,output",
    [
        (None, False),
        (0, False),
        (False, False),
        ([], False),
        ([1, 2, 3], False),
        ({"a": 1}, False),
        (object(), False),
        (..., False),
        ("", False),
        ("a", False),
        (" ", False),
        ("nok", False),
        ("Nok", False),
        ("NOK", False),
        ("n", False),
        ("N", False),
        ("no", False),
        ("No", False),
        ("NO", False),
        ("None", False),
        ("Nose", False),
        ("false", False),
        ("False", False),
        ("FALSE", False),
        (b"", False),
        (b"a", False),
        (b" ", False),
        (b"nok", False),
        (b"Nok", False),
        (b"NOK", False),
        (b"n", False),
        (b"N", False),
        (b"no", False),
        (b"No", False),
        (b"NO", False),
        (b"None", False),
        (b"Nose", False),
        (b"false", False),
        (b"False", False),
        (b"FALSE", False),
        ("ok", True),
        ("Ok", True),
        ("OK", True),
        ("y", True),
        ("Y", True),
        ("Yes", True),
        ("YES", True),
        ("true", True),
        ("True", True),
        ("TRUE", True),
        (b"ok", True),
        (b"Ok", True),
        (b"OK", True),
        (b"y", True),
        (b"Y", True),
        (b"Yes", True),
        (b"YES", True),
        (b"true", True),
        (b"True", True),
        (b"TRUE", True),
        (1, True),
        (42, True),
        (True, True),
    ],
)
def test_safe_bool(input, output):
    assert utils.safe_bool(input) == output


@pytest.mark.parametrize(
    "input,output",
    [
        (None, None),
        ("", ""),
        (" ", " "),
        (1, 1),
        (3.14, 3.14),
        (True, 1),
        (False, 0),
        ([], []),
        ([1, 2], [1, 2]),
        ([True, False], [True, False]),  # not recursive
    ],
)
def test_to_int_if_bool(input, output):
    assert utils.to_int_if_bool(input) == output


@pytest.mark.parametrize(
    "input,output",
    [
        (None, bytes()),
        (42, 42),
        (3.14, 3.14),
        ("Lyngon", b"Lyngon"),
        (b"Lyngon", b"Lyngon"),
        (True, 1),
        (False, 0),
        ([], b"[]"),
        ([1, 2, 3], str([1, 2, 3]).encode()),
        (redgrease.data.ExecutionStatus.created, b"created"),
        (redgrease.data.ExecutionStatus.running, b"running"),
        (redgrease.data.ExecLocality.Shard, b"Shard"),
        (redgrease.data.ExecLocality.Cluster, b"Cluster"),
        (redgrease.data.ExecID("someshardid", 42), b"someshardid-42"),
    ],
)
def test_to_redis_type(input, output):
    assert utils.to_redis_type(input) == output


@pytest.mark.parametrize(
    "input,output",
    [
        ({"mapping": None}, []),
        ({"mapping": {}}, []),
        ({"mapping": {"a": 1}}, ["a", 1]),
        ({"mapping": {"a": 1, "b": 2}}, ["a", 1, "b", 2]),
        (
            {"mapping": {True: 1, False: 0}, "key_transform": str},
            ["True", 1, "False", 0],
        ),
        (
            {"mapping": {"False": 0, "True": 1}, "val_transform": bool},
            ["False", False, "True", True],
        ),
        (
            {"mapping": {0: 0, 1: 1}, "key_transform": str, "val_transform": bool},
            ["0", False, "1", True],
        ),
    ],
)
def test_to_list(input, output):
    assert utils.to_list(**input) == output


@pytest.mark.parametrize(
    "input,output",
    [
        ({"items": None}, {}),
        ({"items": []}, {}),
        ({"items": ["foo", "bar"]}, {"foo": "bar"}),
        ({"items": range(4)}, {0: 1, 2: 3}),
        ({"items": [True, 42], "key_transform": str}, {"True": 42}),
        ({"items": ["meaning", "42"], "val_transform": int}, {"meaning": 42}),
        (
            {
                "items": [42, "42", True, True],
                "key_transform": str,
                "val_transform": int,
            },
            {"42": 42, "True": 1},
        ),
        (
            {
                "items": ["meaning", "42", "news", "False"],
                "val_transform": {"meaning": int, "news": utils.safe_bool},
            },
            {"meaning": 42, "news": False},
        ),
        (
            {
                "items": [0, "0", True, "True"],
                "key_transform": int,
                "val_transform": {0: int, 1: bool},
            },
            {0: 0, 1: True},
        ),
    ],
)
def test_to_dict(input, output):
    assert utils.to_dict(**input) == output


@pytest.mark.parametrize(
    "input,output",
    [
        (None, {}),
        ([], {}),
        (["foo", "bar"], {"foo": "bar"}),
        (range(4), {"0": 1, "2": 3}),
        ([True, 42], {"True": 42}),
        ([b"foo", b"bar", 13, 37], {"foo": b"bar", "13": 37}),
    ],
)
def test_to_kwargs(input, output):
    assert utils.to_kwargs(input) == output


@pytest.mark.parametrize(
    "item_type,input,output",
    [
        (str, [], []),
        (int, [], []),
        (str, [1], ["1"]),
        (str, [1, False], ["1", "False"]),
        (int, [True, "42"], [1, 42]),
    ],
)
def test_list_parser(item_type, input, output):
    parser = utils.list_parser(item_type)
    assert callable(parser)
    assert parser(input) == output


@pytest.mark.parametrize(
    "constructors,input,ckeys,output",
    [
        (
            {"meaning": int, "life": bool},
            ["42", 1],
            ["meaning", "life"],
            {"meaning": 42, "life": True},
        ),
        (
            {"meaning": int, "life": bool},
            [1, "42"],
            ["life", "meaning"],
            {"meaning": 42, "life": True},
        ),
        (
            {},
            [],
            [],
            {},
        ),
        (
            {"meaning": int, "life": bool},
            [],
            [],
            {},
        ),
        (
            {"meaning": int, "life": bool},
            ["42", 1],
            ["banana", "durian"],
            {"banana": "42", "durian": 1},
        ),
    ],
)
def test_dict_of(constructors, input, ckeys, output):
    dict_parser = utils.dict_of(constructors)
    assert callable(dict_parser)
    assert dict_parser(input, ckeys) == output
