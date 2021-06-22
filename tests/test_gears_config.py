# -*- coding: utf-8 -*-
"""
Test Redgrease Config parameters.
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
import redis.exceptions

from redgrease import RedisGears


# @pytest.mark.parametrize("case", [""])  #, "upper", "lower"])
# @pytest.mark.parametrize("new_val", [1000, 1001])
def get_set_by_name(
    rg: RedisGears, name: str, orig_val, new_val, case: str, read_only: bool = False
):
    """Helper test function for repetitive testing of getters and setters
    based on config name strings.
    Note: it is assumed that the config has been set to the 'orig_val'
    prior to calling this function.

    Args:
        rg (redgrease.client.RedisGears): redis instance with gears
        name (str): Name of config option
        orig_val (Any): One config value to try
        new_val (Any): Another config value to write
        case (str): Either "upper", "lower" or anything Falsy.
            Indicates if the config name should be transformed
            to uppercase, lowercase or not at all
        read_only (bool): Indicates that write operations should
            be expected not to go through

    Raises:
        ValueError: [description]
    """
    # Get and Set by name
    if not case:
        n = name
    elif case == "upper":
        n = name.upper()
    elif case == "lower":
        n = name.lower()
    else:
        raise ValueError(f"Unknown string case {case}")

    dict_val = rg.gears.config.get(n)
    assert isinstance(dict_val, dict), f"Get dict, by '{n}' "
    assert dict_val[n] == orig_val

    if read_only:
        assert (
            rg.gears.config.set({n: new_val}) is False
        ), f"Set dict, by '{n}' not allowed"
        final_val = orig_val
    else:
        assert rg.gears.config.set({n: new_val}) is True, f"Set dict, by '{n}'"
        final_val = new_val

    assert (
        rg.gears.config.get_single(n) == final_val
    ), f"Get single value, by string '{n}'"


def raw(value):
    """Returns the expected raw (bytes) version of a value"""

    # Booleans are for some reason not supported by the Redis client,
    # but the logical translation would be as if it was int, so
    # that is how redgrease.client.Redis/RedisGears will handle booleans
    if isinstance(value, bool):
        value = int(value)

    if isinstance(value, (int, float)):
        value = str(value)

    if isinstance(value, str):
        value = value.encode()

    if isinstance(value, bytes):
        return value

    raise TypeError(
        f"Value {value} of type {type(value)} does not have a raw representation. AFAIK"
    )


# ########################################## #
# Test of Read- and Writeable Configurations #
# ########################################## #


@pytest.mark.parametrize("case", [""])  # , "upper", "lower"])
@pytest.mark.parametrize("new_val", [1000, 1001])
def test_MaxExecutions(rg: RedisGears, new_val, case):
    name = "MaxExecutions"
    read_response_type = int

    orig_val = rg.gears.config.MaxExecutions
    assert isinstance(
        orig_val, read_response_type
    ), f"Get '{name}' by Property of type {read_response_type}"

    rg.gears.config.MaxExecutions = new_val
    assert rg.gears.config.MaxExecutions == new_val, f"Set '{name}' by Property updated"

    assert (
        rg.gears.config.set(MaxExecutions=orig_val) is True
    ), f"Set '{name}' by Argument"

    get_set_by_name(rg, name, orig_val, new_val, case)


@pytest.mark.parametrize("case", [""])  # , "upper", "lower"])
@pytest.mark.parametrize("new_val", [100, 101])
def test_MaxExecutionsPerRegistration(rg: RedisGears, new_val, case):
    name = "MaxExecutionsPerRegistration"
    read_response_type = int

    orig_val = rg.gears.config.MaxExecutionsPerRegistration
    assert isinstance(
        orig_val, read_response_type
    ), f"Get '{name}' by Property of type {read_response_type}"

    rg.gears.config.MaxExecutionsPerRegistration = new_val
    assert (
        rg.gears.config.MaxExecutionsPerRegistration == new_val
    ), f"Set '{name}' by Property updated"

    assert (
        rg.gears.config.set(MaxExecutionsPerRegistration=orig_val) is True
    ), f"Set '{name}' by Argument"

    get_set_by_name(rg, name, orig_val, new_val, case)


@pytest.mark.parametrize("case", [""])  # , "upper", "lower"])
@pytest.mark.parametrize("new_val", [True, False])
def test_ProfileExecutions(rg: RedisGears, new_val, case):
    name = "ProfileExecutions"
    read_response_type = bool

    orig_val = rg.gears.config.ProfileExecutions
    assert isinstance(
        orig_val, read_response_type
    ), f"Get '{name}' by Property of type {read_response_type}"

    rg.gears.config.ProfileExecutions = new_val
    assert (
        rg.gears.config.ProfileExecutions == new_val
    ), f"Set '{name}' by Property updated"

    assert (
        rg.gears.config.set(ProfileExecutions=orig_val) is True
    ), f"Set '{name}' by Argument"

    get_set_by_name(rg, name, orig_val, new_val, case)


@pytest.mark.parametrize("case", [""])  # , "upper", "lower"])
@pytest.mark.parametrize("new_val", [True, False])
def test_PythonAttemptTraceback(rg: RedisGears, new_val, case):
    name = "PythonAttemptTraceback"
    read_response_type = bool

    orig_val = rg.gears.config.PythonAttemptTraceback
    assert isinstance(
        orig_val, read_response_type
    ), f"Get '{name}' by Property of type {read_response_type}"

    rg.gears.config.PythonAttemptTraceback = new_val
    assert (
        rg.gears.config.PythonAttemptTraceback == new_val
    ), f"Set '{name}' by Property updated"

    assert (
        rg.gears.config.set(PythonAttemptTraceback=orig_val) is True
    ), f"Set '{name}' by Argument"

    get_set_by_name(rg, name, orig_val, new_val, case)


@pytest.mark.parametrize("case", [""])  # , "upper", "lower"])
@pytest.mark.parametrize("new_val", [5, 6])
def test_ExecutionMaxIdleTime(rg: RedisGears, new_val, case):
    name = "ExecutionMaxIdleTime"
    read_response_type = int

    orig_val = rg.gears.config.ExecutionMaxIdleTime
    assert isinstance(
        orig_val, read_response_type
    ), f"Get '{name}' by Property of type {read_response_type}"

    rg.gears.config.ExecutionMaxIdleTime = new_val
    assert (
        rg.gears.config.ExecutionMaxIdleTime == new_val
    ), f"Set '{name}' by Property updated"

    assert (
        rg.gears.config.set(ExecutionMaxIdleTime=orig_val) is True
    ), f"Set '{name}' by Argument"

    get_set_by_name(rg, name, orig_val, new_val, case)


@pytest.mark.parametrize("case", [""])  # , "upper", "lower"])
@pytest.mark.parametrize("new_val", [30000, 30001])
def test_PythonInstallReqMaxIdleTime(rg: RedisGears, new_val, case):
    name = "PythonInstallReqMaxIdleTime"
    read_response_type = int

    orig_val = rg.gears.config.PythonInstallReqMaxIdleTime
    assert isinstance(
        orig_val, read_response_type
    ), f"Get '{name}' by Property of type {read_response_type}"

    rg.gears.config.PythonInstallReqMaxIdleTime = new_val
    assert (
        rg.gears.config.PythonInstallReqMaxIdleTime == new_val
    ), f"Set '{name}' by Property updated"

    assert (
        rg.gears.config.set(PythonInstallReqMaxIdleTime=orig_val) is True
    ), f"Set '{name}' by Argument"

    get_set_by_name(rg, name, orig_val, new_val, case)


@pytest.mark.parametrize("case", [""])  # , "upper", "lower"])
@pytest.mark.parametrize("new_val", [3, 4])
def test_SendMsgRetries(rg: RedisGears, new_val, case):
    name = "SendMsgRetries"
    read_response_type = int

    orig_val = rg.gears.config.SendMsgRetries
    assert isinstance(
        orig_val, read_response_type
    ), f"Get '{name}' by Property of type {read_response_type}"

    rg.gears.config.SendMsgRetries = new_val
    assert (
        rg.gears.config.SendMsgRetries == new_val
    ), f"Set '{name}' by Property updated"

    assert (
        rg.gears.config.set(SendMsgRetries=orig_val) is True
    ), f"Set '{name}' by Argument"

    get_set_by_name(rg, name, orig_val, new_val, case)


# ################################ #
# Test of Read-Only Configurations #
# ################################ #


@pytest.mark.parametrize("case", [""])  # , "upper", "lower"])
@pytest.mark.parametrize("new_val", [True, False])
def test_DownloadDeps(rg: RedisGears, new_val, case):
    name = "DownloadDeps"
    read_response_type = bool

    orig_val = rg.gears.config.DownloadDeps
    assert isinstance(
        orig_val, read_response_type
    ), f"Get '{name}' by Property of type {read_response_type}"

    with pytest.raises(AttributeError):
        rg.gears.config.DownloadDeps = new_val
    assert (
        rg.gears.config.DownloadDeps == orig_val
    ), f"Set '{name}' by Property not updated"

    assert (
        rg.gears.config.set(DownloadDeps=orig_val) is False
    ), f"Set '{name}' by Argument"

    get_set_by_name(rg, name, orig_val, new_val, case, read_only=True)


@pytest.mark.parametrize("case", [""])  # , "upper", "lower"])
@pytest.mark.parametrize("new_val", ["www.lyngon.com/gears.tgz"])
def test_DependenciesUrl(rg: RedisGears, new_val, case):
    name = "DependenciesUrl"
    read_response_type = str  # URL like actually

    orig_val = rg.gears.config.DependenciesUrl
    assert isinstance(
        orig_val, read_response_type
    ), f"Get '{name}' by Property of type {read_response_type}"

    with pytest.raises(AttributeError):
        rg.gears.config.DependenciesUrl = new_val
    assert (
        rg.gears.config.DependenciesUrl == orig_val
    ), f"Set '{name}' by Property updated"

    assert (
        rg.gears.config.set(DependenciesUrl=orig_val) is False
    ), f"Set '{name}' by Argument"

    get_set_by_name(rg, name, orig_val, new_val, case, read_only=True)


@pytest.mark.parametrize("case", [""])  # , "upper", "lower"])
@pytest.mark.parametrize("new_val", ["Hashibashi"])
def test_DependenciesSha256(rg: RedisGears, new_val, case):
    name = "DependenciesSha256"
    read_response_type = str

    orig_val = rg.gears.config.DependenciesSha256
    assert isinstance(
        orig_val, read_response_type
    ), f"Get '{name}' by Property of type {read_response_type}"

    with pytest.raises(AttributeError):
        rg.gears.config.DependenciesSha256 = new_val
    assert (
        rg.gears.config.DependenciesSha256 == orig_val
    ), f"Set '{name}' by Property updated"

    assert (
        rg.gears.config.set(DependenciesSha256=orig_val) is False
    ), f"Set '{name}' by Argument"

    get_set_by_name(rg, name, orig_val, new_val, case, read_only=True)


@pytest.mark.parametrize("case", [""])  # , "upper", "lower"])
@pytest.mark.parametrize("new_val", ["/home/bob/pictures"])
def test_PythonInstallationDir(rg: RedisGears, new_val, case):
    name = "PythonInstallationDir"
    read_response_type = str  # Path

    orig_val = rg.gears.config.PythonInstallationDir
    assert isinstance(
        orig_val, read_response_type
    ), f"Get '{name}' by Property of type {read_response_type}"

    with pytest.raises(AttributeError):
        rg.gears.config.PythonInstallationDir = new_val
    assert (
        rg.gears.config.PythonInstallationDir == orig_val
    ), f"Set '{name}' by Property updated"

    assert (
        rg.gears.config.set(PythonInstallationDir=orig_val) is False
    ), f"Set '{name}' by Argument"

    get_set_by_name(rg, name, orig_val, new_val, case, read_only=True)


@pytest.mark.parametrize("case", [""])  # , "upper", "lower"])
@pytest.mark.parametrize("new_val", [0, 1])
def test_CreateVenv(rg: RedisGears, new_val, case):
    name = "CreateVenv"
    read_response_type = bool

    orig_val = rg.gears.config.CreateVenv
    assert isinstance(
        orig_val, read_response_type
    ), f"Get '{name}' by Property of type {read_response_type}"

    with pytest.raises(AttributeError):
        rg.gears.config.CreateVenv = new_val
    assert rg.gears.config.CreateVenv == orig_val, f"Set '{name}' by Property updated"

    assert (
        rg.gears.config.set(CreateVenv=orig_val) is False
    ), f"Set '{name}' by Argument"

    get_set_by_name(rg, name, orig_val, new_val, case, read_only=True)


@pytest.mark.parametrize("case", [""])  # , "upper", "lower"])
@pytest.mark.parametrize("new_val", [1000, 1001])
def test_ExecutionThreads(rg: RedisGears, new_val, case):
    name = "ExecutionThreads"
    read_response_type = int

    orig_val = rg.gears.config.ExecutionThreads
    assert isinstance(
        orig_val, read_response_type
    ), f"Get '{name}' by Property of type {read_response_type}"

    with pytest.raises(AttributeError):
        rg.gears.config.ExecutionThreads = new_val
    assert (
        rg.gears.config.ExecutionThreads == orig_val
    ), f"Set '{name}' by Property updated"

    assert (
        rg.gears.config.set(ExecutionThreads=orig_val) is False
    ), f"Set '{name}' by Argument"

    get_set_by_name(rg, name, orig_val, new_val, case, read_only=True)


# ################################## #
# Test of Erroneous Usage and Other  #
# ################################## #


@pytest.mark.parametrize("new_val", ["Meaning", 42, True, 3.14])
def test_UnknownConfigName(rg: RedisGears, var, new_val):
    name = "ThisConfigDoesNotExist"

    with pytest.raises(AttributeError):
        rg.gears.config.ThisConfigDoesNotExist

    with pytest.raises(AttributeError):
        rg.gears.config.ThisConfigDoesNotExist = new_val

    with pytest.raises(AttributeError):
        rg.gears.config.ThisConfigDoesNotExist

    attr_name = var(name)  # For setting/getting by string, well use a unique name
    assert rg.gears.config.set({attr_name: new_val})

    # Special case for RedisGears version 1.2.0 and 1.2.1 due to bug (issue #554)
    # Issue is fixed but Official Docker container does not seem to have been updated.
    if rg.gears.gears_version() in [(1, 2, 0), (1, 2, 1)]:
        assert rg.gears.config.get_single(attr_name) == raw(new_val)


# Interestingly, Gears settings are allowed to be empty strings,
# so Redgrease will allow that too.
@pytest.mark.parametrize("new_val", [1])
@pytest.mark.parametrize("config_name", [None, ..., True, object(), int])
def test_InvalidConfigName(rg: RedisGears, config_name, new_val):

    with pytest.raises(redis.exceptions.DataError):
        rg.gears.config.set({config_name: new_val})

    with pytest.raises(KeyError):
        assert rg.gears.config.get_single(config_name) == raw(new_val)
