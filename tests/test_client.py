import os
from pathlib import Path

import pytest

from redgrease.client import RedisGears

# ################################# #
# Test Data and Python Gear Scripts #
# ################################# #


scripts_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), "gear_scripts")


def gear_script(file):
    return os.path.join(scripts_dir, file)


def gear_scripts(file_pattern):
    directory = Path(str(scripts_dir))
    return directory.rglob(file_pattern)


# @pytest.fixture()
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


# @pytest.mark.parametrize(
#     "redgrease_import", ["import_none", "import_explicit", "import_implicit"]
# )
@pytest.mark.parametrize(
    "gears_script",
    read("runtime_func_*.py"),
    ids=lambda x: x[0],
)
def test_pyexecute(rg: RedisGears, gears_script):
    name, contents = gears_script
    res = rg.gears.pyexecute(contents, requirements=["redgrease"])
    assert res


@pytest.mark.xfail(reason="Testcase not implemented")
def test_pydumpreqs(rg: RedisGears):
    assert False


@pytest.mark.xfail(reason="Testcase not implemented")
def test_getexecution(rg: RedisGears):
    assert False


@pytest.mark.xfail(reason="Testcase not implemented")
def test_getresults(rg: RedisGears):
    assert False


@pytest.mark.xfail(reason="Testcase not implemented")
def test_getresultsblocking(rg: RedisGears):
    assert False


@pytest.mark.xfail(reason="Testcase not implemented")
def test_dumpexecutions(rg: RedisGears):
    assert False


@pytest.mark.xfail(reason="Testcase not implemented")
def test_dropexecution(rg: RedisGears):
    assert False


@pytest.mark.xfail(reason="Testcase not implemented")
def test_abortexecution(rg: RedisGears):
    assert False


@pytest.mark.xfail(reason="Testcase not implemented")
def test_trigger(rg: RedisGears):
    assert False


@pytest.mark.xfail(reason="Testcase not implemented")
def test_dumpregistrations(rg: RedisGears):
    assert False


@pytest.mark.xfail(reason="Testcase not implemented")
def test_unregister(rg: RedisGears):
    assert False


@pytest.mark.xfail(reason="Testcase not implemented")
def test_pystats(rg: RedisGears):
    assert False


@pytest.mark.xfail(reason="Testcase not implemented")
def test_infocluster(rg: RedisGears):
    assert False


@pytest.mark.xfail(reason="Testcase not implemented")
def test_refreshcluster(rg: RedisGears):
    assert False
