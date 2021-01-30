import os
import re
from pathlib import Path

import pytest

import redgrease.client
from redgrease.client import RedisGears

# ################################# #
# Test Data and Python Gear Scripts #
# ################################# #


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


def test_install():
    pass


@pytest.mark.parametrize(
    "redgrease_import", ["import_none", "import_explicit", "import_all"]
)
@pytest.mark.parametrize(
    "gears_script",
    read("runtime_func_*.py"),
    ids=lambda x: x[0],
)
def test_builtin_runtime_functions(rg: RedisGears, gears_script, redgrease_import):
    name, contents = gears_script

    if redgrease_import == "import_explicit":
        script_str = contents
        assert explicit_redgrease_import.findall(script_str)
        assert "import *" not in script_str

    elif redgrease_import == "import_none":
        script_str = explicit_redgrease_import.sub("", contents)
        assert "redgrease" not in script_str

    elif redgrease_import == "import_all":
        implicit_import = "from redgrease import *"
        script_str = explicit_redgrease_import.sub(implicit_import, contents)
        assert implicit_import in script_str

    else:
        raise ValueError(f"Unsupported redgrease import case: '{redgrease_import}'")

    res = rg.gears.pyexecute(script_str)

    assert res
    assert isinstance(res, redgrease.client.Execution)
    assert not res.errors


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
