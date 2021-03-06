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
import time
from pathlib import Path

import importlib_metadata
import pytest
import redis.exceptions

import redgrease.data
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


@pytest.mark.parametrize(
    "extras", ["", "[runtime]", "[all]"], ids=lambda x: f"redgrease{x}"
)
@pytest.mark.parametrize(
    "redgrease_import", ["import_none", "import_explicit", "import_all"]
)
@pytest.mark.parametrize(
    "gears_script", read("builtin_runtime_func_*.py"), ids=lambda x: x[0]
)
def test_builtin_runtime_functions(
    redisgears_container, extras, gears_script, redgrease_import, base_packages
):
    """Tests that
    (a) all Gears' builtin runtime functions are callable,
    (b) regardless how they were imported, and
    (c) regardless of which redgrease extras have been installed.

    Details:
    a). Gears' builtin runtime functions:
        execute, atomic, configGet, gearsConfigGet, hashtag and log.
        see https://oss.redislabs.com/redisgears/runtime.html
        Note: This test is not testing if these functions are doing the right thing,
        just that they don't cause errors at import or invovation.

    b). The builtin functions should be callable regardless how they are imported:
    - Imported explicitly, e.g: 'from redgrease import GB, log'

    - Imported implicitly with '*' notation, i.e: 'from redgrease import *'

    - Without any redgrease imports as all.

    c). The builtin functions should always work regardles of which
        redgrease extras are installed, including:
        - No extras, which means that no third party packages are installed
        - 'runtime' extras, which install the 'runtime.client' dependencies,
            such for example redis.
        - 'all', which install all redgrease optional dependencies.

    Note: This Test is far from optimized, and is currently for every test-case
    (currently 72 in total!) spinning up a fresh redisgears container and installs
    redgrease, before running the actual tests.
    This test takes several minutes to run.
    """

    host, port = redisgears_container.get_addr("6379/tcp")
    rg = RedisGears(host=host, port=port)

    while not rg.ping():
        time.sleep(1)

    print(f">>>> RUNTIME : {base_packages}")

    # TODO: this ain't pretty... really need to sort this mess up.
    assert (
        # rg.gears.pyexecute(requirements=[f"redgrease{extras}=={redgrease_version}"])
        rg.gears.pyexecute(
            requirements=map(
                lambda r: str.replace(str(r), "[runtime]", extras), base_packages
            )
        )
        is not None
    )

    _, contents = gears_script

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

    result = rg.gears.pyexecute(script_str)

    assert result is not None
    assert isinstance(result, redgrease.data.ExecutionResult)
    assert not result.errors

    runtime_extras_import = """
from redgrease import cmd
GB().run()
"""
    redis_import = """
import redis
GB().run()
"""

    if extras:
        # With any of the extras installed, it should be
        # possible to run gears that import both
        # 'redgrease.runtime_client' as well as 'redis'
        assert rg.gears.pyexecute(runtime_extras_import) is not None
        assert rg.gears.pyexecute(redis_import) is not None
    else:
        # If no redgrease extras are installed,
        # then we should not be able to import nither
        # 'redgrease.runtime_client' nor 'redis'
        with pytest.raises(redis.exceptions.ResponseError):
            assert rg.gears.pyexecute(runtime_extras_import)
        with pytest.raises(redis.exceptions.ResponseError):
            assert rg.gears.pyexecute(redis_import)
