import os
import re
import time
from pathlib import Path

import importlib_metadata
import pytest
import redis.exceptions

import redgrease.client
import redgrease.data
from redgrease.client import RedisGears

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
    "gears_script", read("buitin_runtime_func_*.py"), ids=lambda x: x[0]
)
def test_builtin_runtime_functions(
    redisgears_container, extras, gears_script, redgrease_import
):
    """Tests that
    (a) all Gears' builtin runtime functions are callable,
    (b) regardless how they were iported, and
    (c) regardless of which redgrease extras have been installed.

    Details:
    a). Gears' builtin runtime functions:
        execute, atomic, configGet, gearsConfigGet, hashtag and log.
        see https://oss.redislabs.com/redisgears/runtime.html
        Note: This test is not testing if these functions are doing the right thing,
        just that they dont cause errors at import or invovation.

    b). The builtin functions should be callable regardless how they are imported:
    - Imported explicitly, e.g: 'from redgrease import GB, log'

    - Imported implicitly with '*' notation, i.e: 'from redgrease import *'

    - Without any redgrease imports as all.

    c). The builtin functions should always work regardles of which
        redgrease extras are installed, icluding:
        - No extras, which means that no third party packages are installed
        - 'runtime' extras, which install the 'runtime.client' dependencies,
            such for example redis.
        - 'all', which install all redgrease optional dependencies.

    Note: This Test is far from optimized, and is currently for every test-case
    (currently 72 in total!) spinning up a fresh redisgears container and installs
    redgrease, beforer running the actual tests.
    This test takes several minutes to run.
    """

    host, port = redisgears_container.get_addr("6379/tcp")
    rg = RedisGears(host=host, port=port)

    while not rg.ping():
        time.sleep(1)

    assert (
        rg.gears.pyexecute("", requirements=[f"redgrease{extras}=={redgrease_version}"])
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
    assert isinstance(result, redgrease.data.Execution)
    assert not result.errors

    runtime_extras_import = """
import redgrease.command
GB().run()
"""
    redis_import = """
import redis
GB().run()
"""

    if extras:
        # With any of the extras installed, it should be
        # possibleto run gears that import both
        # 'redgrease.command' as well as 'redis'
        assert rg.gears.pyexecute(runtime_extras_import) is not None
        assert rg.gears.pyexecute(redis_import) is not None
    else:
        # If no redgrease extras are installed,
        # then we should not be able to import nither
        # 'redgrease.command' nor 'redis'
        with pytest.raises(redis.exceptions.ResponseError):
            assert rg.gears.pyexecute(runtime_extras_import)
        with pytest.raises(redis.exceptions.ResponseError):
            assert rg.gears.pyexecute(redis_import)
