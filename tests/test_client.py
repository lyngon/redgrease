import pytest

from redgrease.client import RedisGears


def bool_like(default=False):
    return [default, 0, 1, True, False]


def int_like(default=0, min=None, max=None):
    return [default] + [] if min is None else [min] + [] if min is None else [min]


# ^\s{4}[(\w.]*[\n\s]*\(\s*(["\w]+),\s+(\w+),\s*(\w+)[^)]+\),

gears_config_params = [
    {
        "config_name": "MaxExecutions",
        "read_response_type": int,
        "write_response_type": bool,
    },
    {
        "config_name": "MaxExecutionsPerRegistration",
        "read_response_type": int,
        "write_response_type": bool,
    },
    {
        "config_name": "ProfileExecutions",
        "read_response_type": bool,
        "write_response_type": bool,
    },
    {
        "config_name": "PythonAttemptTraceback",
        "read_response_type": bool,
        "write_response_type": bool,
    },
    {
        "config_name": "DownloadDeps",
        "read_response_type": bool,
        "write_response_type": False,
    },
    {
        "config_name": "DependenciesUrl",
        "read_response_type": str,
        "write_response_type": False,
    },  # URL like actually
    {
        "config_name": "DependenciesSha256",
        "read_response_type": str,
        "write_response_type": False,
    },
    {
        "config_name": "PythonInstallationDir",
        "read_response_type": str,
        "write_response_type": False,
    },  # Path
    {
        "config_name": "CreateVenv",
        "read_response_type": bool,
        "write_response_type": False,
    },
    {
        "config_name": "ExecutionThreads",
        "read_response_type": int,
        "write_response_type": False,
    },
    {
        "config_name": "ExecutionMaxIdleTime",
        "read_response_type": int,
        "write_response_type": bool,
    },
    {
        "config_name": "PythonInstallReqMaxIdleTime",
        "read_response_type": int,
        "write_response_type": bool,
    },
    {
        "config_name": "SendMsgRetries",
        "read_response_type": int,
        "write_response_type": bool,
    },
    {
        "config_name": "ThisConfigDoesNotExist",
        "read_response_type": Exception,
        "write_response_type": bool,
    },
    {"config_name": "", "read_response_type": Exception, "write_response_type": bool},
    {
        "config_name": None,
        "read_response_type": Exception,
        "write_response_type": Exception,
    },
    {"config_name": 42, "read_response_type": Exception, "write_response_type": bool},
]


@pytest.mark.parametrize("config", gears_config_params)
def test_configget(gears_instance: RedisGears, config):
    assert isinstance(gears_instance.configget(config["config_name"]), list)


@pytest.mark.xfail(reason="Testcase not implemented")
def test_configset(gears_instance: RedisGears):
    assert False


@pytest.mark.xfail(reason="Testcase not implemented")
def test_pyexecute(gears_instance: RedisGears):
    assert False


@pytest.mark.xfail(reason="Testcase not implemented")
def test_pydumpreqs(gears_instance: RedisGears):
    assert False


@pytest.mark.xfail(reason="Testcase not implemented")
def test_getexecution(gears_instance: RedisGears):
    assert False


@pytest.mark.xfail(reason="Testcase not implemented")
def test_getresults(gears_instance: RedisGears):
    assert False


@pytest.mark.xfail(reason="Testcase not implemented")
def test_getresultsblocking(gears_instance: RedisGears):
    assert False


@pytest.mark.xfail(reason="Testcase not implemented")
def test_dumpexecutions(gears_instance: RedisGears):
    assert False


@pytest.mark.xfail(reason="Testcase not implemented")
def test_dropexecution(gears_instance: RedisGears):
    assert False


@pytest.mark.xfail(reason="Testcase not implemented")
def test_abortexecution(gears_instance: RedisGears):
    assert False


@pytest.mark.xfail(reason="Testcase not implemented")
def test_trigger(gears_instance: RedisGears):
    assert False


@pytest.mark.xfail(reason="Testcase not implemented")
def test_dumpregistrations(gears_instance: RedisGears):
    assert False


@pytest.mark.xfail(reason="Testcase not implemented")
def test_unregister(gears_instance: RedisGears):
    assert False


@pytest.mark.xfail(reason="Testcase not implemented")
def test_pystats(gears_instance: RedisGears):
    assert False


@pytest.mark.xfail(reason="Testcase not implemented")
def test_infocluster(gears_instance: RedisGears):
    assert False


@pytest.mark.xfail(reason="Testcase not implemented")
def test_refreshcluster(gears_instance: RedisGears):
    assert False
