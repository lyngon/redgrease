# -*- coding: utf-8 -*-
"""
Tests for the RedGrease client.
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


import time
from typing import List

import pytest
import redis

import redgrease
import redgrease.client
import redgrease.data
from redgrease.utils import safe_str, str_if_bytes


def test_create_gear(server_connection_params):
    r = redis.Redis(**server_connection_params)

    rg = redgrease.Gears(r)

    r.set("Foo", 1)
    r.set("Bar", 2)
    r.set("Baz", 3)

    res = rg.pyexecute("GB().run()")
    assert res
    assert isinstance(res, redgrease.data.ExecutionResult)
    assert len(res) == 3

    pystats = rg.pystats()
    assert pystats
    assert isinstance(pystats, redgrease.data.PyStats)


@pytest.mark.parametrize("package", ["numpy"])
def test_pydumpreqs(rg: redgrease.RedisGears, package):
    orig_reqs = rg.gears.pydumpreqs()
    assert isinstance(orig_reqs, list)

    preexisting = []
    for req in orig_reqs:
        assert req
        assert isinstance(req, redgrease.client.PyRequirementInfo)
        assert req.GearReqVersion
        assert isinstance(req.GearReqVersion, int)
        assert req.Name
        assert isinstance(req.Name, str)
        preexisting.append(req.Name)
        assert req.IsDownloaded
        assert isinstance(req.IsDownloaded, bool)
        assert req.IsInstalled
        assert isinstance(req.IsInstalled, bool)
        assert req.Wheels
        assert isinstance(req.Wheels, list)

    if package not in preexisting:
        # Add a requirement for package
        assert rg.gears.pyexecute("", requirements=[package])

        new_reqs = rg.gears.pydumpreqs()

        assert any(r.Name == package for r in new_reqs)


@pytest.mark.parametrize(
    "arg_list",
    [[], ["Olufsen"], ["bang", "bang!"], [4, "the", "buck"]],
    ids=lambda x: f"{len(x)}arg",
)
@pytest.mark.parametrize("mode", ["blocking", "unblocking"])
def test_trigger(rg: redgrease.RedisGears, arg_list: List, mode: str):
    trigger_name = "Bang"
    unblocking: bool = mode == "unblocking"
    fun_str = (
        """GB('CommandReader')"""
        """.flatmap(lambda x: x)."""
        f"""register(trigger='{trigger_name}')"""
    )
    assert rg.gears.pyexecute(fun_str, unblocking=unblocking)
    res = rg.gears.trigger(trigger_name, *arg_list)
    assert isinstance(res, redgrease.data.ExecutionResult)
    # assert isinstance(res.value, list)
    assert len(res) == len(arg_list) + 1
    # For some reason flatmap seems to reverse the order
    str_res = list(map(safe_str, res))
    for arg in arg_list:
        assert str(arg) in str_res


@pytest.mark.parametrize("mode", ["blocking", "unblocking"])
def test_dumpregistrations(rg: redgrease.RedisGears, mode: str):
    trigger_name = "Bang"
    unblocking: bool = mode == "unblocking"
    fun_str = (
        """GB('CommandReader')"""
        """.flatmap(lambda x: x)."""
        f"""register(trigger='{trigger_name}')"""
    )
    assert rg.gears.pyexecute(fun_str, unblocking=unblocking)
    registrations_list = rg.gears.dumpregistrations()
    assert registrations_list
    assert isinstance(registrations_list, list)
    assert len(registrations_list) == 1
    reg = registrations_list[0]
    assert reg
    assert isinstance(reg, redgrease.data.Registration)
    assert reg.id
    assert isinstance(reg.id, redgrease.data.ExecID)
    assert reg.reader
    assert isinstance(
        reg.reader, str
    )  # This should maybe be redgrease.Reader but as enum : Issue #4
    assert reg.desc is None  # Not sure when and how this field isr set

    # # Registration data
    assert reg.RegistrationData
    assert isinstance(reg.RegistrationData, redgrease.data.RegData)
    rdat = reg.RegistrationData
    assert rdat.mode
    assert isinstance(
        rdat.mode, str
    )  # This should maybe le redgrease.TriggerMode, but as enum : Issue #5
    assert rdat.mode == redgrease.TriggerMode.Async
    assert isinstance(rdat.numTriggered, int)
    assert rdat.numTriggered == 0
    assert isinstance(rdat.numSuccess, int)
    assert rdat.numSuccess == 0
    assert isinstance(rdat.numFailures, int)
    assert rdat.numFailures == 0
    assert isinstance(rdat.numAborted, int)
    assert rdat.numAborted == 0
    assert rdat.lastError is None
    assert rdat.args
    assert isinstance(rdat.args, dict)

    # # This is only true for CommandReader registered with a trigger
    assert "trigger" in rdat.args
    assert str_if_bytes(rdat.args["trigger"]) == trigger_name

    # # Private Data. Not really important how it is returned
    # # But want to know if it changes for some reason
    assert reg.PD
    assert isinstance(reg.PD, dict)
    assert "sessionId" in reg.PD.keys()
    assert "depsList" in reg.PD.keys()
    assert isinstance(reg.PD["depsList"], list)
    assert reg.PD["depsList"] == []

    # # Chec that NumTriggered and NumSuccess increase after a trigger
    assert rg.gears.trigger(trigger_name)
    registrations_list_2 = rg.gears.dumpregistrations()
    reg2 = registrations_list_2[0]
    assert reg2.RegistrationData.numTriggered == reg.RegistrationData.numTriggered + 1
    assert reg2.RegistrationData.numSuccess == reg.RegistrationData.numSuccess + 1


def test_unregister(rg: redgrease.RedisGears):
    trigger_name = "Bang"
    fun_str = (
        """GB('CommandReader')"""
        """.flatmap(lambda x: x)."""
        f"""register(trigger='{trigger_name}')"""
    )
    assert rg.gears.pyexecute(fun_str)
    registrations_list = rg.gears.dumpregistrations()
    exec_id = None
    for reg in registrations_list:
        if (
            "trigger" in reg.RegistrationData.args
            and str_if_bytes(reg.RegistrationData.args["trigger"]) == trigger_name
        ):
            exec_id = reg.id
    assert exec_id
    # TODO: Also test other "ExecId"-like objects
    assert rg.gears.unregister(exec_id)
    registrations_list_2 = rg.gears.dumpregistrations()
    assert registrations_list_2 == []


@pytest.mark.parametrize("fun_str", ["GB().run()"])
def test_getexecution(rg: redgrease.RedisGears, fun_str: str):
    # TODO: Test cluster Mode more properly

    exec = rg.gears.pyexecute(fun_str, unblocking=True)
    assert exec
    assert isinstance(exec, redgrease.data.ExecutionResult)
    assert exec.value
    assert not exec.errors
    assert isinstance(exec.value, redgrease.data.ExecID)
    shard_id = exec.value.shard_id

    # ! Possibly a race condition that execution is not complete. Ugly AF sln.
    time.sleep(5)

    res = rg.gears.getexecution(exec)
    assert res
    assert isinstance(res, dict)
    assert shard_id in res.keys()
    exe_plan = res[exec.shard_id]  # TODO: Awkward syntax?
    assert exe_plan
    assert isinstance(exe_plan, redgrease.data.ExecutionPlan)
    assert exe_plan.status
    assert isinstance(exe_plan.status, redgrease.data.ExecutionStatus)
    assert isinstance(exe_plan.shards_received, int)
    assert isinstance(exe_plan.shards_completed, int)
    assert isinstance(exe_plan.results, int)
    assert isinstance(exe_plan.errors, list)
    assert isinstance(exe_plan.total_duration, int)
    assert isinstance(exe_plan.read_duration, int)
    assert isinstance(exe_plan.steps, list)
    assert exe_plan.steps
    for exe_step in exe_plan.steps:
        assert exe_step
        assert isinstance(exe_step, redgrease.data.ExecutionStep)
        assert exe_step.type
        assert isinstance(exe_step.type, str)
        assert isinstance(exe_step.duration, int)
        assert exe_step.name
        assert isinstance(exe_step.name, str)
        assert isinstance(exe_step.arg, str)


def test_getresults(rg: redgrease.RedisGears):
    rg.set("AKEY", 42)
    rg.set("ANOTHERKEY", 1)

    fun_str = """GB().count().run()"""

    exec = rg.gears.pyexecute(fun_str, unblocking=True)
    assert exec is not None
    assert isinstance(exec, redgrease.data.ExecutionResult)
    assert exec.value
    assert not exec.errors
    assert isinstance(exec.value, redgrease.data.ExecID)

    # ! Possibly a race condition that execution is not complete. Ugly AF sln.
    time.sleep(5)

    res = rg.gears.getresults(exec)
    assert res
    print(f"test_get_results res: {repr(res)}")
    assert int(res) == 2


def test_getresultsblocking(rg: redgrease.RedisGears):
    fun_str = """
import time

def long_running():
    for x in range(3):
        yield x
        time.sleep(1)

def sum(a, b):
    return a+b

GB("PythonReader").aggregate(0,sum,sum).run(long_running)
"""
    exe_id = rg.gears.pyexecute(fun_str, unblocking=True)
    assert exe_id
    assert isinstance(exe_id.value, redgrease.data.ExecID)
    res = rg.gears.getresultsblocking(exe_id)
    assert res
    assert isinstance(res, redgrease.data.ExecutionResult)
    assert res.value == b"3"
    assert not res.errors


def test_dumpexecutions(rg: redgrease.RedisGears):
    fun_str = """
import time

def long_running():
    for x in range(60):
        yield str(x)
        time.sleep(1)

GB("PythonReader").foreach(lambda x: log(x)).run(long_running)
"""
    exe_id = rg.gears.pyexecute(fun_str, unblocking=True)
    assert exe_id
    assert isinstance(exe_id.value, redgrease.data.ExecID)
    time.sleep(1)

    for status in [
        redgrease.data.ExecutionStatus.running,
        redgrease.data.ExecutionStatus.done,
    ]:

        executions = rg.gears.dumpexecutions()
        assert executions
        assert isinstance(executions, list)
        assert len(executions) == 1
        for exe in executions:
            assert isinstance(exe, redgrease.data.ExecutionInfo)
            assert isinstance(exe.executionId, redgrease.data.ExecID)
            assert isinstance(exe.status, redgrease.data.ExecutionStatus)
            assert exe.status == status
            assert isinstance(exe.registered, bool)
            assert not exe.registered
            # For some reason it is ok to abort an already aborted execution
            assert rg.gears.abortexecution(exe)


def test_dropexecution(rg: redgrease.RedisGears):
    fun_str = """
import time

def long_running():
    for x in range(60):
        yield str(x)
        time.sleep(1)

GB("PythonReader").run(long_running)
"""
    exe_id = rg.gears.pyexecute(fun_str, unblocking=True)
    assert exe_id
    assert isinstance(exe_id.value, redgrease.data.ExecID)
    time.sleep(1)

    executions = rg.gears.dumpexecutions()
    assert executions
    assert isinstance(executions, list)
    assert len(executions) == 1
    exe = executions[0]

    assert exe.status == redgrease.data.ExecutionStatus.running

    assert rg.gears.abortexecution(exe)

    assert rg.gears.dumpexecutions()[0].status == redgrease.data.ExecutionStatus.done

    assert rg.gears.dropexecution(exe)

    assert len(rg.gears.dumpexecutions()) == 0


def test_abortexecution(rg: redgrease.RedisGears):
    fun_str = """
import time
import redgrease

def long_running():
    for x in range(60):
        yield str(x)
        time.sleep(1)

GB("PythonReader").foreach(lambda x: log(x)).run(long_running)
"""
    exe_id = rg.gears.pyexecute(fun_str, unblocking=True)
    assert exe_id
    assert isinstance(exe_id.value, redgrease.data.ExecID)
    assert rg.gears.abortexecution(exe_id)


def test_pystats(rg: redgrease.RedisGears):
    stats = rg.gears.pystats()
    assert stats
    assert isinstance(stats, redgrease.data.PyStats)

    assert stats.TotalAllocated
    assert isinstance(stats.TotalAllocated, int)
    assert stats.TotalAllocated > 0

    assert stats.PeakAllocated
    assert isinstance(stats.PeakAllocated, int)
    assert stats.PeakAllocated > 0

    assert stats.CurrAllocated
    assert isinstance(stats.CurrAllocated, int)
    assert stats.CurrAllocated > 0


# TODO: Actually test on a cluster setup
@pytest.mark.parametrize("cluster_mode", [False])
def test_infocluster(rg: redgrease.RedisGears, cluster_mode):
    info = rg.gears.infocluster()
    # Non-c
    if not cluster_mode:
        assert info is None
        return

    assert info


# TODO: Actually test on a cluster setup
def test_refreshcluster(rg: redgrease.RedisGears):
    # Pretty pointless test, but anyway
    # TODO: Somehow validate that it is run... Unsure of how though
    # TODO: See issue #11
    assert rg.gears.refreshcluster()
