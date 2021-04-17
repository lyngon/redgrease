# -*- coding: utf-8 -*-
"""
Client library module for Redis Gears servers, exposing the gears-specific commands
Can be instantiated in a few ways:

Examples:
    - As a redis client::

        import redgrease

        r = redgrease.RedisGears()  # Takes same arguments as redis.Redis
        r.gears.pyexecute(...)


    - As a separate Gears object, taking a redis.Redis as parameter::

        import redis
        import redgrease

        r = redis.Redis()
        g = redgrease.Gears(r)
        g.pyexecute(...)

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
import fnmatch
import logging
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Union

import redis

import redgrease.config
import redgrease.data
import redgrease.exceptions
import redgrease.gearialization
import redgrease.gears
import redgrease.reader
import redgrease.requirements
import redgrease.runtime
from redgrease.utils import (
    CaseInsensitiveDict,
    bool_ok,
    dict_of,
    list_parser,
    safe_bool,
    safe_str,
    to_redis_type,
)

log = logging.getLogger(__name__)

ExecutionID = Union[bytes, str, redgrease.data.ExecID, redgrease.data.ExecutionInfo]
"""Type alias for valid execution identifiers"""

RegistrationID = Union[bytes, str, redgrease.data.ExecID, redgrease.data.Registration]
"""Type alias for valid registration identifiers"""


class Gears:
    """Client class for Redis Gears commands.

    Attributes:
        redis (redis.Redis):
            Redis client / connection, used for the underlying communication with
            the server.

        config (redgrease.config.Config):
            Redis Gears Configuration 'client'
    """

    RESPONSE_CALLBACKS: Dict[str, Callable] = {
        "RG.ABORTEXECUTION": bool_ok,
        "RG.CONFIGGET": dict_of(
            CaseInsensitiveDict(redgrease.config.Config.ValueTypes)
        ),
        "RG.CONFIGSET": lambda res: all(map(bool_ok, res)),
        "RG.DROPEXECUTION": bool_ok,
        "RG.DUMPEXECUTIONS": list_parser(redgrease.data.ExecutionInfo.from_redis),
        "RG.DUMPREGISTRATIONS": list_parser(redgrease.data.Registration.from_redis),
        "RG.GETEXECUTION": redgrease.data.ExecutionPlan.parse,
        "RG.GETRESULTS": redgrease.data.parse_execute_response,
        "RG.GETRESULTSBLOCKING": redgrease.data.parse_execute_response,
        "RG.INFOCLUSTER": redgrease.data.ClusterInfo.parse,
        "RG.PYEXECUTE": redgrease.data.parse_execute_response,
        "RG.PYSTATS": redgrease.data.PyStats.from_redis,
        "RG.PYDUMPREQS": list_parser(redgrease.data.PyRequirementInfo.from_redis),
        "RG.REFRESHCLUSTER": bool_ok,
        "RG.TRIGGER": redgrease.data.parse_trigger_response,
        "RG.UNREGISTER": bool_ok,
    }

    def __init__(self, redis: redis.Redis):
        """Instatiate a Gears client objeect

        Args:
            redis (redis.Redis):
                redis.Redis client object, used for the underlying communication with
                the server.
        """
        self.redis = redis
        self.config = redgrease.config.Config(redis)

    def _ping(self) -> bool:
        """Test server liveness/connectivty

        Returns:
            bool:
                True if, and only if, the Redis server is responsive and is running the
                gears module
        """
        return self.config.MaxExecutions > 0

    def _trigger_proxy(self, trigger):
        def trigger_function(*args):
            return self.trigger(trigger, *args)

        return redgrease.data.ExecutionResult(trigger_function)

    def abortexecution(self, id: ExecutionID) -> bool:
        """Abort the execution of a function mid-flight

        Args:
            id (Union[redgrease.data.ExecutionInfo, redgrease.data.ExecID, bytes, str]):
                The execution id to abort

        Returns:
            bool:
                True or an error if the execution does not exist or had already
                finished.
        """
        if isinstance(id, redgrease.data.ExecutionInfo):
            id = id.executionId

        return self.redis.execute_command("RG.ABORTEXECUTION", to_redis_type(id))

    def dropexecution(self, id: ExecutionID) -> bool:
        """Remove the execution of a function from the executions list.

        Args:
            id (Union[redgrease.data.ExecutionInfo, redgrease.data.ExecID, bytes, str]):
                Execution ID to remove

        Returns:
            bool:
                True if successful, or an error if the execution does not exist or is
                still running.
        """
        if isinstance(id, redgrease.data.ExecutionInfo):
            id = id.executionId
        return self.redis.execute_command("RG.DROPEXECUTION", to_redis_type(id))

    def dumpexecutions(
        self,
        status: Union[str, redgrease.data.ExecutionStatus] = None,
        registered: bool = None,
    ) -> List[redgrease.data.ExecutionInfo]:
        """Get list of function executions.
        The executions list's length is capped by the 'MaxExecutions' configuration
        option.

        Args:
            status (Union[str, redgrease.data.ExecutionStatus], optional):
                Only return executions that match this status.
                Either: "created", "running", "done", "aborted", "pending_cluster",
                "pending_run", "pending_receive" or "pending_termination".
                Defaults to None.

            registered (bool, optional):
                If `True`, only return registered executions.
                If `False`, only return non-registered executions.

                Defaults to None.

        Returns:
            List[redgrease.data.ExecutionInfo]:
                A list of ExecutionInfo, with an entry per execution.
        """
        executions: List[redgrease.data.ExecutionInfo] = []
        executions = self.redis.execute_command("RG.DUMPEXECUTIONS")

        if status or registered is not None:
            filtered_executions = []
            for exe in executions:
                if status and safe_str(status) != safe_str(exe.status):
                    continue

                if registered is not None and registered != safe_bool(exe.registered):
                    continue

                filtered_executions.append(exe)

            executions = filtered_executions

        return executions

    def dumpregistrations(
        self,
        reader: str = None,
        desc: str = None,
        mode: str = None,
        key: str = None,
        stream: str = None,
        trigger: str = None,
    ) -> List[redgrease.data.Registration]:
        """Get list of function registrations.

        Args:
            reader (str, optional):
                Only return registrations of this reader type.
                E.g: "StreamReader"
                Defaults to None.

            desc (str, optional):
                Only return registrations, where the description match this pattern.
                E.g: "transaction*log*"
                Defaults to None.

            mode (str, optional):
                Only return registrations, in this mode.
                Either "async", "async_local" or "sync".
                Defaults to None.

            key (str, optional):
                Only return (KeysReader) registrations, where the key pattern match
                this key.
                Defaults to None.

            stream (str, optional):
                Only return (StreamReader) registrations, where the stream pattern
                match this key.
                Defaults to None.

            trigger (str, optional):
                Only return (CommandReader) registrations, where the trigger pattern
                match this key.
                Defaults to None.

        Returns:
            List[redgrease.data.Registration]:
                A list of Registration, with one entry per registered function.
        """
        registrations: List[redgrease.data.Registration] = []
        registrations = self.redis.execute_command("RG.DUMPREGISTRATIONS")

        if reader or desc or mode or key or stream or trigger:
            filtered_regsistrations = []
            for reg in registrations:
                if trigger and (
                    "trigger" not in reg.RegistrationData.args
                    and safe_str(reg.RegistrationData.args["trigger"]) == trigger
                ):
                    continue
                if stream and not fnmatch.fnmatch(
                    stream, safe_str(reg.RegistrationData.args.get("stream", ""))
                ):
                    continue
                if key and fnmatch.fnmatch(
                    key, safe_str(reg.RegistrationData.args.get("regex", ""))
                ):
                    continue
                if reader and reader != reg.reader:
                    continue
                if desc and reg.desc and not fnmatch.fnmatch(safe_str(reg.desc), desc):
                    continue
                if mode and mode != reg.RegistrationData.mode:
                    continue

                filtered_regsistrations.append(reg)

            registrations = filtered_regsistrations

        return registrations

    def getexecution(
        self,
        id: ExecutionID,
        locality: Optional[redgrease.data.ExecLocality] = None,
    ) -> Mapping[bytes, redgrease.data.ExecutionPlan]:
        """Get the executoin plan details for a function in the execution list.

        Args:
            id (Union[redgrease.data.ExecutionInfo, redgrease.data.ExecID, bytes, str]):
                Execution identifier for the function to fetch execution plan for.

            locality (Optional[redgrease.data.ExecLocality], optional):
                Set to 'Shard' to get only local execution pland and set to 'Cluster'
                to collect executions from all shards.
                Defaults to 'Shard' in stand-alone mode, but "Cluster" in cluster mode.

        Returns:
            Mapping[bytes, redgrease.data.ExecutionPlan]:
                A dict, mapping cluster ID to ExecutionPlan
        """
        if isinstance(id, redgrease.data.ExecutionInfo):
            id = id.executionId

        loc = [] if locality is None else [safe_str(locality).upper()]

        return self.redis.execute_command("RG.GETEXECUTION", to_redis_type(id), *loc)

    def getresults(
        self,
        id: ExecutionID,
    ) -> redgrease.data.ExecutionResult:
        """Get the results of a function in the execution list.

        Args:
            id (Union[redgrease.data.ExecutionInfo, redgrease.data.ExecID, bytes, str]):
                Execution identifier for the fuction to fetch the output for.

        Returns:
            redgrease.data.ExecutionResult:
                Results and errors from the gears function, if, and only if, execution
                exists and is completed.

        Raises:
            redis.exceptions.ResponseError:
                If the the execution does not exist or is still running
        """

        if isinstance(id, redgrease.data.ExecutionInfo):
            id = id.executionId

        return self.redis.execute_command("RG.GETRESULTS", to_redis_type(id))

    def getresultsblocking(self, id: ExecutionID) -> redgrease.data.ExecutionResult:
        """Get the results and errors from the execution details of a function.
        If the execution is not finished, the call is blocked until execution
        ends.

        Args:
            id (Union[redgrease.data.ExecutionInfo, redgrease.data.ExecID, bytes, str]):
                Execution identifier for the fuction to fetch the results and errors
                for.

        Returns:
            redgrease.data.ExecutionResult:
                Results and errors from the gears function if the execution exists.

        Raises:
            redis.exceptions.ResponseError:
                If the the execution does not exist.
        """
        if isinstance(id, redgrease.data.ExecutionInfo):
            id = id.executionId

        return self.redis.execute_command("RG.GETRESULTSBLOCKING", to_redis_type(id))

    def infocluster(self) -> redgrease.data.ClusterInfo:
        """Gets information about the cluster and its shards.

        Returns:
            redgredase.data.ClusterInfo:
                Cluster information or None if not in cluster mode.
        """
        return self.redis.execute_command("RG.INFOCLUSTER")

    def pyexecute(
        self,
        gear_function: Union[
            str, redgrease.runtime.GearsBuilder, redgrease.gears.GearFunction
        ] = "",
        unblocking=False,
        requirements: Optional[
            Iterable[Union[str, redgrease.requirements.Requirement]]
        ] = None,
        enforce_redgrease: redgrease.requirements.PackageOption = None,
    ) -> redgrease.data.ExecutionResult:
        """Execute a gear function.

        Args:
            gear_function (Union[str, redgrease.gears.GearFunction], optional):
                - A string containgg a clear-text serialized Gears Python function as
                    per the official documentation.
                    (https://oss.redislabs.com/redisgears/intro.html#the-simplest-example)

                - A GearsBuilder or GearFunction object. Notes:
                    * Python version must match the Gear runtime.
                    * If the function is not "closed" with a `run()` or `register()`
                    operation, an `run()` operation without arguments will be assumed,
                    and automatically added to the function to close it.
                    * The default for `enforce_redgrease` is True.

                - A file path to a gear script. This script can

                Defaults to "" (no function).

            unblocking (bool, optional):
                Execute function without waiting for it to finish, before returnining.
                Defaults to False. I.e. block until the function returns or fails.

            requirements (Iterable[Union[None, str, redgrease.requirements.Requirement]], optional):
                List of 3rd party package requirements needed to execute the function
                on the server.
                Defaults to None.

            enforce_redgrease (redgrease.requirements.PackageOption, optional):
                Indicates if redgrease runtime package requirement should be added or not,
                and potentially which version and/or extras or source.
                It can take several optional types::

                    - None :  no enforcing. Requirements are passed through,
                            with or without 'redgrease'.

                    - bool :
                        True - enforces latest 'redgrease[runtime] ' package on PyPi,
                        False - enforces that 'redgrease' is NOT in the requirements,
                            any redgrease requirements will be removed from the function.
                            Note that it will not force redgrease to be uninstalled.

                    - str :
                        a. Specific version. E.g. "1.2.3".
                        b. Version qualifier. E.g. ">=1.0.0"
                        c. Extras. E.g. "all" or "runtime".
                            Will enforce the latest version on PyPi, with this/these extras
                        d. Full requirement qualifier or source. E.g:
                            "redgrease[all]>=1.2.3"
                            "redgrease[runtime]@git+https://github.com/lyngon/redgrease.git@main"

                    - Version : behaves just as string version (a.)

                    - Requirement : behaves just as string version (d.)

                Defaults to False (for str function), True for GearFunction objects.

        Returns:
            redgrease.data.ExecutionResult:
                The value of the ExecutionResult will contain the result of the function, unless:

                - When used in 'unblocking' mode, the value is set to the execution ID

                - If the function has no output (i.e. it is closed by `register()`
                    or for some other reason is not closed by a `run()` action),
                    the value is True (boolean).

                Any results and errors generated by the function are accumulated in the results'
                'errors' property, as a list.

        Raises:
            redis.exceptions.ResponseError:
                If the funvction cannot be parsed.
        """  # noqa
        requirements = set(requirements if requirements else [])

        function_string, ctx = redgrease.gearialization.get_function_string(
            gear_function
        )

        params = []
        if unblocking:
            params.append("UNBLOCKING")

        # Resolve requirement conflicts, remove duplicates
        requirements = redgrease.requirements.resolve_requirements(
            requirements.union(ctx.get("requirements", set())),
            enforce_redgrease=ctx.get("enforce_redgrease", enforce_redgrease),
        )

        if requirements:
            params.append("REQUIREMENTS")
            params += list(map(str, requirements))

        try:
            command_response = self.redis.execute_command(
                "RG.PYEXECUTE",
                function_string,
                *params,
            )
        except redis.exceptions.ResponseError as ex:
            ex.args = ast.literal_eval(ex.args[0])
            if "trigger already registered" in ex.args[-1]:
                raise redgrease.exceptions.DuplicateTriggerError() from ex
            raise

        if command_response and "trigger" in ctx:
            return self._trigger_proxy(ctx["trigger"])

        return command_response

    def pystats(self) -> redgrease.data.PyStats:
        """Gets memory usage statisticy from the Python interpreter

        Returns:
            redgrease.data.PyStats:
                Python interpretere memory statistics, including total,
                peak and current amount of allocated memory, in bytes.
        """
        return self.redis.execute_command("RG.PYSTATS")

    def pydumpreqs(
        self, name: str = None, is_downloaded: bool = None, is_installed: bool = None
    ) -> List[redgrease.data.PyRequirementInfo]:
        """Gets all the python requirements available (with information about
        each requirement).

        Args:
            name (str, optional):
                Only return packages with this **base name**.
                I.e. it is not filtering on version number, extras etc.
                Defaults to None.

            is_downloaded (bool, optional):
                If `True`, only return requirements that have been downloaded.
                If `False`, only return requirements that have NOT been downloaded.
                Defaults to None.

            is_installed (bool, optional):
                If `True`, only return requirements that have been installed.
                If `False`, only return requirements that have NOT been installed.
                Defaults to None.

        Returns:
            List[redgrease.data.PyRequirementInfo]:
                List of Python requirement information objects.
        """
        requirements: List[redgrease.data.PyRequirementInfo] = []
        requirements = self.redis.execute_command("RG.PYDUMPREQS")

        if name or is_downloaded is not None or is_installed is not None:
            filtered_requirements = []
            for req in requirements:
                if name and not redgrease.requirements.same_name(name, req.Name):
                    continue
                if is_downloaded is not None and is_downloaded != safe_bool(
                    req.IsDownloaded
                ):
                    continue
                if is_installed is not None and is_installed != safe_bool(
                    req.IsInstalled
                ):
                    continue

                filtered_requirements.append(req)

            requirements = filtered_requirements

        return requirements

    def refreshcluster(self) -> bool:
        """Refreshes the local node's view of the cluster topology.

        Returns:
            bool:
                True if successful.

        Raises:
            redis.exceptions.ResponseError:
                If not successful
        """
        return self.redis.execute_command("RG.REFRESHCLUSTER")

    def trigger(self, trigger_name: str, *args) -> List[Any]:
        """Trigger the execution of a registered 'CommandReader' function.

        Args:
            trigger_name (str):
                The registered 'trigger' name of the function

            *args (Any):
                Any additional arguments to the trigger

        Returns:6
            List: A list of the functions output records.
        """
        return self.redis.execute_command("RG.TRIGGER", safe_str(trigger_name), *args)

    def unregister(self, id: RegistrationID) -> bool:
        """Removes the registration of a function

        Args:
            id (Union[redgrease.data.Registration, redgrease.data.ExecID, bytes, str]):
                Execution identifier for the function to unregister.

        Returns:
            bool: True if successful

        Raises:
            redis.exceptions.ResponseError:
                If the registration ID doesn't exist or if the function's reader
                doesn't support the unregister operation.
        """
        if isinstance(id, redgrease.data.Registration):
            id = id.id

        return self.redis.execute_command("RG.UNREGISTER", to_redis_type(id))


class Redis(redis.Redis):
    """Redis client class, with support for gears features.

    Behaves exactly like the redis.Redis client, but is extended with a 'gears'
    property fo executiong Gears commands.

    Attributes:
        gears (redgrease.client.Gears):
            Gears command client.
    """

    RESPONSE_CALLBACKS = {
        **redis.Redis.RESPONSE_CALLBACKS,
        **Gears.RESPONSE_CALLBACKS,
    }

    def __init__(self, *args, **kwargs):
        """Instantiate a redis client, with gears features"""
        self._gears = None
        super().__init__(*args, **kwargs)

    @property
    def gears(self) -> Gears:
        """Gears client, exposing gears commands

        Returns:
            Gears:
                Gears client
        """
        if not self._gears:
            self._gears = Gears(self)

        return self._gears
