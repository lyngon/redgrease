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

# import warnings
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Tuple, Union

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
    to_dict,
    to_redis_type,
)

log = logging.getLogger(__name__)

ExecutionID = Union[bytes, str, redgrease.data.ExecID, redgrease.data.ExecutionInfo]
"""Type alias for valid execution identifiers"""

RegistrationID = Union[bytes, str, redgrease.data.ExecID, redgrease.data.Registration]
"""Type alias for valid registration identifiers"""


get_python_version = """
import sys

GearsBuilder("ShardsIDReader").map(lambda shard: tuple(sys.version_info)).run()
"""

get_gears_version = """
import redgrease

GearsBuilder("ShardsIDReader").map(lambda shard: redgrease.GEARS_RUNTIME or ()).run()
"""


class Gears:
    """Client class for Redis Gears commands.

    Attributes:
        redis (redis.Redis):
            Redis client / connection, used for the underlying communication with
            the server.

        config (redgrease.config.Config):
            Redis Gears Configuration 'client'
    """

    _RESPONSE_CALLBACKS: Dict[str, Callable] = {
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

    # NODES_FLAGS - (cluster only)
    # States how target node is selected for cluster commands:
    # - blocked : command is not allowed - Raises a RedisClusterException
    # - random : executed on one randomly selected node
    # - all-masters : executed on all master node
    # - all-nodes : executed on all nodes
    # - slot-id : executed on the node defined by the second argument
    _NODES_FLAGS = {
        "RG.INFOCLUSTER": "random",
        "RG.PYSTATS": "all-nodes",
        "RG.PYDUMPREQS": "random",
        "RG.REFRESHCLUSTER": "all-nodes",
        "RG.DUMPEXECUTIONS": "random",
        "RG.DUMPREGISTRATIONS": "random",
    }

    # RESULT_CALLBACKS - (cluster only)
    # Not to be confused with redis.Redis.RESPONSE_CALLBACKS
    # RESULT_CALLBACKS is special to rediscluster.RedisCluster.
    # It decides how results of commands defined in `NODES_FLAGS` are aggregated into
    # a final response, **after** redis.Redis.RESPONSE_CALLBACKS as been applied to
    # each response individually.
    _RESULT_CALLBACKS = {
        "RG.INFOCLUSTER": lambda _, res: next(iter(res.values())),
        "RG.PYSTATS": lambda _, res: res,
        "RG.PYDUMPREQS": lambda _, res: next(iter(res.values())),
        "RG.REFRESHCLUSTER": lambda _, res: all(res.values()),
    }

    def __init__(self, redis: redis.Redis):
        """Instantiate a Gears client object

        Args:
            redis (redis.Redis):
                redis.Redis client object, used for the underlying communication with
                the server.
        """
        for command, callback in Gears._RESPONSE_CALLBACKS.items():
            redis.set_response_callback(command, callback)

        # Node Flags is only set on `rediscluster.RedisCluster`
        node_flags: Dict = getattr(redis, "nodes_flags", dict())
        if node_flags:
            node_flags.update(Gears._NODES_FLAGS)
            setattr(redis, "node_flags", node_flags)

        # Result Callbacks is only set on `rediscluster.RedisCluster`
        result_callbacks: Dict = getattr(redis, "result_callbacks", dict())
        if result_callbacks:
            result_callbacks.update(Gears._RESULT_CALLBACKS),
            setattr(redis, "result_callbacks", result_callbacks)

        self._redis = redis
        self.config = redgrease.config.Config(redis)

        self._python_version: Optional[Tuple] = None
        self._gears_version: Optional[Tuple] = None

    def _ping(self) -> bool:
        """Test server liveness/connectivity

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

        return self._redis.execute_command("RG.ABORTEXECUTION", to_redis_type(id))

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
        return self._redis.execute_command("RG.DROPEXECUTION", to_redis_type(id))

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
        executions = self._redis.execute_command("RG.DUMPEXECUTIONS")

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
        registrations = self._redis.execute_command("RG.DUMPREGISTRATIONS")

        if reader or desc or mode or key or stream or trigger:
            filtered_registrations = []
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

                filtered_registrations.append(reg)

            registrations = filtered_registrations

        return registrations

    def getexecution(
        self,
        id: ExecutionID,
        locality: Optional[redgrease.data.ExecLocality] = None,
    ) -> Mapping[bytes, redgrease.data.ExecutionPlan]:
        """Get the execution plan details for a function in the execution list.

        Args:
            id (Union[redgrease.data.ExecutionInfo, redgrease.data.ExecID, bytes, str]):
                Execution identifier for the function to fetch execution plan for.

            locality (Optional[redgrease.data.ExecLocality], optional):
                Set to 'Shard' to get only local execution plan and set to 'Cluster'
                to collect executions from all shards.
                Defaults to 'Shard' in stand-alone mode, but "Cluster" in cluster mode.

        Returns:
            Mapping[bytes, redgrease.data.ExecutionPlan]:
                A dict, mapping cluster ID to ExecutionPlan
        """
        if isinstance(id, redgrease.data.ExecutionInfo):
            id = id.executionId

        loc = [] if locality is None else [safe_str(locality).upper()]

        return self._redis.execute_command("RG.GETEXECUTION", to_redis_type(id), *loc)

    def getresults(
        self,
        id: ExecutionID,
    ) -> redgrease.data.ExecutionResult:
        """Get the results of a function in the execution list.

        Args:
            id (Union[redgrease.data.ExecutionInfo, redgrease.data.ExecID, bytes, str]):
                Execution identifier for the function to fetch the output for.

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

        return self._redis.execute_command("RG.GETRESULTS", to_redis_type(id))

    def getresultsblocking(self, id: ExecutionID) -> redgrease.data.ExecutionResult:
        """Get the results and errors from the execution details of a function.
        If the execution is not finished, the call is blocked until execution
        ends.

        Args:
            id (Union[redgrease.data.ExecutionInfo, redgrease.data.ExecID, bytes, str]):
                Execution identifier for the function to fetch the results and errors
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

        return self._redis.execute_command("RG.GETRESULTSBLOCKING", to_redis_type(id))

    def infocluster(self) -> redgrease.data.ClusterInfo:
        """Gets information about the cluster and its shards.

        Returns:
            redgrease.data.ClusterInfo:
                Cluster information or None if not in cluster mode.
        """
        return self._redis.execute_command("RG.INFOCLUSTER")

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
                Function to execute. Either:

                - A :ref:`raw function string <exe_gear_function_str>` containing a clear-text serialized Gears Python function as per the `examples in the official documentation <https://oss.redislabs.com/redisgears/intro.html#the-simplest-example>`_.

                - A :ref:`script file path <exe_gear_function_file>`.

                - A :ref:`GearFunction object <exe_gear_function_obj>`, e.g. :class:`.GearsBuilder` or either of the :ref:`gearfun_readers` types.

                .. note::

                    * Python version must match the Gear runtime.

                    * If the function is not "closed" with a :meth:`run <.OpenGearFunction.run>` or :meth:`register <.OpenGearFunction.register>` operation, an ``run()`` operation without additional arguments will be assumed, and automatically added to the function to close it.

                    * The default for ``enforce_redgrease`` is ``True``.

                Defaults to ``""``, i.e. no function.

            unblocking (bool, optional):
                Execute function without waiting for it to finish before returning.

                Defaults to ``False``. I.e. block until the function returns or fails.

            requirements (Iterable[Union[None, str, redgrease.requirements.Requirement]], optional):
                List of 3rd party package requirements needed to execute the function
                on the server.

                Defaults to ``None``.

            enforce_redgrease (redgrease.requirements.PackageOption, optional):
                Indicates if redgrease runtime package requirement should be added or not, and potentially which version and/or extras or source.

                It can take several optional types:

                    - ``None`` :  No enforcement. Requirements are passed through, with or without 'redgrease' runtime package.

                    - ``True`` : Enforces latest ``"redgrease[runtime]"`` package on PyPi,

                    - ``False`` : Enforces that *redgrease* is **not** in the requirements list, any *redgrease* requirements will be removed from the function's requirements. Note that it will **not** force *redgrease* to be uninstalled from the server runtime.

                    - Otherwise, the argument's string-representation is evaluated, and interpreted as either:

                        a. A specific version. E.g. ``"1.2.3"``.

                        b. A version qualifier. E.g. ``">=1.0.0"``.

                        c. Extras. E.g. ``"all"`` or ``"runtime"``. Will enforce the latest version on PyPi, with this/these extras.

                        d. Full requirement qualifier or source. E.g: ``"redgrease[all]>=1.2.3"`` or  ``"redgrease[runtime]@git+https://github.com/lyngon/redgrease.git@main"``

                Defaults to ``None`` when ``gear_function`` is a :ref:`script file path <exe_gear_function_file>` or a :ref:`raw function string <exe_gear_function_str>`, but ``True`` when it is a :ref:`GearFunction object <exe_gear_function_obj>`.

        Returns:
            redgrease.data.ExecutionResult:

                The returned :class:`ExecutionResult <.data.ExecutionResult>` has two properties: ``value`` and ``errors``, containing the result value and any potential errors, respectively.

                The value contains the result of the function, unless:

                - When used in 'unblocking' mode, the value is set to the execution ID

                - If the function has no output (i.e. it is closed by `register()` or for some other  reason is not closed by a `run()` action), the value is True (boolean).

                Any errors generated by the function are accumulated in the ``errors`` property, as a list.

                .. note::

                    The :class:`ExecutionResult <redgrease.data.ExecutionResult>` object itself behaves *for the most part* like its contained ``value``, so for many simple operations, such as checking `True-ness`, result length, iterate results etc, it can be used directly. But the the safest was to get the actual result is by means of the ``value`` property.

        Raises:
            redis.exceptions.ResponseError:
                If the function cannot be parsed.
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
            command_response = self._redis.execute_command(
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
        return self._redis.execute_command("RG.PYSTATS")

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
        requirements = self._redis.execute_command("RG.PYDUMPREQS")

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
        return self._redis.execute_command("RG.REFRESHCLUSTER")

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
        return self._redis.execute_command("RG.TRIGGER", safe_str(trigger_name), *args)

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

        return self._redis.execute_command("RG.UNREGISTER", to_redis_type(id))

    def python_version(self) -> Optional[Tuple]:
        if self._python_version is None:
            ver = self.pyexecute(get_python_version)
            if isinstance(ver, list):
                ver = ver[0]
            self._python_version = ast.literal_eval(safe_str(ver))

        return self._python_version

    def gears_version(self) -> Optional[Tuple]:
        if self._gears_version is None:

            module_list = [
                to_dict(
                    mod,
                    key_transform=safe_str,
                    val_transform=safe_str,
                )
                for mod in self._redis.execute_command("MODULE", "LIST")
            ]

            for module_info in module_list:
                if module_info.get("name", None) == "rg":
                    try:
                        ver_int = int(module_info["ver"])
                        major, minor_build = divmod(ver_int, 10000)
                        minor, build = divmod(minor_build, 100)
                        self._gears_version = (major, minor, build)

                    except (KeyError, ValueError):
                        pass

            # ver = self.pyexecute(get_gears_version, enforce_redgrease=True)
            # if isinstance(ver, list):
            #     ver = ver[0]
            # self._gears_version = ast.literal_eval(safe_str(ver))

        return self._gears_version


class RedisGearsModule:
    def __init__(self, **kwargs) -> None:
        self.connection = None
        self._gears = None

    @property
    def gears(self):
        """Gears client, exposing gears commands

        Returns:
            Gears:
                Gears client
        """
        if not self._gears:
            self._gears = Gears(self)

        return self._gears


class RedisModules(RedisGearsModule):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def module_list(self):
        return [
            to_dict(
                mod,
                key_transform=safe_str,
                val_transform=safe_str,
            )
            for mod in self.execute_command("MODULE", "LIST")
        ]


class Redis(redis.Redis, RedisModules):
    def __init__(
        self,
        host="localhost",
        port=6379,
        db=0,
        password=None,
        socket_timeout=None,
        socket_connect_timeout=None,
        socket_keepalive=None,
        socket_keepalive_options=None,
        connection_pool=None,
        unix_socket_path=None,
        encoding="utf-8",
        encoding_errors="strict",
        charset=None,
        errors=None,
        decode_responses=False,
        retry_on_timeout=False,
        ssl=False,
        ssl_keyfile=None,
        ssl_certfile=None,
        ssl_cert_reqs="required",
        ssl_ca_certs=None,
        ssl_check_hostname=False,
        max_connections=None,
        single_connection_client=False,
        health_check_interval=0,
        client_name=None,
        username=None,
        **kwargs,
    ):
        redis.Redis.__init__(
            self,
            host=host,
            port=port,
            db=db,
            password=password,
            socket_timeout=socket_timeout,
            socket_connect_timeout=socket_connect_timeout,
            socket_keepalive=socket_keepalive,
            socket_keepalive_options=socket_keepalive_options,
            connection_pool=connection_pool,
            unix_socket_path=unix_socket_path,
            encoding=encoding,
            encoding_errors=encoding_errors,
            charset=charset,
            errors=errors,
            decode_responses=decode_responses,
            retry_on_timeout=retry_on_timeout,
            ssl=ssl,
            ssl_keyfile=ssl_keyfile,
            ssl_certfile=ssl_certfile,
            ssl_cert_reqs=ssl_cert_reqs,
            ssl_ca_certs=ssl_ca_certs,
            ssl_check_hostname=ssl_check_hostname,
            max_connections=max_connections,
            single_connection_client=single_connection_client,
            health_check_interval=health_check_interval,
            client_name=client_name,
            username=username,
            **kwargs,
        )
        RedisModules.__init__(self)


try:
    # If the `rediscluster` package is present, then
    # define `RedisCluster` as `rediscluster.RedisCluster` with `RedisModules`

    import rediscluster
    import rediscluster.exceptions

    class RedisCluster(rediscluster.RedisCluster, RedisModules):
        def __init__(
            self,
            host=None,
            port=None,
            startup_nodes=None,
            max_connections=None,
            max_connections_per_node=False,
            init_slot_cache=True,
            readonly_mode=False,
            reinitialize_steps=None,
            skip_full_coverage_check=False,
            nodemanager_follow_cluster=False,
            connection_class=None,
            read_from_replicas=False,
            cluster_down_retry_attempts=3,
            host_port_remap=None,
            **kwargs,
        ):
            self.connection = None
            rediscluster.RedisCluster.__init__(
                self,
                host=host,
                port=port,
                startup_nodes=startup_nodes,
                max_connections=max_connections,
                max_connections_per_node=max_connections_per_node,
                init_slot_cache=init_slot_cache,
                readonly_mode=readonly_mode,
                reinitialize_steps=reinitialize_steps,
                skip_full_coverage_check=skip_full_coverage_check,
                nodemanager_follow_cluster=nodemanager_follow_cluster,
                connection_class=connection_class,
                read_from_replicas=read_from_replicas,
                cluster_down_retry_attempts=cluster_down_retry_attempts,
                host_port_remap=host_port_remap,
                **kwargs,
            )
            RedisModules.__init__(self)


except ModuleNotFoundError as mnf_err:

    # If the `rediscluster` package is NOT present, then
    # define `RedisCluster` as a class that throws a `ModuleNotFoundError`.
    ex = mnf_err

    class RedisCluster:  # type: ignore
        def __init__(self, *args, **kwargs) -> None:
            raise ex


# Redis with Modules
def RedisMods(
    host=None,
    port=None,
    db=None,
    password=None,
    socket_timeout=None,
    socket_connect_timeout=None,
    socket_keepalive=None,
    socket_keepalive_options=None,
    connection_pool=None,
    unix_socket_path=None,
    encoding=None,
    encoding_errors=None,
    charset=None,
    errors=None,
    decode_responses=None,
    retry_on_timeout=None,
    ssl=None,
    ssl_keyfile=None,
    ssl_certfile=None,
    ssl_cert_reqs=None,
    ssl_ca_certs=None,
    ssl_check_hostname=None,
    max_connections=None,
    single_connection_client=None,
    health_check_interval=None,
    client_name=None,
    username=None,
    # Redis Cluster
    startup_nodes=None,
    max_connections_per_node=None,
    init_slot_cache=None,
    readonly_mode=None,
    reinitialize_steps=None,
    skip_full_coverage_check=None,
    nodemanager_follow_cluster=None,
    connection_class=None,
    read_from_replicas=None,
    cluster_down_retry_attempts=None,
    host_port_remap=None,
    # Catchall
    **kwargs,
) -> Redis:

    default_args = {
        "db": db,
        "password": password,
        "socket_timeout": socket_timeout,
        "socket_connect_timeout": socket_connect_timeout,
        "socket_keepalive": socket_keepalive,
        "socket_keepalive_options": socket_keepalive_options,
        "connection_pool": connection_pool,
        "unix_socket_path": unix_socket_path,
        "encoding": encoding,
        "encoding_errors": encoding_errors,
        "charset": charset,
        "errors": errors,
        "decode_responses": decode_responses,
        "retry_on_timeout": retry_on_timeout,
        "ssl": ssl,
        "ssl_keyfile": ssl_keyfile,
        "ssl_certfile": ssl_certfile,
        "ssl_cert_reqs": ssl_cert_reqs,
        "ssl_ca_certs": ssl_ca_certs,
        "ssl_check_hostname": ssl_check_hostname,
        "max_connections": max_connections,
        "single_connection_client": single_connection_client,
        "health_check_interval": health_check_interval,
        "client_name": client_name,
        "username": username,
    }

    cluster_args = {
        "startup_nodes": startup_nodes,
        "max_connections_per_node": max_connections_per_node,
        "init_slot_cache": init_slot_cache,
        "readonly_mode": readonly_mode,
        "reinitialize_steps": reinitialize_steps,
        "skip_full_coverage_check": skip_full_coverage_check,
        "nodemanager_follow_cluster": nodemanager_follow_cluster,
        "connection_class": connection_class,
        "read_from_replicas": read_from_replicas,
        "cluster_down_retry_attempts": cluster_down_retry_attempts,
        "host_port_remap": host_port_remap,
    }

    if host is None and startup_nodes is None:
        host = "localhost"

    try:

        return RedisCluster(
            host=host,
            port=port,
            **{
                **{k: v for k, v in cluster_args.items() if v is not None},
                **{k: v for k, v in default_args.items() if v is not None},
                **kwargs,
            },
        )
    except (
        rediscluster.exceptions.RedisClusterException,
        ModuleNotFoundError,
    ):

        if port is None:
            port = 6379

        return Redis(
            host=host,
            port=port,
            **{**{k: v for k, v in default_args.items() if v is not None}, **kwargs},
        )


# Deprecated
def RedisGears(*args, **kwargs):
    """Deprecated
    Use RedisMods instead
    """
    # warnings.warn(
    #     """Instantiation using `RedisGears` will be deprecated.
    #     Please use `RedisMods` instead.""",
    #     DeprecationWarning,
    #     stacklevel=2,
    # )
    return RedisMods(*args, **kwargs)
