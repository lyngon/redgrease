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

import logging
import os.path
from typing import Any, Iterable, List, Mapping, Optional, Union

import redis

import redgrease.config
import redgrease.data
import redgrease.gears
import redgrease.reader
import redgrease.requirements
from redgrease.utils import (
    CaseInsensitiveDict,
    bool_ok,
    dict_of,
    list_parser,
    safe_str,
    to_redis_type,
)

log = logging.getLogger(__name__)


class Gears:
    """Client class for Redis Gears commands.

    Attributes:
        redis (redis.Redis):
            Redis client / connection, used for the underlying communication with
            the server.

        config (redgrease.config.Config):
            Redis Gears Configuration 'client'
    """

    RESPONSE_CALLBACKS = {
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

    def abortexecution(self, id: Union[redgrease.data.ExecID, bytes, str]) -> bool:
        """Abort the execution of a function mid-flight

        Args:
            id (Union[redgrease.data.ExecID, bytes, str]):
                The execution id to abort

        Returns:
            bool:
                True or an error if the execution does not exist or had already
                finished.
        """
        return self.redis.execute_command("RG.ABORTEXECUTION", id)

    def dropexecution(self, id: Union[redgrease.data.ExecID, bytes, str]) -> bool:
        """Remove the execution of a function from the executions list.

        Args:
            id (Union[redgrease.data.ExecID, bytes, str]):
                Execution ID to remove

        Returns:
            bool:
                True if successful, or an error if the execution does not exist or is
                still running.
        """
        return self.redis.execute_command("RG.DROPEXECUTION", id)

    def dumpexecutions(self) -> List[redgrease.data.ExecutionInfo]:
        """Get list of function executions.
        The executions list's length is capped by the 'MaxExecutions' configuration
        option.

        Returns:
            List[redgrease.data.ExecutionInfo]:
                A list of ExecutionInfo, with an entry per execution.
        """
        return self.redis.execute_command("RG.DUMPEXECUTIONS")

    def dumpregistrations(self) -> List[redgrease.data.Registration]:
        """Get list of function registrations.

        Returns:
            List[redgrease.data.Registration]:
                A list of Registration, with one entry per registered function.
        """
        return self.redis.execute_command("RG.DUMPREGISTRATIONS")

    def getexecution(
        self,
        id: Union[redgrease.data.ExecutionInfo, redgrease.data.ExecID, str, bytes],
        locality: Optional[redgrease.data.ExecLocality] = None,
    ) -> Mapping[bytes, redgrease.data.ExecutionPlan]:
        """Get the executoin plan details for a function in the execution list.

        Args:
            id (Union[redgrease.data.ExecutionInfo, redgrease.data.ExecID, str, bytes]):
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
        id: Union[redgrease.data.ExecutionInfo, redgrease.data.ExecID, str, bytes],
    ) -> redgrease.data.ExecutionResult:
        """Get the results of a function in the execution list.

        Args:
            id (Union[redgrease.data.ExecutionInfo, redgrease.data.ExecID, str, bytes]):
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
        if isinstance(id, redgrease.data.ExecID):
            id = str(id)

        return self.redis.execute_command("RG.GETRESULTS", id)

    def getresultsblocking(
        self, id: Union[redgrease.data.ExecutionInfo, redgrease.data.ExecID, str, bytes]
    ) -> redgrease.data.ExecutionResult:
        """Get the results and errors from the execution details of a function.
        If the execution is not finished, the call is blocked until execution
        ends.

        Args:
            id (Union[ExecutionInfo, ExecID, str, bytes]):
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

        return self.redis.execute_command("RG.GETRESULTSBLOCKING", id)

    def infocluster(self) -> redgrease.data.ClusterInfo:
        """Gets information about the cluster and its shards.

        Returns:
            redgredase.data.ClusterInfo:
                Cluster information or None if not ion cluster mode.
        """
        return self.redis.execute_command("RG.INFOCLUSTER")

    def pyexecute(
        self,
        gear_function: Union[str, redgrease.gears.ClosedGearFunction] = "",
        unblocking=False,
        requirements: Optional[
            Iterable[Union[str, redgrease.requirements.Requirement]]
        ] = None,
        enforce_redgrease: redgrease.requirements.PackageOption = False,
    ) -> redgrease.data.ExecutionResult:
        """Execute a gear function.

        Args:
            gear_function (Union[str, redgrease.gears.GearFunction], optional):
                - A serialized Gears Python function as per the official documentation.
                    (https://oss.redislabs.com/redisgears/intro.html#the-simplest-example)

                - A GearFunction object. Notes:
                    * Only Python 3.7 is supported
                    * If the function is not "closed" with a `run()` or `register()`
                    operation, an empty `run()` operation will be assumed, and
                    automatically added to the function to close it.
                    * The default for `enforce_redgrease` is True.

                - A file path to a gear script.

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

                    - None : enforces that 'redgrease' is NOT in thequirements,
                        any redgrease requirements will be removed from the function.

                    - bool :
                        True - enforces latest 'redgrease[runtime] ' package on PyPi,
                        False - no enforcing. Requirements are passed through,
                            with or without 'redgrease'.

                    - str :
                        a. Specific version. E.g. "1.2.3".
                        b. Version qualifier. E.g. ">=1.0.0"
                        c. Extras. E.g. "all" or "runtime".
                            Will enforce the latest version on PyPi, with this/these extras
                        d. Full requirement qualifier or source. E.g:
                            "redgrease[all]>=1.2.3"
                            "redgrease[runtime]@git+https://github.com/lyngon/redgrease.git@some_branch"

                    - Version : behaves just as string version (a.)

                    - Requirment : behaves just as string version (d.)

                efaults to False (for str function), True for GearFunction objects.

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

        if isinstance(gear_function, redgrease.gears.GearFunction):
            # If the input is a GearFunction, we get the requirements from it,
            # and ensure that redgrease is included
            requirements = requirements.union(gear_function.requirements)
            enforce_redgrease = enforce_redgrease or True

            if isinstance(gear_function, redgrease.gears.PartialGearFunction):
                # If the function isn't closed with either 'run' or 'register'
                # we assume it is meant to be closed with a 'run'
                gear_function = gear_function.run()

            function_string = redgrease.data.seralize_gear_function(gear_function)

        elif os.path.exists(gear_function):
            # If the gear function is a fle path,
            # then we load the contents of the file
            with open(gear_function) as script_file:
                function_string = script_file.read()

        else:
            # Otherwise we default to the string version of the function
            function_string = str(gear_function)

        params = []
        if unblocking:
            params.append("UNBLOCKING")

        requirements = redgrease.requirements.resolve_requirements(
            requirements, enforce_redgrease=enforce_redgrease
        )

        if requirements:
            params.append("REQUIREMENTS")
            params += list(map(str, requirements))

        return self.redis.execute_command(
            "RG.PYEXECUTE",
            function_string,
            *params,
            pickled=isinstance(gear_function, redgrease.gears.GearFunction),
        )

    def pystats(self) -> redgrease.data.PyStats:
        """Gets memory usage statisticy from the Python interpreter

        Returns:
            redgrease.data.PyStats:
                Python interpretere memory statistics, including total,
                peak and current amount of allocated memory, in bytes.
        """
        return self.redis.execute_command("RG.PYSTATS")

    def pydumpreqs(self) -> List[redgrease.data.PyRequirementInfo]:
        """Gets all the python requirements available (with information about
        each requirement).

        Returns:
            List[redgrease.data.PyRequirementInfo]:
                List of Python requirement information objects.
        """
        return self.redis.execute_command("RG.PYDUMPREQS")

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

    def unregister(
        self, id: Union[redgrease.data.ExecutionInfo, redgrease.data.ExecID, str, bytes]
    ) -> bool:
        """Removes the registration of a function

        Args:
            id (Union[ExecutionInfo, ExecID, str, bytes]):
                Execution identifier for the function to unregister.

        Returns:
            bool: True if successful

        Raises:
            redis.exceptions.ResponseError:
                If the registration ID doesn't exist or if the function's reader
                doesn't support the unregister operation.
        """
        if isinstance(id, redgrease.data.ExecutionInfo):
            id = id.executionId
        if isinstance(id, redgrease.data.ExecID):
            id = str(id)

        return self.redis.execute_command("RG.UNREGISTER", id)


def geared(cls):
    class GearsClient(cls):

        RESPONSE_CALLBACKS = {
            **redis.Redis.RESPONSE_CALLBACKS,
            **Gears.RESPONSE_CALLBACKS,
        }

        def __init__(self, *args, **kwargs):
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

    return GearsClient


@geared
class Redis(redis.Redis):
    """Redis client class, with support for gears features.

    Behaves exactly like the redis.Redis client, but is extended with a 'gears'
    property fo executiong Gears commands.

    Attributes:
        gears (redgrease.client.Gears):
            Gears command client.
    """

    def __init__(self, *args, **kwargs):
        """Instantiate a redis client, with gears features"""
        super().__init__(*args, **kwargs)
