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
    def __init__(self, redis):
        self.redis = redis
        self.config = redgrease.config.Config(redis)

    def _ping(self):
        return self.config.MaxExecutions > 0

    def abortexecution(self, id: Union[redgrease.data.ExecID, bytes, str]) -> bool:
        """Abort the execution of a function mid-flight

        Args:
            id (Union[ID, bytes, str]): The execution id to abort

        Returns:
            [bool]: True or an error if the execution does not exist or had
            already finished.
        """
        return self.redis.execute_command("RG.ABORTEXECUTION", id)

    def dropexecution(self, id: Union[redgrease.data.ExecID, bytes, str]) -> bool:
        """
        Remove the execution of a function from the executions list.

        Args:
            id (Union[ID, bytes, str]): Execution ID to remove

        Returns:
            bool: True if successful, or an error if the execution
            does not exist or is still running.
        """
        return self.redis.execute_command("RG.DROPEXECUTION", id)

    def dumpexecutions(self) -> List[redgrease.data.ExecutionInfo]:
        """Get list of function executions.
        The executions list's length is capped by the 'MaxExecutions'
        configuration option.

        Returns:
            List[ExecutionInfo]: A list of ExecutionInfo,
            with an entry per execution.
        """
        return self.redis.execute_command("RG.DUMPEXECUTIONS")

    def dumpregistrations(self) -> List[redgrease.data.Registration]:
        """Get list of function registrations.

        Returns:
            List[Registration]: A list of Registration,
            with one entry per registered function.
        """
        return self.redis.execute_command("RG.DUMPREGISTRATIONS")

    def getexecution(
        self,
        id: Union[redgrease.data.ExecutionInfo, redgrease.data.ExecID, str, bytes],
        locality: Optional[redgrease.data.ExecLocality] = None,
    ) -> Mapping[bytes, redgrease.data.ExecutionPlan]:
        """Get the execution plan details for a function in the execution list.

        Args:
            id (Union[ExecutionInfo, ExecID, str, bytes]):
            Execution identifier for the fuction to fetch execution plan for.
            locality (Optional[ExecLocality], optional): Set to 'Shard' to get
            only local execution pland and set to 'Cluster' to collect
            executions from all shards.
            Defaults to 'Shard' in stand-alone mode,
            but "Cluster" in cluster mode.

        Returns:
            Mapping[bytes, ExecutionPlan]: A dict, mapping cluster ID to
            ExecutionPlan
        """
        if isinstance(id, redgrease.data.ExecutionInfo):
            id = id.executionId

        loc = [] if locality is None else [safe_str(locality).upper()]

        return self.redis.execute_command("RG.GETEXECUTION", to_redis_type(id), *loc)

    def getresults(
        self,
        id: Union[redgrease.data.ExecutionInfo, redgrease.data.ExecID, str, bytes],
    ):
        """Get the results and errors from the execution details of a function
        that's in the execution list.

        Args:
            id (Union[ExecutionInfo, ExecID, str, bytes]):
            Execution identifier for the fuction to fetch the results
            and errors for.

        Returns:
            Tuple: A tuple of results and errors from the gears function.
        """
        if isinstance(id, redgrease.data.ExecutionInfo):
            id = id.executionId
        if isinstance(id, redgrease.data.ExecID):
            id = str(id)

        return self.redis.execute_command("RG.GETRESULTS", id)

    def getresultsblocking(
        self, id: Union[redgrease.data.ExecutionInfo, redgrease.data.ExecID, str, bytes]
    ):
        """Get the results and errors from the execution details of a function.
        If the execution is not finished, the call is blocked until execution
        ends.

        Args:
            id (Union[ExecutionInfo, ExecID, str, bytes]):
            Execution identifier for the fuction to fetch the results
            and errors for.

        Returns:
            Tuple: A tuple of results and errors from the gears function
        """
        if isinstance(id, redgrease.data.ExecutionInfo):
            id = id.executionId

        return self.redis.execute_command("RG.GETRESULTSBLOCKING", id)

    def infocluster(self) -> redgrease.data.ClusterInfo:
        """Gets information about the cluster and its shards.

        Returns:
            ClusterInfo: Cluster information or None if not ion cluster mode.
        """
        return self.redis.execute_command("RG.INFOCLUSTER")

    def pyexecute(
        self,
        gear_function: Union[str, redgrease.gears.ClosedGearFunction] = "",
        unblocking=False,
        requirements: Iterable[Union[str, redgrease.requirements.Requirement]] = None,
        enforce_redgrease: redgrease.requirements.PackageOption = False,
    ):
        """Execute Python code

        Args:
            gear_function (str): Serialized Gears Python function

            unblocking (bool, optional): Execute function without waiting for
            it to finish, before returnining.
            Defaults to False. I.e. block until the function returns or fails.

            requirements (List[str], optional): List of python package
            dependencies that the function requires in order to execute.
            These packages will be download if not already installed.
            Defaults to None.

        Returns:
            An error is returned if the function can't be parsed, as well as
            any that are generated by non-RedisGears functions used.

            When used in UNBLOCKING mode reply is an execution ID .

            Any results and errors generated by the function are returned as
            an array made of two sub-arrays: one for results and the other for
            errors.

            A simple 'OK' string is returned if the function has no output
            (i.e. it doesn't consist of any functions with the run action).
        """
        requirements = set(requirements if requirements else [])

        if isinstance(gear_function, redgrease.gears.GearFunction):
            # If the input is a GearFunction, we get the requirements from it,
            # and ensure that redgrease is included
            requirements = requirements.union(gear_function.requirements)
            enforce_redgrease = True

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
            pickled_results=isinstance(gear_function, redgrease.gears.GearFunction)
        )

    def pystats(self) -> redgrease.data.PyStats:
        """Get memory usage statisticy from the Python interpreter

        Returns:
            PyStats: Python interpretere memory statistics, including total,
            peak and current amount of allocated memory, in bytes.
        """
        return self.redis.execute_command("RG.PYSTATS")

    def pydumpreqs(self) -> List[redgrease.data.PyRequirementInfo]:
        """Gets all the python requirements available (with information about
        each requirement).

        Returns:
            List[PyRequirementInfo]: List of Python requirement information
            objects.
        """
        return self.redis.execute_command("RG.PYDUMPREQS")

    def refreshcluster(self) -> bool:
        """Refreshes the local node's view of the cluster topology.

        Returns:
            bool: True if successful, raises an error othewise
        """
        return self.redis.execute_command("RG.REFRESHCLUSTER")

    def trigger(self, trigger_name: str, *args) -> List[Any]:
        """Trigger the execution of a registered 'CommandReader' function.

        Args:
            trigger_name (str): The registered 'trigger' name of the function

            *args (Any): Any additional arguments to the trigger

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
            bool: True or an error.
            An error is returned if the registration ID doesn't exist or if
            the function's reader doesn't support the unregister operation.
        """
        if isinstance(id, redgrease.data.ExecutionInfo):
            id = id.executionId
        if isinstance(id, redgrease.data.ExecID):
            id = str(id)

        return self.redis.execute_command("RG.UNREGISTER", id)


class Redis(redis.Redis):
    RESPONSE_CALLBACKS = {
        **redis.Redis.RESPONSE_CALLBACKS,
        **{
            "RG.ABORTEXECUTION": bool_ok,
            "RG.CONFIGGET": dict_of(
                CaseInsensitiveDict(redgrease.config.Config.ValueTypes)
            ),
            "RG.CONFIGSET": lambda res: all(map(bool_ok, res)),
            "RG.DROPEXECUTION": bool_ok,
            "RG.DUMPEXECUTIONS": list_parser(redgrease.data.Registration.from_redis),
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
        },
    }

    def __init__(self, *args, **kwargs):
        self._gears = None
        super().__init__(*args, **kwargs)

    @property
    def gears(self) -> Gears:
        if not self._gears:
            self._gears = Gears(self)

        return self._gears


RedisGears = Redis
