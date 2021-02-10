import logging
from typing import Any, Dict, List, Mapping, Optional, Union

import redis
import redis.client
import redis.exceptions

import redgrease.data
from redgrease.typing import Constructor
from redgrease.utils import (
    CaseInsensitiveDict,
    as_is,
    bool_ok,
    dict_of,
    list_parser,
    safe_str,
    to_list,
    to_redis_type,
)

log = logging.getLogger(__name__)


class Config:
    ValueTypes: Dict[str, Constructor[Any]] = {
        "MaxExecutions": int,
        "MaxExecutionsPerRegistration": int,
        "ProfileExecutions": bool,
        "PythonAttemptTraceback": bool,
        "DownloadDeps": bool,
        "DependenciesUrl": safe_str,
        "DependenciesSha256": safe_str,
        "PythonInstallationDir": safe_str,
        "CreateVenv": bool,
        "ExecutionThreads": int,
        "ExecutionMaxIdleTime": int,
        "PythonInstallReqMaxIdleTime": int,
        "SendMsgRetries": int,
    }

    __slots__ = "redis"

    def __init__(self, redis):
        self.redis = redis

    def get(self, *config_option) -> List[str]:
        """Get the value of one or more built-in configuration or
        a user-defined options.

        Args:
            config_name (Union[ID, bytes, str]): One or more names/key
            of configurations to get

        Returns:
            List of config values
        """
        if config_option:
            conf = [safe_str(c) for c in config_option]
        else:
            conf = list(Config.ValueTypes.keys())

        return self.redis.execute_command("RG.CONFIGGET", *conf, keys=conf)

    def set(self, config_dict=None, **config_setting) -> bool:
        """Set a value of one ore more built-in configuration or
        a user-defined options.

        Args:
            config_setting: Key-value-pairs of config settings

        Returns:
            bool: True if all was successful, false oterwise
        """
        settings = {}
        if config_dict:
            settings.update(config_dict)

        if config_setting:
            settings.update(config_setting)

        return self.redis.execute_command("RG.CONFIGSET", *to_list(settings))

    def get_single(self, key):
        return self.get(key)[key]

    @property
    def MaxExecutions(self):
        return self.get_single("MaxExecutions")

    @MaxExecutions.setter
    def MaxExecutions(self, value):
        return self.set(MaxExecutions=value)

    @property
    def MaxExecutionsPerRegistration(self):
        return self.get_single("MaxExecutionsPerRegistration")

    @MaxExecutionsPerRegistration.setter
    def MaxExecutionsPerRegistration(self, value):
        self.set(MaxExecutionsPerRegistration=value)

    @property
    def ProfileExecutions(self):
        return self.get_single("ProfileExecutions")

    @ProfileExecutions.setter
    def ProfileExecutions(self, value):
        self.set(ProfileExecutions=value)

    @property
    def PythonAttemptTraceback(self):
        return self.get_single("PythonAttemptTraceback")

    @PythonAttemptTraceback.setter
    def PythonAttemptTraceback(self, value):
        self.set(PythonAttemptTraceback=value)

    @property
    def DownloadDeps(self):
        return self.get_single("DownloadDeps")

    # @DownloadDeps.setter
    # def DownloadDeps(self, value):
    #     self.set(DownloadDeps=value)

    @property
    def DependenciesUrl(self):
        return self.get_single("DependenciesUrl")

    # @DependenciesUrl.setter
    # def DependenciesUrl(self, value):
    #     self.set(DependenciesUrl=value)

    @property
    def DependenciesSha256(self):
        return self.get_single("DependenciesSha256")

    # @DependenciesSha256.setter
    # def DependenciesSha256(self, value):
    #     self.set(DependenciesSha256=value)

    @property
    def PythonInstallationDir(self):
        return self.get_single("PythonInstallationDir")

    # @PythonInstallationDir.setter
    # def PythonInstallationDir(self, value):
    #     self.set(PythonInstallationDir=value)

    @property
    def CreateVenv(self):
        return self.get_single("CreateVenv")

    # @CreateVenv.setter
    # def CreateVenv(self, value):
    #     self.set(CreateVenv=value)

    @property
    def ExecutionThreads(self):
        return self.get_single("ExecutionThreads")

    # @ExecutionThreads.setter
    # def ExecutionThreads(self, value):
    #     self.set(ExecutionThreads=value)

    @property
    def ExecutionMaxIdleTime(self):
        return self.get_single("ExecutionMaxIdleTime")

    @ExecutionMaxIdleTime.setter
    def ExecutionMaxIdleTime(self, value):
        self.set(ExecutionMaxIdleTime=value)

    @property
    def PythonInstallReqMaxIdleTime(self):
        return self.get_single("PythonInstallReqMaxIdleTime")

    @PythonInstallReqMaxIdleTime.setter
    def PythonInstallReqMaxIdleTime(self, value):
        self.set(PythonInstallReqMaxIdleTime=value)

    @property
    def SendMsgRetries(self):
        return self.get_single("SendMsgRetries")

    @SendMsgRetries.setter
    def SendMsgRetries(self, value):
        self.set(SendMsgRetries=value)


class Gears:
    def __init__(self, redis):
        self.redis = redis
        self.config = Config(redis)

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
        self, function_string: str, unblocking=False, requirements: List[str] = None
    ):
        """Execute Python code

        Args:
            function_string (str): Serialized Gears Python function

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
        params = []
        if unblocking:
            params.append("UNBLOCKING")

        if requirements is not None:
            params.append("REQUIREMENTS")
            params += requirements

        return self.redis.execute_command("RG.PYEXECUTE", function_string, *params)

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
            "RG.CONFIGGET": dict_of(CaseInsensitiveDict(Config.ValueTypes)),
            "RG.CONFIGSET": lambda res: all(map(bool_ok, res)),
            "RG.DROPEXECUTION": bool_ok,
            "RG.DUMPEXECUTIONS": list_parser(redgrease.data.Registration.from_redis),
            "RG.DUMPREGISTRATIONS": list_parser(redgrease.data.Registration.from_redis),
            "RG.GETEXECUTION": redgrease.data.ExecutionPlan.parse,
            "RG.GETRESULTS": as_is,
            "RG.GETRESULTSBLOCKING": as_is,
            "RG.INFOCLUSTER": redgrease.data.ClusterInfo.parse,
            "RG.PYEXECUTE": redgrease.data.parse_execute_response,
            "RG.PYSTATS": redgrease.data.PyStats.from_redis,
            "RG.PYDUMPREQS": list_parser(redgrease.data.PyRequirementInfo.from_redis),
            "RG.REFRESHCLUSTER": bool_ok,
            "RG.TRIGGER": as_is,
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
