from redis import Redis
from typing import Optional, Union, List, Iterable, Mapping
import attr
from enum import Enum
import redgrease
import logging

log = logging.getLogger(__name__)


def to_bool(input):
    if isinstance(input, bytes):
        input = input.decode()
    if isinstance(input, str):
        return input.lower() in ["true", "yes", "ok", "1", "y", "ja", "oui"]
    return bool(input)


def to_bytes(input):
    if isinstance(input, bytes):
        return input

    if isinstance(input, str):
        return input.encode()

    if isinstance(input, ExecID):
        return input.id

    if hasattr(input, "__bytes__"):
        input.__bytes__()

    raise ValueError(
        f"Value {input} :: {type(input)} is not a valid '{attr}' as bytes."
    )


def to_str(input):
    return input.decode() if isinstance(input, bytes) else str(input)


def to_list(mapping: dict):
    return list([item for kwpair in mapping.items() for item in kwpair])


def to_dict(
    items: Iterable,
    keyname: str = None,
    keytype=lambda x: x,
    valuename: str = None,
    valuetype=lambda x: x,
):
    kwargs = {}
    iterator = iter(items)
    key_is_set = False
    value_is_set = False
    for item in iterator:
        if not key_is_set:
            if keyname is None:
                key = keytype(item)
                key_is_set = True
            else:
                if to_str(item) == to_str(keyname):
                    item = next(iterator)
                    key = keytype(item)
                    key_is_set = True
        elif not value_is_set:
            if valuename is None:
                value = valuetype(item)
                value_is_set = True
            else:
                if to_str(item) == to_str(valuename):
                    item = next(iterator)
                    value = valuetype(item)
                    value_is_set = True

        if key_is_set and value_is_set:
            kwargs[key] = value
            key_is_set, value_is_set = False, False

    return kwargs


def to_kwargs(items):
    return to_dict(items, keytype=to_str)


def list_parser(item_parser):
    def parser(input_list):
        return list(map(item_parser, input_list))

    return parser


def ok(command_result):
    return True if command_result.startswith(b"OK") else False


@attr.s(frozen=True, auto_attribs=True)
class ExecID:
    shard_id: str = "0000000000000000000000000000000000000000"
    sequence: int = 0

    def __str__(self):
        return f"{self.shard_id}-{self.sequence}"

    def __bytes__(self):
        return str(self).encode()

    def __repr__(self):
        class_name = self.__class__.__name__
        return (
            f"{class_name}(" f"shard_id={self.shard_id}," f"sequence={self.sequence})"
        )

    @staticmethod
    def parse(value):
        if isinstance(value, bytes):
            value = value.decode()

        values = value.split("-")

        shard_id = values[0]
        sequence = int(values[1])

        return ExecID(shard_id=shard_id, sequence=sequence)


class REnum(Enum):
    def __str__(self):
        return str(self.value)

    def __bytes__(self):
        return bytes(self.value)

    def __repr__(self):
        return f"{self.__class__.__name__}({self.value})"


class ExecutionStatus(REnum):
    created = b"created"
    running = b"running"
    done = b"done"
    aborted = b"aborted"
    pending_cluster = b"pending_cluster"
    pending_run = b"pending_run"
    pending_receive = b"pending_receive"
    pending_termination = b"pending_termination"


class ExecLocality(REnum):
    Shard = "Shard"
    Cluster = "Cluster"


class RedisObject:
    @classmethod
    def from_redis(cls, params):
        return cls(**to_kwargs(params))


# @dataclass
@attr.s(auto_attribs=True, frozen=True)
class ExecutionInfo(RedisObject):
    executionId: ExecID = attr.ib(converter=ExecID.parse)
    status: ExecutionStatus = attr.ib(converter=ExecutionStatus)
    registered: bool


# @dataclass
@attr.s(auto_attribs=True, frozen=True)
class RegData(RedisObject):
    mode: redgrease.TriggerMode = attr.ib()
    numTriggered: int
    numSuccess: int
    numFailures: int
    numAborted: int
    lastError: str
    args: dict = attr.ib(converter=to_kwargs)


# @dataclass
@attr.s(auto_attribs=True, frozen=True)
class Registration(RedisObject):
    id: ExecID = attr.ib(converter=ExecID.parse)
    reader: redgrease.Reader
    desc: str
    RegistrationData: RegData = attr.ib(converter=RegData.from_redis)
    PD: dict


@attr.s(auto_attribs=True, frozen=True)
class ExecutionStep(RedisObject):
    type: str = attr.ib(converter=to_str)
    duration: int
    name: str = attr.ib(converter=to_str)
    arg: str = attr.ib(converter=to_str)


@attr.s(auto_attribs=True, frozen=True)
class ExecutionPlan(RedisObject):
    status: ExecutionStatus  # = attr.ib(converter=ExecutionStatus)
    shards_received: int
    shards_completed: int
    results: int
    errors: int
    total_duration: int
    read_duration: int
    steps: List[ExecutionStep] = attr.ib(
        converter=list_parser(ExecutionStep.from_redis)
    )

    @classmethod
    def parse(res):
        executions = map(
            lambda x: to_dict(
                x,
                keyname="shard_id",
                valuename="execution_plan",
                valuetype=ExecutionPlan.from_redis,
            ),
            res,
        )
        return {k: v for d in executions for k, v in d.items()}


@attr.s(auto_attribs=True, frozen=True)
class ShardInfo(RedisObject):
    id: str = attr.ib(converter=to_str)
    ip: str = attr.ib(converter=to_str)
    port: int
    unixSocket: str = attr.ib(converter=to_str)
    runid: str = attr.ib(converter=to_str)
    minHslot: int
    maxHslot: int


@attr.s(auto_attribs=True, frozen=True)
class ClusterInfo(RedisObject):
    my_id: str
    shards: List[ShardInfo] = attr.ib(converter=list_parser(ShardInfo.from_redis))

    @classmethod
    def parse(res):
        if res is None or res == b"no cluster mode":
            return None

        cluster_info = ClusterInfo(
            my_id=to_str(res[1]),
            shards=res[2],  # list(map(ShardInfo.from_redis, res[2]))
        )

        return cluster_info


@attr.s(auto_attribs=True, frozen=True)
class PyStats(RedisObject):
    TotalAllocated: int
    PeakAllocated: int
    CurrAllocated: int


@attr.s(auto_attribs=True, frozen=True)
class PyRequirementInfo(RedisObject):
    GearReqVersion: int
    Name: str = attr.ib(converter=to_str)
    IsDownloaded: bool = attr.ib(converter=to_bool)
    IsInstalled: bool = attr.ib(converter=to_bool)
    CompiledOs: str = attr.ib(converter=to_str)
    Wheels: List[str] = attr.ib(
        converter=lambda wheels: to_str(wheels)
        if isinstance(wheels, bytes)
        else [to_str(wheel) for wheel in wheels]
    )


class RedisGears(Redis):
    class ConfigKey(REnum):
        MaxExecutions = b"MaxExecutions"
        MaxExecutionsPerRegistration = b"MaxExecutionsPerRegistration"
        ProfileExecutions = b"ProfileExecutions"
        PythonAttemptTraceback = b"PythonAttemptTraceback"
        DownloadDeps = b"DownloadDeps"
        DependenciesUrl = b"DependenciesUrl"
        DependenciesSha256 = b"DependenciesSha256"
        PythonInstallationDir = b"PythonInstallationDir"
        CreateVenv = b"CreateVenv"
        ExecutionThreads = b"ExecutionThreads"
        ExecutionMaxIdleTime = b"ExecutionMaxIdleTime"
        PythonInstallReqMaxIdleTime = b"PythonInstallReqMaxIdleTime"
        SendMsgRetries = b"SendMsgRetries"

    # TODO: Use callbacks from 'redis.utils'
    # TODO: and/or 'redis.client' where applicable
    RESPONSE_CALLBACKS = {
        **Redis.RESPONSE_CALLBACKS,
        **{
            "RG.ABORTEXECUTION": ok,
            # 'RG.CONFIGGET': None,
            "RG.CONFIGSET": lambda res: all(map(ok, res)),
            "RG.DROPEXECUTION": ok,
            "RG.DUMPEXECUTIONS": lambda res: list(map(ExecutionInfo.from_redis, res)),
            "RG.DUMPREGISTRATIONS": lambda res: list(map(Registration.from_redis, res)),
            "RG.GETEXECUTION": ExecutionPlan.parse,
            # 'RG.GETRESULTS': None,
            # 'RG.GETRESULTSBLOCKING': None,
            "RG.INFOCLUSTER": ClusterInfo.parse,
            "RG.PYEXECUTE": lambda res: "OK" if res == b"OK" else res,
            "RG.PYSTATS": PyStats.from_redis,
            "RG.PYDUMPREQS": lambda res: list(map(PyRequirementInfo.from_redis, res)),
            "RG.REFRESHCLUSTER": to_bool,
            # 'RG.TRIGGER': None,
            "RG.UNREGISTER": to_bool,
        },
    }

    def abortexecution(self, id: Union[ExecID, bytes, str]) -> bool:
        """Abort the execution of a function mid-flight

        Args:
            id (Union[ID, bytes, str]): The execution id to abort

        Returns:
            [bool]: True or an error if the execution does not exist or had
            already finished.
        """
        return self.execute_command("RG.ABORTEXECUTION", id)

    # TODO: Problematic name, as it is very similar to Redis' config_get
    def configget(self, *config_name: Union[ExecID, bytes, str]) -> List:
        """Get the value of one or more built-in configuration or
        a user-defined options.

        Args:
            config_name (Union[ID, bytes, str]): One or more names/key
            of configurations to get

        Returns:
            List of config values
        """
        return self.execute_command("RG.CONFIGGET", *config_name)

    # TODO: Problematic name, as it is very similar to Redis' config_set
    def configset(self, **config_setting) -> bool:
        """Set a value of one ore more built-in configuration or
        a user-defined options.

        Args:
            config_setting: Key-value-pairs of config settings

        Returns:
            bool: True if all was successful, false oterwise
        """
        return self.execute_command("RG.CONFIGSET", *to_list(config_setting))

    def dropexecution(self, id: Union[ExecID, bytes, str]) -> bool:
        """
        Remove the execution of a function from the executions list.

        Args:
            id (Union[ID, bytes, str]): Execution ID to remove

        Returns:
            bool: True if successful, or an error if the execution
            does not exist or is still running.
        """
        return self.execute_command("RG.DROPEXECUTION", id)

    def dumpexecutions(self) -> List[ExecutionInfo]:
        """Get list of function executions.
        The executions list's length is capped by the 'MaxExecutions'
        configuration option.

        Returns:
            List[ExecutionInfo]: A list of ExecutionInfo,
            with an entry per execution.
        """
        return self.execute_command("RG.DUMPEXECUTIONS")

    def dumpregistrations(self) -> List[Registration]:
        """Get list of function registrations.

        Returns:
            List[Registration]: A list of Registration,
            with one entry per registered function.
        """
        return self.execute_command("RG.DUMPREGISTRATIONS")

    def getexecution(
        self,
        id: Union[ExecutionInfo, ExecID, str, bytes],
        locality: Optional[ExecLocality] = None,
    ) -> Mapping[bytes, ExecutionPlan]:
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
        if isinstance(id, ExecutionInfo):
            id = id.executionId

        locality = [] if locality is None else [to_str(locality).upper()]
        return self.execute_command("RG.GETEXECUTION", to_bytes(id), *locality)

    def getresults(
        self,
        id: Union[ExecutionInfo, ExecID, str, bytes],
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
        if isinstance(id, ExecutionInfo):
            id = id.executionId

        return self.execute_command("RG.GETRESULTS", id)

    def getresultsblocking(self, id: Union[ExecutionInfo, ExecID, str, bytes]):
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
        if isinstance(id, ExecutionInfo):
            id = id.executionId

        return self.execute_command("RG.GETRESULTSBLOCKING", id)

    def infocluster(self) -> ClusterInfo:
        """Gets information about the cluster and its shards.

        Returns:
            ClusterInfo: Cluster information or None if not ion cluster mode.
        """
        return self.execute_command("RG.INFOCLUSTER")

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

        return self.execute_command("RG.PYEXECUTE", function_string, *params)

    def pystats(self) -> PyStats:
        """Get memory usage statisticy from the Python interpreter

        Returns:
            PyStats: Python interpretere memory statistics, including total,
            peak and current amount of allocated memory, in bytes.
        """
        return self.execute_command("RG.PYSTATS")

    def pydumpreqs(self) -> List[PyRequirementInfo]:
        """Gets all the python requirements available (with information about
        each requirement).

        Returns:
            List[PyRequirementInfo]: List of Python requirement information
            objects.
        """
        return self.execute_command("RG.PYDUMPREQS")

    def refreshcluster(self) -> bool:
        """Refreshes the local node's view of the cluster topology.

        Returns:
            bool: True if successful, raises an error othewise
        """
        return self.execute_command("RG.REFRESHCLUSTER")

    def trigger(self, trigger_name: str, *args) -> List:
        """Trigger the execution of a registered 'CommandReader' function.

        Args:
            trigger_name (str): The registered 'trigger' name of the function

            *args (Any): Any additional arguments to the trigger

        Returns:
            List: A list of the functions output records.
        """
        return self.execute_command("RG.TRIGGER", to_str(trigger_name), *args)

    def unregister(self, id: Union[ExecutionInfo, ExecID, str, bytes]) -> bool:
        """Removes the registration of a function

        Args:
            id (Union[ExecutionInfo, ExecID, str, bytes]):
            Execution identifier for the function to unregister.

        Returns:
            bool: True or an error.
            An error is returned if the registration ID doesn't exist or if
            the function's reader doesn't support the unregister operation.
        """
        if isinstance(id, ExecutionInfo):
            id = id.executionId

        return self.execute_command("RG.UNREGISTER", id)
