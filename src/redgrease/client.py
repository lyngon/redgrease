import logging
from enum import Enum
from typing import Any, AnyStr, Callable, Dict, Iterable, List, Mapping, Optional, Union

import attr
import redis
import redis.client
import redis.exceptions

import redgrease
from redgrease.typing import Constructor

log = logging.getLogger(__name__)


def as_is(value):
    return value


# Just to le able to use CaseInsensitiveDict without modifications
def iteritems(x):
    return iter(x.items())


# Copied as-is from redis.client, as it is not exported
class CaseInsensitiveDict(dict):
    "Case insensitive dict implementation. Assumes string keys only."

    def __init__(self, data):
        for k, v in iteritems(data):
            self[k.upper()] = v

    def __contains__(self, k):
        return super(CaseInsensitiveDict, self).__contains__(k.upper())

    def __delitem__(self, k):
        super(CaseInsensitiveDict, self).__delitem__(k.upper())

    def __getitem__(self, k):
        return super(CaseInsensitiveDict, self).__getitem__(k.upper())

    def get(self, k, default=None):
        return super(CaseInsensitiveDict, self).get(k.upper(), default)

    def __setitem__(self, k, v):
        super(CaseInsensitiveDict, self).__setitem__(k.upper(), v)

    def update(self, data):
        data = CaseInsensitiveDict(data)
        super(CaseInsensitiveDict, self).update(data)


# Copied as-is from redis.utils, as it is not exported
def str_if_bytes(value):
    return (
        value.decode("utf-8", errors="replace") if isinstance(value, bytes) else value
    )


# Copied as is from redis.utils, as it is not exported
def safe_str(value: Any) -> str:
    return str(str_if_bytes(value))


# Same name as in redis.client but slightly different implementation.
# Should be better for long non-Ok replies
def bool_ok(value):
    if isinstance(value, (bytes, str)):
        return str_if_bytes(value[:2]).upper() == "OK"
    else:
        return False


def safe_str_upper(value: AnyStr) -> str:
    return safe_str(value).upper()


def safe_bool(input) -> bool:
    """You want bool? You get bool!"""
    if isinstance(input, bytes):
        input = str_if_bytes(input)
    if isinstance(input, str):
        return any(
            [
                input.lower().startswith(y)
                for y in ["true", "ok", "1", "y", "yes", "ja", "oui"]
            ]
        )
    return bool(input)


def to_int_if_bool(value):
    return int(value) if isinstance(value, bool) else value


# Should it be renamed safe_bytes, for consistency?
# TODO: where is this used? and why?
def to_bytes(input) -> bytes:
    if isinstance(input, bytes):
        return input

    if isinstance(input, str):
        return input.encode()

    if isinstance(input, ExecID):
        return bytes(input)

    if hasattr(input, "__bytes__"):
        input.__bytes__()

    raise ValueError(
        f"Value {input} :: {type(input)} is not a valid '{attr}' as bytes."
    )


# Can / Should be replaced by safe_string
# def to_str(input: Any) -> str:
#     return input.decode() if isinstance(input, bytes) else str(input)


def to_list(
    mapping: Dict[Any, Any], key_transform=str_if_bytes, val_transform=to_int_if_bool
) -> List[Any]:
    return list(
        [
            item
            for key, value in mapping.items()
            for item in (key_transform(key), val_transform(value))
        ]
    )


# TODO: REMOVE THIS MONSTROSITY ANS USE
# redis.client.pairs_to_dict and
# redis.client.pairs_to_dict_typed instead
# or at least clean it up!!!
def to_dict(
    items: Iterable[Any],
    keyname: str = None,
    keytype=lambda x: x,
    valuename: str = None,
    valuetype=lambda x: x,
) -> Dict[Any, Any]:
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
                if safe_str(item) == safe_str(keyname):
                    item = next(iterator)
                    key = keytype(item)
                    key_is_set = True
        elif not value_is_set:
            if valuename is None:
                value = valuetype(item)
                value_is_set = True
            else:
                if safe_str(item) == safe_str(valuename):
                    item = next(iterator)
                    value = valuetype(item)
                    value_is_set = True
        # I have forgotten why, below is not an else branch...
        # A
        if key_is_set and value_is_set:
            kwargs[key] = value
            key_is_set, value_is_set = False, False

    return kwargs


# TODO: why not use redis.client.pairs_to_dict_with_str_keys ?
def to_kwargs(items) -> Dict[str, Any]:
    # return to_dict(items, keytype=safe_str)
    return redis.client.pairs_to_dict(items, decode_keys=True)


# Should maybe be renamed to list_of or parse_list or sometiong
def list_parser(item_parser) -> Callable[[Iterable[Any]], List[Any]]:
    def parser(input_list):
        return list(map(item_parser, input_list))

    return parser


# is this used anywhere
list_of_str = list_parser(safe_str)


def hetero_list(constructors):
    def parser(results, keys):
        return [
            constructors[key](res) if key in constructors else res
            for res, key in zip(results, keys)
        ]

    return parser


def dict_of(constructors: Dict[str, Constructor[Any]]) -> Callable[..., Dict[str, Any]]:
    def parser(results, keys):
        return dict(
            [
                (key, constructors[key](value) if key in constructors else value)
                for (key, value) in zip(keys, results)
            ]
        )

    return parser


@attr.s(auto_attribs=True, frozen=True)
class Execution:
    results: List
    errors: List


def parse_execute_response(response):
    if bool_ok(response):
        return True
    elif isinstance(response, list) and len(response) == 2:
        return Execution(*response)
    elif isinstance(response, bytes):
        return Execution(ExecID.parse(response))
    else:
        return response


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
    def parse(value: Union[str, bytes]) -> "ExecID":
        if isinstance(value, bytes):
            value = value.decode()

        values = value.split("-")

        shard_id = values[0]
        sequence = int(values[1])

        return ExecID(shard_id=shard_id, sequence=sequence)


class REnum(Enum):
    def __str__(self):
        return safe_str(self.value)

    def __bytes__(self):
        return to_bytes(self.value)

    def __repr__(self):
        return f"{self.__class__.__name__}({self.value})"

    def __eq__(self, other):
        return str(self) == safe_str(other)

    def __hash__(self):
        return hash(str(self))


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
    executionId: ExecID = attr.ib(converter=ExecID.parse)  # type: ignore #7912
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
    args: Dict[str, Any] = attr.ib(converter=to_kwargs)


# @dataclass
@attr.s(auto_attribs=True, frozen=True)
class Registration(RedisObject):
    id: ExecID = attr.ib(converter=ExecID.parse)  # type: ignore #7912
    reader: redgrease.Reader
    desc: str
    RegistrationData: RegData = attr.ib(
        converter=RegData.from_redis  # type: ignore #7912
    )
    PD: Dict[Any, Any]


@attr.s(auto_attribs=True, frozen=True)
class ExecutionStep(RedisObject):
    type: str = attr.ib(converter=safe_str)
    duration: int
    name: str = attr.ib(converter=safe_str)
    arg: str = attr.ib(converter=safe_str)


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
        converter=list_parser(ExecutionStep.from_redis)  # type: ignore #7912
    )

    @classmethod
    def parse(res):
        executions = map(
            # lambda x: to_dict(
            #     x,
            #     keyname="shard_id",
            #     valuename="execution_plan",
            #     valuetype=ExecutionPlan.from_redis,
            # ),
            # TODO: is this the same thing?
            lambda x: redis.client.pairs_to_dict_typed(
                x, type_info={"shard_id": ExecutionPlan.from_redis}
            ),
            res,
        )
        return {k: v for d in executions for k, v in d.items()}


@attr.s(auto_attribs=True, frozen=True)
class ShardInfo(RedisObject):
    id: str = attr.ib(converter=safe_str)
    ip: str = attr.ib(converter=safe_str)
    port: int
    unixSocket: str = attr.ib(converter=safe_str)
    runid: str = attr.ib(converter=safe_str)
    minHslot: int
    maxHslot: int


@attr.s(auto_attribs=True, frozen=True)
class ClusterInfo(RedisObject):
    my_id: str
    shards: List[ShardInfo] = attr.ib(
        converter=list_parser(ShardInfo.from_redis)  # type: ignore #7912
    )

    @classmethod
    def parse(res):
        if not res or res == b"no cluster mode":
            return None

        cluster_info = ClusterInfo(
            my_id=safe_str(res[1]),
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
    Name: str = attr.ib(converter=safe_str)
    IsDownloaded: bool = attr.ib(converter=safe_bool)
    IsInstalled: bool = attr.ib(converter=safe_bool)
    CompiledOs: str = attr.ib(converter=safe_str)
    Wheels: List[str] = attr.ib(
        converter=lambda wheels: safe_str(wheels)  # type: ignore
        if isinstance(wheels, bytes)
        else [safe_str(wheel) for wheel in wheels]
    )


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

    def abortexecution(self, id: Union[ExecID, bytes, str]) -> bool:
        """Abort the execution of a function mid-flight

        Args:
            id (Union[ID, bytes, str]): The execution id to abort

        Returns:
            [bool]: True or an error if the execution does not exist or had
            already finished.
        """
        return self.redis.execute_command("RG.ABORTEXECUTION", id)

    def dropexecution(self, id: Union[ExecID, bytes, str]) -> bool:
        """
        Remove the execution of a function from the executions list.

        Args:
            id (Union[ID, bytes, str]): Execution ID to remove

        Returns:
            bool: True if successful, or an error if the execution
            does not exist or is still running.
        """
        return self.redis.execute_command("RG.DROPEXECUTION", id)

    def dumpexecutions(self) -> List[ExecutionInfo]:
        """Get list of function executions.
        The executions list's length is capped by the 'MaxExecutions'
        configuration option.

        Returns:
            List[ExecutionInfo]: A list of ExecutionInfo,
            with an entry per execution.
        """
        return self.redis.execute_command("RG.DUMPEXECUTIONS")

    def dumpregistrations(self) -> List[Registration]:
        """Get list of function registrations.

        Returns:
            List[Registration]: A list of Registration,
            with one entry per registered function.
        """
        return self.redis.execute_command("RG.DUMPREGISTRATIONS")

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

        loc = [] if locality is None else [safe_str(locality).upper()]
        return self.redis.execute_command("RG.GETEXECUTION", to_bytes(id), *loc)

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

        return self.redis.execute_command("RG.GETRESULTS", id)

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

        return self.redis.execute_command("RG.GETRESULTSBLOCKING", id)

    def infocluster(self) -> ClusterInfo:
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

    def pystats(self) -> PyStats:
        """Get memory usage statisticy from the Python interpreter

        Returns:
            PyStats: Python interpretere memory statistics, including total,
            peak and current amount of allocated memory, in bytes.
        """
        return self.redis.execute_command("RG.PYSTATS")

    def pydumpreqs(self) -> List[PyRequirementInfo]:
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

        return self.redis.execute_command("RG.UNREGISTER", id)


class Redis(redis.Redis):
    RESPONSE_CALLBACKS = {
        **redis.Redis.RESPONSE_CALLBACKS,
        **{
            "RG.ABORTEXECUTION": bool_ok,
            "RG.CONFIGGET": dict_of(CaseInsensitiveDict(Config.ValueTypes)),
            "RG.CONFIGSET": lambda res: all(map(bool_ok, res)),
            "RG.DROPEXECUTION": bool_ok,
            "RG.DUMPEXECUTIONS": list_parser(Registration.from_redis),
            "RG.DUMPREGISTRATIONS": list_parser(Registration.from_redis),
            "RG.GETEXECUTION": ExecutionPlan.parse,
            "RG.GETRESULTS": as_is,
            "RG.GETRESULTSBLOCKING": as_is,
            "RG.INFOCLUSTER": ClusterInfo.parse,
            "RG.PYEXECUTE": parse_execute_response,
            "RG.PYSTATS": PyStats.from_redis,
            "RG.PYDUMPREQS": list_parser(PyRequirementInfo.from_redis),
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
