import ast
import sys
from typing import Any, Dict, Generic, Iterable, List, Optional, TypeVar, Union

import attr
import cloudpickle
import wrapt

import redgrease.gears
from redgrease.utils import (
    REnum,
    bool_ok,
    list_parser,
    safe_bool,
    safe_str,
    str_if_bytes,
    to_bytes,
    to_dict,
    to_kwargs,
)

T = TypeVar("T")


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


class ExecutionResult(wrapt.ObjectProxy, Generic[T]):
    def __init__(self, value: T, errors: Optional[List] = None):
        wrapt.ObjectProxy.__init__(self, value)
        self._self_errors: Optional[List] = errors

    @property
    def value(self):
        return self.__wrapped__

    @property
    def errors(self):
        return self._self_errors

    def __repr__(self) -> str:
        cls_nm = "ExecutionResult"
        val_typ_nm = self.__class__.__name__
        if self._self_errors:
            err = f", errors={repr(self._self_errors)}"
        else:
            err = ""

        return f"{cls_nm}[{val_typ_nm}]({self.__wrapped__.__repr__()}{err})"

    def __iter__(self):
        if hasattr(self.value, "__iter__"):
            return iter(self.value)
        else:
            return iter([] if self.value is None else [self.value])

    def __len__(self) -> int:
        if hasattr(self.value, "__len__") and not isinstance(self.value, (str, bytes)):
            return len(self.value)
        else:
            return 0 if self.value is None else 1

    def __eq__(self, other):
        if not isinstance(self.value, list) and isinstance(other, list):
            return [self.value] == other
        else:
            return self.value == other

    def __getitem__(self, *args, **kwargs):
        if hasattr(self.value, "__getitem__"):
            return self.value.__getitem__(*args, **kwargs)
        else:
            if args and args[0] == 0:
                return self.value
            else:
                raise TypeError(f"{self} is not subscriptable")

    def __contains__(self, val) -> bool:
        if hasattr(self.value, "__contains__"):
            return val in self.value
        else:
            return self.value == val

    def __bytes__(self) -> bytes:
        return to_bytes(self.value)


def parse_execute_response(response, pickled_results=False) -> ExecutionResult:
    if bool_ok(response):
        return ExecutionResult(True)
    elif isinstance(response, list) and len(response) == 2:
        result, errors = response
        if isinstance(result, list):
            if pickled_results:
                result = [cloudpickle.loads(value) for value in result]
            if len(result) == 1:
                result = result[0]
        return ExecutionResult(result, errors=errors)
    elif isinstance(response, bytes):
        return ExecutionResult(ExecID.parse(response))
    else:
        return ExecutionResult(response)


def parse_trigger_response(response, pickled_results=False) -> ExecutionResult:
    if pickled_results:
        response = [cloudpickle.loads(value) for value in response]

    if len(response) == 1:
        response = response[0]
    return ExecutionResult(response)


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


def parse_PD(value) -> Dict:
    str_val = safe_str(value)
    return dict(ast.literal_eval(str_val))


# @dataclass
@attr.s(auto_attribs=True, frozen=True)
class ExecutionInfo(RedisObject):
    executionId: ExecID = attr.ib(converter=ExecID.parse)  # type: ignore #7912
    status: ExecutionStatus = attr.ib(converter=ExecutionStatus)
    registered: bool


# @dataclass
@attr.s(auto_attribs=True, frozen=True)
class RegData(RedisObject):
    mode: str = attr.ib(converter=safe_str)  # Should be redgrease.TriggerMode
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
    reader: str = attr.ib(converter=safe_str)  # Should be redgrease.Reader
    desc: str
    RegistrationData: RegData = attr.ib(
        converter=RegData.from_redis  # type: ignore #7912
    )
    PD: Dict[Any, Any] = attr.ib(converter=parse_PD)


@attr.s(auto_attribs=True, frozen=True)
class ExecutionStep(RedisObject):
    type: str = attr.ib(converter=safe_str)
    duration: int
    name: str = attr.ib(converter=safe_str)
    arg: str = attr.ib(converter=safe_str)


@attr.s(auto_attribs=True, frozen=True)
class ExecutionPlan(RedisObject):
    status: ExecutionStatus = attr.ib(converter=ExecutionStatus)
    shards_received: int
    shards_completed: int
    results: int
    errors: int
    total_duration: int
    read_duration: int
    steps: List[ExecutionStep] = attr.ib(
        converter=list_parser(ExecutionStep.from_redis)  # type: ignore #7912
    )

    @staticmethod
    def parse(res: Iterable):
        return {
            str_if_bytes(shard[b"shard_id"]): ExecutionPlan.from_redis(  # type: ignore
                shard[b"execution_plan"]  # type: ignore
            )
            for shard in map(to_dict, res)
        }


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

    @staticmethod
    def parse(res):
        if not res or res == b"no cluster mode":
            return None

        cluster_info = ClusterInfo(
            my_id=safe_str(res[1]),
            shards=res[2],
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


def deseralize_gear_function(
    serialized_gear: str, python_version: str
) -> redgrease.gears.GearFunction:
    try:
        return cloudpickle.loads(serialized_gear)
    except Exception as err:
        import sys

        def pystr(pyver):
            return "Python %s.%s" % pyver

        runtime_python_version = sys.version_info[:2]
        function_python_version = ast.literal_eval(str(python_version))[:2]
        if runtime_python_version != function_python_version:
            raise SystemError(
                f"{pystr(runtime_python_version)} runtime cannot execute "
                f"Gears functions created in {function_python_version}. "
                "Only matching Python versions are supported"
            ) from err
        raise


def seralize_gear_function(gear_function: redgrease.gears.ClosedGearFunction) -> str:
    return f"""
import redgrease.data
import redgrease.runtime

gear_function = redgrease.data.deseralize_gear_function(
    {cloudpickle.dumps(gear_function, protocol=4)},
    python_version={tuple(sys.version_info)},
)
redgrease.runtime.run(gear_function, GearsBuilder)
"""
