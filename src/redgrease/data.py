import ast
from typing import Any, Dict, Iterable, List, Union

import attr
import cloudpickle

from redgrease.utils import (
    REnum,
    bool_ok,
    list_parser,
    safe_bool,
    safe_str,
    str_if_bytes,
    to_dict,
    to_kwargs,
)


@attr.s(auto_attribs=True, frozen=True)
class Execution:
    results: List
    errors: List


# TODO: Should this be in utils?
# TODO: Rethink how execution redponses should be handlde
def parse_execute_response(response, pickled_results=False):
    if bool_ok(response):
        return True
    elif isinstance(response, list) and len(response) == 2:
        results, errors = response
        if pickled_results:
            results = [cloudpickle.loads(result) for result in results]
        return Execution(results, errors)
    elif isinstance(response, bytes):
        return Execution(ExecID.parse(response), [])
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
