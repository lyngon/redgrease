# -*- coding: utf-8 -*-
"""
Datatypes and parsers for the various structures, specific to Redis Gears.

These datatypes are returend from various redgrease functions, merely for the purpose
of providig more convenient structure, typing and documentation compared to the native
'list-based' structures.

They are generally not intended to be instantiated by end-users.
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
from typing import Any, Dict, Generic, Iterable, List, Optional, TypeVar, Union

import attr
import cloudpickle
import wrapt

from redgrease.utils import (
    REnum,
    bool_ok,
    list_parser,
    optional,
    safe_bool,
    safe_str,
    str_if_bytes,
    to_bytes,
    to_dict,
    to_kwargs,
)

T = TypeVar("T")


@attr.s(frozen=True, auto_attribs=True, repr=False)
class ExecID:
    """Execution ID

    Attributes:
        shard_id (str):
            Shard Identifier

        sequence (in):
            Sequence number
    """

    shard_id: str = "0000000000000000000000000000000000000000"
    sequence: int = 0

    def __repr__(self) -> str:
        class_name = self.__class__.__name__
        if self.shard_id == "0000000000000000000000000000000000000000":
            return f"{class_name}(sequence={self.sequence})"
        else:
            return f"""{class_name}("{self.shard_id}", {self.sequence})"""

    def __str__(self):
        return f"{self.shard_id}-{self.sequence}"

    def __bytes__(self):
        return str(self).encode()

    @staticmethod
    def parse(value: Union[str, bytes]) -> "ExecID":
        """Parses a string or bytes representation into a `redgrease.data.ExecID`

        Returns:
            redgrease.data.ExecID:
                Theo parsed ExecID

        Raises:
            ValueError:
                If the the value cannot be parsed.
        """
        if isinstance(value, bytes):
            value = value.decode()

        try:
            values = value.split("-")

            shard_id = values[0]
            sequence = int(values[1])
        except (AttributeError, IndexError) as e:
            raise ValueError(
                f"Unable to parse ExecID. Invalid serialization: '{value}'"
            ) from e

        return ExecID(shard_id=shard_id, sequence=sequence)


class ExecutionResult(wrapt.ObjectProxy, Generic[T]):
    """Common class for all types of execution results.
    Generic / Polymorphic on the result type (T) of the Gears function.

    Redis Gears specifies a few different commands for getting the results of a
    function execution (`pyexecute`, `getresults`, `getresultsblocking` and `trigger`),
    each potentially having more than one different possible value type, depending on
    context.

    In addition, while most gears functions result in collection of values, some
    functions (notably those ending with a `count` or `avg` operation) semantically
    always have scalar results, but are still natively returned as a list.

    redgrease.data.ExecutionResult is a unified result type for all scenarios,
    aiming at providing as intuitive API experience as possible.

    It is generic and walks and quacks just like the main result type (T) it wraps.
    With some notable exceptions:
        - It has an additional property `errors` containing any acumulated errors
        - Scalar results, behaves like scalars, but **also** like a single element list.

    This behavior isn't always perfect but gives for the most part an intuitive api
    experience.

    If the behaviour in some situations are confusing, the raw wrapped value can be
    accessed through the `value` property.
    """

    def __init__(self, value: T, errors: Optional[List] = None):
        wrapt.ObjectProxy.__init__(self, value)
        self._self_errors: Optional[List] = errors

    @property
    def value(self):
        """Gets the raw result value of the Gears function execution.

        Returns:
            T:
                The result value / values
        """
        return self.__wrapped__

    @property
    def errors(self) -> Optional[List]:
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

    def __call__(self, *args: Any, **kwds: Any) -> Any:
        return self.value(*args, **kwds)


def parse_execute_response(response) -> ExecutionResult:
    """Parses raw responses from `pyexecute`, `getresults` and `getresultsblocking`
    into a `redgrease.data.ExecuteResponse` object.

    Args:
        response (Any):
            The raw gears function response.
            This is most commonly a tuple of a list with the actual results and a list
            of errors  List[List[Union[T, Any]]].

            For some scenarios the response may take other forms, like a simple `Ok`
            (e.g. in the absence of a closing `run()` operation) or an excecution ID
            (e.g. for non-blocking executions).

    Returns:
        ExecutionResult[T]:
            A parsed execution response
    """
    if bool_ok(response):
        # Any "Ok" response is just True
        return ExecutionResult(True)
    elif isinstance(response, list) and len(response) == 2:
        # The 'normal' list-of-lists response
        # Results are unpickled if they are pickled
        # Note that the special case when the resiult only has one element,
        #   then the single value is used instread
        #   as the ExecutionResults would pretend it is a list anyway, if needed
        #   This way scalar results from for example `count` and `avg` will behave
        #   like scalars which is what is typically wanted

        result, errors = response
        if isinstance(result, list):
            try:
                result = [cloudpickle.loads(value) for value in result]
            except (TypeError, cloudpickle.pickle.UnpicklingError):
                pass
            if len(result) == 1:
                result = result[0]
        return ExecutionResult(result, errors=errors)
    elif isinstance(response, bytes):
        # Bytes response means ExecID
        return ExecutionResult(ExecID.parse(response))
    else:
        # If the response doesnt fit any known pattern, its returned as-is
        return ExecutionResult(response)


def parse_trigger_response(response) -> ExecutionResult:
    """Parses raw responses from `trigger` into a `redgrease.data.ExecuteResponse`
    object.

    Args:
        response (Any):
            The gears function response.
            This is a tuple of a list with the actual results and a list of errors
            List[List[Union[T, Any]]].

        pickled (bool, optional):
            Indicates if the response is pickled and need to be unpickled.
            Defaults to False.

    Returns:
        ExecutionResult[T]:
            A parsed execution response
    """
    try:
        response = [cloudpickle.loads(value) for value in response]
    except (TypeError, cloudpickle.pickle.UnpicklingError):
        pass

    if len(response) == 1:
        response = response[0]
    return ExecutionResult(response)


class ExecutionStatus(REnum):
    """Representation of the various states an execution could be in."""

    created = b"created"
    """Created - The execution has been created."""

    running = b"running"
    """Running - The execution is running."""

    done = b"done"
    """Done - Thee execution is done."""

    aborted = b"aborted"
    """Aborted - The execution has been aborted."""

    pending_cluster = b"pending_cluster"
    """Pending Cluster - Initiator is waiting for all workers to finish."""

    pending_run = b"pending_run"
    """Pending Run - Worker is pending ok from initiator to execute."""

    pending_receive = b"pending_receive"
    """Pending Receive - Initiator is pending acknowledgement from workers on receiving
    execution.
    """

    pending_termination = b"pending_termination"
    """Pending Termination - Worker is pending termination messaging from initiator"""


# TODO: Isn't this sugar rather than data?
class ExecLocality(REnum):
    """Locality of exetution: Shard or Cluster"""

    Shard = "Shard"
    Cluster = "Cluster"


class RedisObject:
    """Base class for many of the more complex Redis Gears configuration values"""

    @classmethod
    def from_redis(cls, params) -> "RedisObject":
        """Default parser

        Assumes the object is seralized as a list of alternating attribute names and
        values.

        Note: This method should not be invoked directly on the 'RedisObject'
        base class.
        It should be only be invoked on subclasses of RedisObjects.

        Returns:
            RedisObect:
                Returns the RedisObject subclass if, and only if, its constructor
                argument names and value types exactly match the names and values
                in the input list.

        Raises:
            TypeError:
                If either the input list contains attributes not defined in the
                subclass consructor, or if the subclass defines mandatory constructor
                arguments that are not present in the input list.
        """
        return cls(**to_kwargs(params))  # type: ignore


def parse_PD(value: Union[str, bytes]) -> Dict:
    """Parses str or bytes to a dict.

    Used for the 'PD' field in the 'Registration' type, returned by 'getregistrations'.

    Args:
        value (Union[str,bytes]):
            Serialized version of a dict.

    Returns:
        Dict:
            A dictionary
    """
    str_val = safe_str(value)
    return dict(ast.literal_eval(str_val))


@attr.s(auto_attribs=True, frozen=True)
class ExecutionInfo(RedisObject):
    """Return object for `redgrease.client.Redis.dumpexecutions` command."""

    executionId: ExecID = attr.ib(converter=ExecID.parse)  # type: ignore #7912
    """The execution Id"""

    status: ExecutionStatus = attr.ib(converter=ExecutionStatus)
    """The status"""

    registered: bool = attr.ib(converter=safe_bool)
    """Indicates whether this is a registered execution"""


@attr.s(auto_attribs=True, frozen=True)
class RegData(RedisObject):
    """Object reprenting the values for the `Registration.RegistrationData`, part of
    the return value of `redgrease.client.dupregistrations` command.
    """

    mode: str = attr.ib(converter=safe_str)
    """Registration mode."""

    numTriggered: int
    """A counter of triggered executions."""

    numSuccess: int
    """A counter of successful executions."""

    numFailures: int
    """A counter of failed executions."""

    numAborted: int
    """A conter of aborted executions."""

    lastError: str
    """The last error returned."""

    args: Dict[str, Any] = attr.ib(converter=to_kwargs)
    """Reader-specific arguments"""

    status: Optional[bool] = attr.ib(
        converter=optional(bool_ok),  # type: ignore #7912
        default=None,
    )
    """Undocumented status field"""


# @dataclass
@attr.s(auto_attribs=True, frozen=True)
class Registration(RedisObject):
    """Return object for `redgrease.client.Redis.dumpregistrations` command.
    Contains the information about a function registration.

    """

    id: ExecID = attr.ib(converter=ExecID.parse)  # type: ignore #7912
    """The registration ID."""

    reader: str = attr.ib(converter=safe_str)
    """The Reader."""

    desc: str
    """The description."""

    RegistrationData: RegData = attr.ib(
        converter=RegData.from_redis  # type: ignore #7912
    )
    """Registration Data, see `RegData`."""

    PD: Dict[Any, Any] = attr.ib(converter=parse_PD, repr=False)
    """Private data"""


@attr.s(auto_attribs=True, frozen=True)
class ExecutionStep(RedisObject):
    """Object reprenting a 'step' in the `ExecutionPlan.steps`, attribut of
    the return value of `redgrease.client.getexecution` command.
    """

    type: str = attr.ib(converter=safe_str)
    """Step type."""

    duration: int
    """The step's duration in milliseconds (0 when `ProfileExecutions` is disabled)"""

    name: str = attr.ib(converter=safe_str)
    """Step callback"""

    arg: str = attr.ib(converter=safe_str)
    """Step argument"""


@attr.s(auto_attribs=True, frozen=True)
class ExecutionPlan(RedisObject):
    """Object representing the exetution plan for a given shard in the response from the
    `redgrease.client.Redis.getexetution` command.
    """

    status: ExecutionStatus = attr.ib(converter=ExecutionStatus)
    """The current status of the execution."""

    shards_received: int
    """Number of shards that received the execution."""

    shards_completed: int
    """Number of shards that completed the execution."""

    results: int
    """Count of results returned."""

    errors: int
    """Count of the errors raised."""

    total_duration: int
    """Total execution duration in milliseconds."""

    read_duration: int
    """Reader execution duration in milliseconds."""

    steps: List[ExecutionStep] = attr.ib(
        converter=list_parser(ExecutionStep.from_redis)  # type: ignore #7912
    )
    """The steps of the execution plan."""

    @staticmethod
    def parse(res: Iterable[bytes]) -> Dict[str, "ExecutionPlan"]:
        """Parse the raw results of `redgrease.client.Redis.getexetution` into a dict
        that maps from shard identifiers to ExecutionStep objects.

        Returns:
            Dict[str, ExecutionPlan]:
                Execution plan mapping.
        """
        return {
            str_if_bytes(shard[b"shard_id"]): ExecutionPlan.from_redis(  # type: ignore
                shard[b"execution_plan"]  # type: ignore
            )
            for shard in map(to_dict, res)
        }


@attr.s(auto_attribs=True, frozen=True)
class ShardInfo(RedisObject):
    """Object representing a shard in the `CluserInfo.shards` attribute in the response
    from `redgrease.client.Redis.infocluster` command.
    """

    id: str = attr.ib(converter=safe_str)
    """The shard's identifoer int the cluster."""

    ip: str = attr.ib(converter=safe_str)
    """The shard's IP address."""

    port: int
    """The shard's port."""

    unixSocket: str = attr.ib(converter=safe_str)
    """The shards UDS."""

    runid: str = attr.ib(converter=safe_str)
    """The engines run identifier."""

    minHslot: int
    """Lowest hash slot served by the shard."""

    maxHslot: int
    """Highest hash slot served by the shard."""

    pendingMessages: int
    """Number of pending messages"""


@attr.s(auto_attribs=True, frozen=True)
class ClusterInfo(RedisObject):
    """Information about the Redis Gears cluster.

    Return object for `redgrease.client.Redis.infocluster` command.
    """

    my_id: str
    """The identifier of the shard the client is connected to."""

    my_run_id: str

    shards: List[ShardInfo] = attr.ib(
        converter=list_parser(ShardInfo.from_redis)  # type: ignore
    )
    """List of the all the shards in the cluster."""

    @staticmethod
    def parse(res: bytes) -> Optional["ClusterInfo"]:
        """Parses the response from `redgrease.client.Redis.infocluster` into a
        `ClusterInfo` object.

        If the client is not connected to a Redis Gears cluster, `None` is returned.

        Returns:
            Optional[ClusterInfo]:
                A ClusterInfo object or None (if not in cluster mode).s
        """

        if not res or safe_str(res) == "no cluster mode":
            return None

        cluster_info = ClusterInfo(
            my_id=safe_str(res[1]),
            my_run_id=safe_str(res[3]),
            shards=res[4],
        )

        return cluster_info


@attr.s(auto_attribs=True, frozen=True)
class PyStats(RedisObject):
    """Memory usage statistics from the Python interpreter.
    As returned by `redgrease.client.Redis.pystats`
    """

    TotalAllocated: int
    """A total of all allocations over time, in bytes."""

    PeakAllocated: int
    """The peak allocations, in bytes."""

    CurrAllocated: int
    """The currently allocated memory, in bytes."""


@attr.s(auto_attribs=True, frozen=True)
class PyRequirementInfo(RedisObject):
    """Information about a Python requirement / dependency."""

    GearReqVersion: int
    """An internally-assigned version of the requirement.
    (note: this isn't the package's version)
    """

    Name: str = attr.ib(converter=safe_str)
    """The name of the requirement as it was given to the 'requirements' argument
    of the `pyexecute` command.
    """

    IsDownloaded: bool = attr.ib(converter=safe_bool)
    """`True` if the requirement wheels was successfully download, otherwise `False`.
    """

    IsInstalled: bool = attr.ib(converter=safe_bool)
    """`True` if the requirement wheels was successfully installed, otherwise `False`.
    """

    CompiledOs: str = attr.ib(converter=safe_str)
    """The underlying Operating System"""

    Wheels: List[str] = attr.ib(
        converter=lambda wheels: safe_str(wheels)  # type: ignore
        if isinstance(wheels, bytes)
        else [safe_str(wheel) for wheel in wheels]
    )
    """A List of Wheels required by the requirement."""
