import logging

import redgrease.operation as gearop
from redgrease.gears import GearTrainBuilder
from redgrease.sugar import LogLevel, Reader, TriggerMode
from redgrease.typing import Callback

logger = logging.getLogger(__name__)


class GearsBuilder(GearTrainBuilder):
    def __init__(
        self,
        reader: str = Reader.KeysReader,
        defaultArg: str = "*",
        desc: str = None,
    ):
        """Gear function / process factory
        Args:
            reader (str, optional): Input records reader
                Defining where the input to the gear will come from.
                One of:
                - 'KeysReader':
                - 'KeysOnlyReader':
                - 'StreamReader':
                - 'PythonReader':
                - 'ShardsReader':
                - 'CommandReader':
                Defaults to 'KeysReader'.

            defaultArg (str, optional):
                Additional arguments to the reader.
                These are usually a key's name, prefix, glob-like
                or a regular expression.
                Its use depends on the function's reader type and action.
                Defaults to '*'.

            desc (str, optional): An optional description.
                Defaults to None.
        """
        super().__init__(reader=reader, defaultArg=defaultArg, desc=desc)

    def run(
        self,
        arg: str = None,  # TODO: This can also be a Python generator
        convertToStr: bool = False,
        collect: bool = False,
        **kwargs,
        # TODO: Add all the Reader specific args here
    ):
        """Runs a gear function as a batch.
        The function is executed once and exits once the data is
        exhausted by its reader.
        Args:
            arg (str, optional): An optional argument that's passed to
                the reader as its defaultArg.
                It means the following:
                - A glob-like pattern for the KeysReader
                and KeysOnlyReader readers.
                - A key name for the StreamReader reader.
                - A Python generator for the PythonReader reader.
                Defaults to None.

            convertToStr (bool, optional): When `True` adds a map
                operation to the flow's end that stringifies records.
                Defaults to False.

            collect (bool, optional): When `True` adds a collect operation
                to flow's end.
                Defaults to False.
        Returns:s
            [type]: [description]
        """
        self.operations.append(
            gearop.Run(
                arg=arg, convertToStr=convertToStr, collect=collect, kwargs=kwargs
            )
        )
        return self

    def register(
        self,
        prefix: str = "*",  # Reader Specific: ...
        convertToStr: bool = False,
        collect: bool = False,
        mode: str = TriggerMode.Async,
        onRegistered: Callback = None,
        trigger: str = None,  # Reader Specific: CommandReader
        **kwargs,
        # TODO: Add all the Reader specific args here
    ):
        """Runs a Gear function as an event handler.
        The function is executed each time an event arrives.
        Each time it is executed, the function operates on the event's
        data and once done is suspended until its future invocations by
        new events.
        Args:
            prefix (str, optional): Key prefix pattern to match on.
                Not relevant for 'CommandReader' readers (see 'trigger').
                Defaults to '*'.

            convertToStr (bool, optional): When `True` adds a map
                operation to the flow's end that stringifies records.
                Defaults to True.
                collect (bool, optional): When True adds a collect operation
                to flow's end.
                Defaults to False.

            mode (str, optional): The execution mode of the function.
                Can be one of:
                - 'async': Execution will be asynchronous across the entire
                cluster.
                - 'async_local': Execution will be asynchronous and restricted
                to the handling shard.
                - 'sync': Execution will be synchronous and local.
                Defaults to 'async'.

            onRegistered (Callback, optional): A function callback that's
                called on each shard upon function registration.
                It is a good place to initialize non-serializable objects
                such as network connections.
                Defaults to None.

            trigger (str, optional): For 'CommandReader' only.
                The trigger string that will trigger the function.
                Defaults to None.
        Returns:
            [type]: [description]
        """
        self.operations.append(
            gearop.Register(
                prefix=prefix,
                convertToStr=convertToStr,
                collect=collect,
                mode=mode,
                onRegistered=onRegistered,
                trigger=trigger,
                kwargs=kwargs,
            )
        )
        return self


GB = GearsBuilder


class atomic:
    def __enter__(self):
        pass

    def __exit__(self, ex_type, ex_value, ex_traceback):
        pass


def execute(command: str, *args) -> bytes:
    """Execute an arbitrary Redis command.

    Args:
        command (str): The commant to execute

    Returns:
        bytes: Raw command response
    """
    return b"[PLACEHOLDER]"


def hashtag() -> bytes:
    """Returns a hashtag that maps to the lowest hash slot served by the local
    engine's shard. Put differently, it is useful as a hashtag for partitioning
    in a cluster.

    Returns:
        str: [description]
    """
    log("[PLACEHOLDER] hashtag()")
    return b"[PLACEHOLDER]"


def log(message: str, level: str = LogLevel.Notice):
    """Print a message to Redis' log.

    Args:
        message (str): The message to output
        level (str, optional): Message loglevel. Either:
        'debug', 'verbose', 'notice' or 'wartning'
            Defaults to 'notice'.
    """
    logger.log(level=LogLevel.to_logging_level(level), msg=message)


def configGet(key: str) -> bytes:
    """Fetches the current value of a RedisGears configuration option.

    Args:
        key (str): The configuration option key
    """
    log(f"[PLACEHOLDER] configGet(key={key})")
    return b"[PLACEHOLDER]"


def gearsConfigGet(key: str, default=None) -> bytes:
    """Fetches the current value of a RedisGears configuration option and returns a
    default value if that key does not exist.

    Args:
        key (str): The configuration option key.
        default ([type], optional): A default value.
            Defaults to None.
    """
    log(f"[PLACEHOLDER] gearsConfigGet(key={key}, default={default})")
    return b"[PLACEHOLDER]"
