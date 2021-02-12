from typing import AnyStr, Callable, Iterable, Union

import redis
from packaging.version import Version

import redgrease
from redgrease.gears import GearTrainBuilder
from redgrease.typing import Callback


class GearReader(GearTrainBuilder):
    def __init__(
        self,
        reader: str,
        defaultArg: str = "*",
        desc: str = None,
        requirements: Iterable[str] = None,
        require_runtime: Union[Version, str, bool] = True,
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

            requirements (Iterable[str], optional):
                Package lependencies for the gear train.
                Defaults to None.

            require_runtime (Union[Version, str, bool], optional):
                Auto-require the redgrease server runtime.
                Defaults to False.
        """
        super().__init__(reader=reader, defaultArg=defaultArg, desc=desc)

        if require_runtime:
            if isinstance(require_runtime, Version):
                runtime_package = f"redgrease[runtime]=={require_runtime}"
            elif isinstance(require_runtime, str):
                if require_runtime[0] in ["=", ">", "<", "!"]:
                    runtime_package = f"redgrease[runtime]{require_runtime}"
                elif require_runtime[0].isnumeric():
                    runtime_package = f"redgrease[runtime]=={require_runtime}"
                else:
                    runtime_package = require_runtime
            else:
                runtime_package = "redgrease[runtime]"
            self.requirements = [runtime_package]
        else:
            self.requirements = []

        if requirements:
            self.requirements += list(requirements)

    def _run(
        self,
        on: redis.Redis,
        arg: str = None,  # TODO: This can also be a Python generator
        convertToStr: bool = True,
        collect: bool = True,
        **kwargs,
    ):
        pass

    def _register(
        self,
        on: redis.Redis,
        convertToStr: bool = False,
        collect: bool = False,
        mode: str = redgrease.TriggerMode.Async,
        onRegistered: Callback = None,
        **kwargs,
    ):
        pass


class KeysReader(GearReader):
    def __init__(
        self,
        default_key_pattern: str = "*",
    ):
        super().__init__(
            reader=redgrease.Reader.KeysReader, defaultArg=default_key_pattern
        )
        self.default_key_pattern = default_key_pattern

    def run(
        self,
        on: redis.Redis,
        key_pattern=None,
        convertToStr: bool = False,
        collect: bool = False,
        noScan: bool = False,
        readValue: bool = True,
        **kwargs,
    ):
        return self._run(
            on=on,
            arg=key_pattern,
            convertToStr=convertToStr,
            collect=collect,
            noScan=noScan,
            readValue=readValue,
            **kwargs,
        )

    def register(
        self,
        on: redis.Redis,
        prefix: str = None,
        event_types: list = None,  # TODO: Enumerator?
        key_types: list = None,  # TODO: Enumerator?
        readValue: bool = True,
        convertToStr: bool = False,
        collect: bool = False,
        mode: str = redgrease.TriggerMode.Async,
        onRegistered: Callback = None,
        **kwargs,
    ):
        return self._register(
            on=on,
            prefix=prefix,
            collect=collect,
            mevent_types=event_types,
            key_types=key_types,
            readValue=readValue,
            convertToStr=convertToStr,
            ode=mode,
            onRegistered=onRegistered,
            **kwargs,
        )


class KeysOnlyReader(GearReader):
    def __init__(
        self,
        default_key_pattern: str = "*",
    ):
        super().__init__(
            reader=redgrease.Reader.KeysReader, defaultArg=default_key_pattern
        )
        self.default_key_pattern = default_key_pattern

    def run(
        self,
        on: redis.Redis,
        pattern: str = "*",
        count: int = 1000,
        patternGenerator: Callable[[AnyStr], str] = None,
        convertToStr: bool = False,
        collect: bool = False,
        noScan: bool = False,
        readValue: bool = True,
        **kwargs,
    ):
        return self._run(
            on=on,
            pattern=pattern,
            count=count,
            patternGenerator=patternGenerator,
            convertToStr=convertToStr,
            collect=collect,
            noScan=noScan,
            readValue=readValue,
            **kwargs,
        )


class StreamReader(GearReader):
    def __init__(
        self,
        default_key_pattern: str = "*",
    ):
        super().__init__(
            reader=redgrease.Reader.KeysReader, defaultArg=default_key_pattern
        )
        self.default_key_pattern = default_key_pattern

    def run(
        self,
        on: redis.Redis,
        fromId: Union[redgrease.data.ExecID, str] = None,
        convertToStr: bool = False,
        collect: bool = False,
        noScan: bool = False,
        readValue: bool = True,
        **kwargs,
    ):
        return self._run(
            on=on,
            fromId=fromId if fromId else redgrease.data.ExecID(),
            convertToStr=convertToStr,
            collect=collect,
            noScan=noScan,
            readValue=readValue,
            **kwargs,
        )

    def register(
        self,
        on: redis.Redis,
        prefix: str = "*",
        batch: int = 1,
        duration: int = 0,
        onFailedPolicy: str = "continue",
        onFailedRetryInterval: int = 1,
        trimStream: bool = True,
        convertToStr: bool = False,
        collect: bool = False,
        mode: str = redgrease.TriggerMode.Async,
        onRegistered: Callback = None,
        **kwargs,
    ):
        return self._register(
            on=on,
            prefix=prefix,
            batch=batch,
            duration=duration,
            onFailedPolicy=onFailedPolicy,
            onFailedRetryInterval=onFailedRetryInterval,
            trimStream=trimStream,
            convertToStr=convertToStr,
            ode=mode,
            onRegistered=onRegistered,
            **kwargs,
        )


class PythonReader(GearReader):
    def __init__(
        self,
        dummy_arg: str = None,
    ):
        super().__init__(reader=redgrease.Reader.KeysReader, defaultArg="*")
        self.dummy_arg = dummy_arg

    def run(
        self,
        on: redis.Redis,
        generator: Iterable,
        convertToStr: bool = False,
        collect: bool = False,
        noScan: bool = False,
        readValue: bool = True,
        **kwargs,
    ):
        return self._run(
            on=on,
            generator=generator,
            convertToStr=convertToStr,
            collect=collect,
            noScan=noScan,
            readValue=readValue,
            **kwargs,
        )


class ShardsIDReader(GearReader):
    def __init__(
        self,
        dummy_arg: str = None,
    ):
        super().__init__(reader=redgrease.Reader.KeysReader, defaultArg="*")
        self.dummy_arg = dummy_arg

    def run(
        self,
        on: redis.Redis,
        convertToStr: bool = False,
        collect: bool = False,
        noScan: bool = False,
        readValue: bool = True,
        **kwargs,
    ):
        return self._run(
            on=on,
            convertToStr=convertToStr,
            collect=collect,
            noScan=noScan,
            readValue=readValue,
            **kwargs,
        )


class CommandReader(GearReader):
    def __init__(
        self,
        dummy_arg: str = None,
    ):
        super().__init__(reader=redgrease.Reader.KeysReader, defaultArg="*")
        self.dummy_arg = dummy_arg

    def register(
        self,
        on: redis.Redis,
        trigger: str,
        convertToStr: bool = False,
        collect: bool = False,
        mode: str = redgrease.TriggerMode.Async,
        onRegistered: Callback = None,
        **kwargs,
    ):
        return self._register(
            on=on,
            trigger=trigger,
            convertToStr=convertToStr,
            ode=mode,
            onRegistered=onRegistered,
            **kwargs,
        )
