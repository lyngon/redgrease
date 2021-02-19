from typing import Iterable, Union

from packaging.version import Version

import redgrease
import redgrease.runtime


class GearReader(redgrease.runtime.GearsBuilder):
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


class KeysReader(GearReader):
    def __init__(
        self,
        default_key_pattern: str = "*",
    ):
        super().__init__(
            reader=redgrease.Reader.KeysReader, defaultArg=default_key_pattern
        )
        self.default_key_pattern = default_key_pattern


class KeysOnlyReader(GearReader):
    def __init__(
        self,
        default_key_pattern: str = "*",
    ):
        super().__init__(
            reader=redgrease.Reader.KeysOnlyReader, defaultArg=default_key_pattern
        )
        self.default_key_pattern = default_key_pattern


class StreamReader(GearReader):
    def __init__(
        self,
        default_key_pattern: str = "*",
    ):
        super().__init__(
            reader=redgrease.Reader.StreamReader, defaultArg=default_key_pattern
        )
        self.default_key_pattern = default_key_pattern


class PythonReader(GearReader):
    def __init__(self):
        super().__init__(reader=redgrease.Reader.PythonReader)


class ShardsIDReader(GearReader):
    def __init__(self):
        super().__init__(reader=redgrease.Reader.ShardsIDReader)


class CommandReader(GearReader):
    def __init__(self):
        super().__init__(reader=redgrease.Reader.CommandReaderReader)
