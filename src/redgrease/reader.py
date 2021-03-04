import redgrease.runtime
import redgrease.sugar

# from packaging.version import Version


class GearReader(redgrease.runtime.GearsBuilder):
    def __init__(
        self,
        reader: str = redgrease.sugar.ReaderType.KeysReader,
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

            requirements (Iterable[str], optional):
                Package lependencies for the gear train.
                Defaults to None.

            require_runtime (Union[Version, str, bool], optional):
                Auto-require the redgrease server runtime.
                Defaults to False.
        """
        super().__init__(reader=reader, defaultArg=defaultArg, desc=desc)


class KeysReader(GearReader):
    def __init__(
        self,
        default_key_pattern: str = "*",
    ):
        super().__init__(
            reader=redgrease.sugar.ReaderType.KeysReader, defaultArg=default_key_pattern
        )
        self.default_key_pattern = default_key_pattern


class KeysOnlyReader(GearReader):
    def __init__(
        self,
        default_key_pattern: str = "*",
    ):
        super().__init__(
            reader=redgrease.sugar.ReaderType.KeysOnlyReader,
            defaultArg=default_key_pattern,
        )
        self.default_key_pattern = default_key_pattern


class StreamReader(GearReader):
    def __init__(
        self,
        default_key_pattern: str = "*",
    ):
        super().__init__(
            reader=redgrease.sugar.ReaderType.StreamReader,
            defaultArg=default_key_pattern,
        )
        self.default_key_pattern = default_key_pattern


class PythonReader(GearReader):
    def __init__(self):
        super().__init__(reader=redgrease.sugar.ReaderType.PythonReader)


class ShardsIDReader(GearReader):
    def __init__(self):
        super().__init__(reader=redgrease.sugar.ReaderType.ShardsIDReader)


class CommandReader(GearReader):
    def __init__(self):
        super().__init__(reader=redgrease.sugar.ReaderType.CommandReader)
