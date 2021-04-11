# -*- coding: utf-8 -*-
"""
Sugar classes for the various Gears Reader types.
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


from typing import Callable, Dict, Iterable, Optional

import redgrease.gears
import redgrease.sugar
import redgrease.typing
import redgrease.utils


class GearReader(redgrease.gears.PartialGearFunction[redgrease.typing.OutputRecord]):
    """Base class for the Reader sugar classes.

    Extends `redgrease.runtime.GearsBuilder' with arguments for collecting
    requirements.
    """

    def __init__(
        self,
        reader: str = redgrease.sugar.ReaderType.KeysReader,
        defaultArg: str = "*",
        desc: str = None,
        requirements: Optional[Iterable[str]] = None,
    ):
        """Gear function / process factory
        Args:
            reader (str, optional):
                Input records reader.
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

            desc (str, optional):
                An optional description.
                Defaults to None.

            requirements (Iterable[str], optional):
                Package lependencies for the gear train.
                Defaults to None.
        """
        reader_op = redgrease.gears.Reader(reader, defaultArg, desc)
        super().__init__(operation=reader_op, requirements=requirements)


class KeysReader(GearReader[redgrease.typing.Record]):
    """KeysReader is a convenience class for GearsBuilder("KeysReader", ...)"""

    def __init__(
        self,
        default_key_pattern: str = "*",
        desc: str = None,
        requirements: Optional[Iterable[str]] = None,
    ):
        """Instantiate a KeysReader partial Gear function.

        Args:
            default_key_pattern (str, optional):
                Default Redis key pattern for the keys (and its values, type) to read.
                Defaults to "*".

            desc (str, optional):
                An optional description.
                Defaults to None.

            requirements (Iterable[str], optional):
                Package lependencies for the gear train.
                Defaults to None.
        """
        super().__init__(
            reader=redgrease.sugar.ReaderType.KeysReader,
            defaultArg=default_key_pattern,
            desc=desc,
            requirements=requirements,
        )
        self.default_key_pattern = default_key_pattern

    def values(self, type=..., event=...) -> redgrease.gears.PartialGearFunction:
        """Filter out and select the values of the records only.

        Args:
            type (Union[str, Container[str]], optional):
                A single string, or a container of several strings, representing the
                Redis type(s) of keys to select.
                Valid values include: "string", "hash", "list", "set", "zset",
                "stream" or "module".
                Defaults to ... (Ellipsis), meaning any type.

            event (Union[str, Container[str]], optional):
                A single string, or a container of several strings, representing the
                Redis command or event `that .
                Defaults to ... (Ellipsis), menaing any event.

        Returns:
            PartialGearFunction:
                A new partial gear function generating the matching values.
        """
        f: redgrease.gears.PartialGearFunction = self
        if type != ... or event != ...:
            f = f.filter(redgrease.utils.dict_filter(type=type, event=event))
        return f.map(lambda record: record["value"])

    def keys(self, type=..., event=...) -> redgrease.gears.PartialGearFunction[str]:
        """Filter out and select the keys of the records only.

        Args:
            type (Union[str, Container[str]], optional):
                A single string, or a container of several strings, representing the
                Redis type(s) of keys to select.
                Valid values include: "string", "hash", "list", "set", "zset",
                "stream" or "module".
                Defaults to ... (Ellipsis), meaning any type.

            event (Union[str, Container[str]], optional):
                A single string, or a container of several strings, representing the
                Redis command or event `that .
                Defaults to ... (Ellipsis), menaing any event.

        Returns:
            PartialGearFunction[str]:
                A new partial gear function generating the matching keys.
        """
        f: redgrease.gears.PartialGearFunction = self
        if type != ... or event != ...:
            f = f.filter(redgrease.utils.dict_filter(type=type, event=event))
        return f.map(lambda record: record["key"])

    def records(
        self, type=None, event=...
    ) -> redgrease.gears.PartialGearFunction[redgrease.utils.Record]:
        """Filter out and map the records to `redgrease.utils.Record` objects.

        This provides the fields `key`, `value`, `type` and `event` as typed attributes
        on an object, instead of items in a `dict`, making it a little bit more
        pleasant to work with.

        Args:
            type (Union[str, Container[str]], optional):
                A single string, or a container of several strings, representing the
                Redis type(s) of keys to select.
                Valid values include: "string", "hash", "list", "set", "zset",
                "stream" or "module".
                Defaults to ... (Ellipsis), meaning any type.

            event (Union[str, Container[str]], optional):
                A single string, or a container of several strings, representing the
                Redis command or event `that.
                Defaults to ... (Ellipsis), menaing any event.

        Returns:
            PartialGearFunction[redgrease.utils.Record]:
                A new partial gear function generating the matching Record values.
        """
        f: redgrease.gears.PartialGearFunction = self
        if type != ... or event != ...:
            f = f.filter(redgrease.utils.dict_filter(type=type, event=event))
        return f.map(redgrease.utils.record)


class KeysOnlyReader(GearReader[str]):
    """KeysOnlyReader is a convenience class for GearsBuilder("KeysOnlyReader", ...)"""

    def __init__(
        self,
        default_key_pattern: str = "*",
        desc: str = None,
        requirements: Optional[Iterable[str]] = None,
    ):
        """Instantiate a KeysOnlyReader partial Gear function.

        Args:
            default_key_pattern (str, optional):
                Default Redis keys pattern for the keys to read.
                Defaults to "*".

            desc (str, optional):
                An optional description.
                Defaults to None.

            requirements (Iterable[str], optional):
                Package lependencies for the gear train.
                Defaults to None.
        """
        super().__init__(
            reader=redgrease.sugar.ReaderType.KeysOnlyReader,
            defaultArg=default_key_pattern,
            desc=desc,
            requirements=requirements,
        )
        self.default_key_pattern = default_key_pattern


class StreamReader(GearReader[redgrease.typing.Record]):
    """StreamReader is a convenience class for GearsBuilder("StreamReader", ...)"""

    def __init__(
        self,
        default_key_pattern: str = "*",
        desc: str = None,
        requirements: Optional[Iterable[str]] = None,
    ):
        """Instantiate a StreamReader partial Gear function.

        Args:
            default_key_pattern (str, optional):
                Default Redis keys pattern for the redis stream(s) to read.
                Defaults to "*".

            desc (str, optional):
                An optional description.
                Defaults to None.

            requirements (Iterable[str], optional):
                Package lependencies for the gear train.
                Defaults to None.
        """
        super().__init__(
            reader=redgrease.sugar.ReaderType.StreamReader,
            defaultArg=default_key_pattern,
            desc=desc,
            requirements=requirements,
        )
        self.default_key_pattern = default_key_pattern

    def values(self) -> redgrease.gears.PartialGearFunction[Dict]:
        """Select the values of the stream only.

        Returns:
            PartialGearFunction:
                A new partial gear function generating values.
        """
        return self.map(lambda record: record["value"])

    def keys(self) -> redgrease.gears.PartialGearFunction[str]:
        """Select the keys, i.e. stream names, only.

        Returns:
            PartialGearFunction[str]:
                A new partial gear function generating names.
        """
        return self.map(lambda record: record["key"])

    def records(
        self,
    ) -> redgrease.gears.PartialGearFunction[redgrease.utils.StreamRecord]:
        """Filter out and map the records to `redgrease.utils.StreamRecord` objects.

        This provides the fields `key`, `id` and `value` as typed attributes on an
        object, instead of items in a `dict`, making it a little bit more pleasant
        to work with.

        Returns:
            PartialGearFunction[redgrease.utils.Record]:
                A new partial gear function generating the matching Record values.
        """
        return self.map(redgrease.utils.stream_record)


class PythonReader(GearReader):
    """PythonReader is a convenience class for GearsBuilder("PythonReader", ...)"""

    def __init__(
        self,
        desc: str = None,
        requirements: Optional[Iterable[str]] = None,
    ):
        """Instantiate a PythonReader partial Gear function.

        Args:
            desc (str, optional):
                An optional description.
                Defaults to None.

            requirements (Iterable[str], optional):
                Package lependencies for the gear train.
                Defaults to None.
        """
        super().__init__(
            reader=redgrease.sugar.ReaderType.PythonReader,
            desc=desc,
            requirements=requirements,
        )


class ShardsIDReader(GearReader[str]):
    """ShardsIDReader is a convenience class for GearsBuilder("ShardsIDReader", ...)"""

    def __init__(
        self,
        desc: str = None,
        requirements: Optional[Iterable[str]] = None,
    ):
        """Instantiate a ShardsIDReader partial Gear function.

        Args:
            desc (str, optional):
                An optional description.
                Defaults to None.

            requirements (Iterable[str], optional):
                Package lependencies for the gear train.
                Defaults to None.
        """
        super().__init__(
            reader=redgrease.sugar.ReaderType.ShardsIDReader,
            desc=desc,
            requirements=requirements,
        )


class CommandReader(GearReader):
    """CommandReader is a convenience class for GearsBuilder("CommandReader", ...)"""

    def __init__(
        self,
        desc: str = None,
        requirements: Optional[Iterable[str]] = None,
    ):
        """Instantiate a CommandReader partial Gear function.

        Args:
            desc (str, optional):
                An optional description.
                Defaults to None.

            requirements (Iterable[str], optional):
                Package lependencies for the gear train.
                Defaults to None.
        """
        super().__init__(
            reader=redgrease.sugar.ReaderType.CommandReader,
            desc=desc,
            requirements=requirements,
        )

    def args(self, max_count: int = None) -> redgrease.gears.PartialGearFunction:
        """Ignore the trigger name and take only the arguments following the trigger.

        Args:
            max_count (int, optional):
                Maximum number of args to take. Any additional args will be truncated.
                Defaults to None.

        Returns:
            redgrease.gears.PartialGearFunction:
                A new partial gear function only generating the trigger arguments.
        """
        if max_count:
            max_count += 1
        return self.map(lambda x: x[1:max_count])

    def apply(
        self, fun: Callable[..., redgrease.typing.OutputRecord]
    ) -> redgrease.gears.PartialGearFunction[redgrease.typing.OutputRecord]:
        """Apply a function to the trigger arguments.

        Args:
            fun (Callable[..., redgrease.typing.OutputRecord]):
                The function to call with the trigger arguments.

        Returns:
            redgrease.gears.PartialGearFunction[redgrease.typing.OutputRecord]:
                A new partial gear function generating the results of the function.
        """
        return self.map(lambda args: fun(*args[1:]))
