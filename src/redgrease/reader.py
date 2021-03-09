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


from typing import Iterable, Optional

import redgrease.runtime
import redgrease.sugar


class GearReader(redgrease.runtime.GearsBuilder):
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
        super().__init__(
            reader=reader,
            defaultArg=defaultArg,
            desc=desc,
            requirements=requirements,
        )


class KeysReader(GearReader):
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


class KeysOnlyReader(GearReader):
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


class StreamReader(GearReader):
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


class ShardsIDReader(GearReader):
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
