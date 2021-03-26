# -*- coding: utf-8 -*-
"""
Gears function decorators
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
import uuid
from typing import Callable

import redgrease.reader
import redgrease.sugar
from redgrease.gears import ClosedGearFunction
from redgrease.typing import Callback


def trigger(
    trigger: str = None,
    prefix: str = "*",
    convertToStr: bool = True,
    collect: bool = True,
    mode: str = redgrease.sugar.TriggerMode.Async,
    onRegistered: Callback = None,
    **kargs,
) -> Callable[[Callable], ClosedGearFunction]:
    """Decorator for creation of CommandReader + Tigger GearFunctions

    Args:
        trigger (str, optional):
            The trigger string
            Will be a unique id if not specified.

        prefix (str, optional):
            Register prefix.
            Same as for the `register` operation.
            Defaults to "*".

        convertToStr (bool, optional):
            Convert the results to str.
            Same as for the `register` operation.
            Defaults to True.

        collect (bool, optional):
            Add a `collect' operation to the end of the function.
            Same as for the `register` operation.
            Defaults to True.

        mode (str, optional):
            The execution mode of the triggered function.
            Same as for the `register` operation.
            Defaults to redgrease.sugar.TriggerMode.Async.

        onRegistered (Callback, optional):
            A function callback thats called on each shard upon function registration.
            It is a good place to initialize non-serializable objects such as
            network connections.
            Same as for the `register` operation.
            Defaults to None.

    Returns:
        Callable[[Callable], ClosedGearFunction]:
            A ClosedGearFunction generator.
    """

    def command_gear(function):
        return (
            redgrease.reader.CommandReader()
            .map(lambda commmand_params: function(*commmand_params[1:]))
            .register(
                prefix=prefix,
                convertToStr=convertToStr,
                collect=collect,
                mode=mode,
                onRegistered=onRegistered,
                trigger=trigger if trigger else str(uuid.uuid4),
                **kargs,
            )
        )

    return command_gear
