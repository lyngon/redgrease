# -*- coding: utf-8 -*-
"""
Helper functions for searialization, de-serialization, reading and writing Gear
functions.
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
import os
import sys
from typing import Any, Dict, Tuple, Union

import cloudpickle

import redgrease.gears
import redgrease.runtime


def deseralize_gear_function(
    serialized_gear: str, python_version: str
) -> redgrease.gears.GearFunction:
    """Safely deserializes (unpickles) a serialized GearFunction.

    This function is only executed on the Gear server.

    It handles and appropriately reports errors due to mismatch between
    client and server Python versions.

    Args:
        serialized_gear (str):
            The serialized (cloudpickled) GearFunction.

        python_version (str):
            The python version of the client.

    Raises:
        SystemError:
            If the Python versions of the client and server mismatch.

    Returns:
        redgrease.gears.GearFunction:
            The Gear function, as created on at the client.
    """
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
                "Client / server Python version mismatch."
                f"{pystr(runtime_python_version)} runtime unable to execute "
                f"Gears functions created in {function_python_version}. "
            ) from err
        raise


def seralize_gear_function(gear_function: redgrease.gears.GearFunction) -> str:
    """Serializes a GearFunction into a wrapper code-string that can be sent to the
    Gear server to execute.

    Args:
        gear_function (redgrease.gears.ClosedGearFunction):
            GearFunction to serialize.

    Returns:
        str:
            Code string that will execute the GearFunction on the server.
    """

    # The Gear function is serialized with 'cloudpickle' and embedded in a
    # code string that will de-serialize it back and then 'construct' the actual
    # Gear function and run it.

    return f"""
import redgrease.gearialization
import redgrease.runtime
gear_function = redgrease.gearialization.deseralize_gear_function(
    {cloudpickle.dumps(gear_function, protocol=4)},
    python_version={tuple(sys.version_info)},
)
redgrease.runtime.run(gear_function, GearsBuilder)
"""


def get_function_string(
    gear_function: Union[
        str, redgrease.runtime.GearsBuilder, redgrease.gears.GearFunction
    ]
) -> Tuple[str, Dict[str, Any]]:
    """Generate the function string representation.

    Args:
        gear_function (Union[ str, redgrease.runtime.GearsBuilder, redgrease.gears.GearFunction ]):
            - A string containgg a clear-text serialized Gears Python function as
                per the official documentation.
                (https://oss.redislabs.com/redisgears/intro.html#the-simplest-example)

            - A GearsBuilder or GearFunction object. Notes:
                * Python version must match the Gear runtime.
                * If the function is not "closed" with a `run()` or `register()`
                operation, an `run()` operation without arguments will be assumed,
                and automatically added to the function to close it.
                * The default for `enforce_redgrease` is True.

            - A file path to a gear script. This script can

    Returns:
        Tuple[str: Dict[str,Any]]
            A tuple consting of the serialized function, and a dictionary of contexual
            information about the function.
    """  # noqa
    ctx: Dict[str, Any] = {}

    if isinstance(gear_function, redgrease.runtime.GearsBuilder):
        gear_function = gear_function._function

    if isinstance(gear_function, redgrease.gears.GearFunction):
        # If the input is a GearFunction, we get the requirements from it,
        # and ensure that redgrease is included
        ctx["requirements"] = gear_function.requirements
        ctx["enforce_redgrease"] = True

        if isinstance(gear_function, redgrease.gears.PartialGearFunction):
            # If the function isn't closed with either 'run' or 'register'
            # we assume it is meant to be closed with a 'run'
            gear_function = gear_function.run()

        function_string = seralize_gear_function(gear_function)

        # Special case for CommandReader functions:
        # return a new function that calls its "trigger" and returns the results.
        if gear_function.reader == "CommandReader":

            ctx["trigger"] = gear_function.operation.kwargs["trigger"]

    elif os.path.exists(gear_function):
        # If the gear function is a fle path,
        # then we load the contents of the file
        with open(gear_function) as script_file:
            function_string = script_file.read()

    else:
        # Otherwise we default to the string version of the function
        function_string = str(gear_function)

    return function_string, ctx
