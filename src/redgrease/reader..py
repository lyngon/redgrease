from typing import Iterable, Union

from packaging.version import Version

import redgrease.runtime


class ReaderBase(redgrease.runtime.GearsBuilder):
    def __init__(
        self,
        *args,
        requirements: Iterable[str] = None,
        require_runtime: Union[Version, str, bool] = True,
        **kwargs,
    ):
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

        super().__init__(*args, **kwargs)
