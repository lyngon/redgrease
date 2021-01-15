from redgrease import GearsBuilder, TriggerMode
from redgrease.typing import Callback


def trigger(
    trigger: str,
    prefix: str = "*",
    convertToStr: bool = True,
    collect: bool = True,
    mode: str = TriggerMode.Async,
    onRegistered: Callback = None,
    **kargs
):
    def gear(func):
        GearsBuilder("CommandReader").map(lambda params: func(params[1:])).register(
            prefix=prefix,
            convertToStr=convertToStr,
            collect=collect,
            mode=mode,
            onRegistered=onRegistered,
            trigger=trigger,
            **kargs
        )

    return gear
