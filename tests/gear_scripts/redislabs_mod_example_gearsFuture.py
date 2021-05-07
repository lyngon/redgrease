from typing import Dict

import redgrease

blocked: Dict[str, redgrease.gearsFuture] = {}


# Command that will block until the provided key expires
@redgrease.command(mode=redgrease.TriggerMode.Sync)
def WaitForKeyExpiration(key):
    f = redgrease.gearsFuture()
    if key not in blocked.keys():
        blocked[key] = []
    blocked[key].append(f)
    return f


def unblock(key):
    res = 0
    futures = blocked.pop(key, None)
    if futures:
        res = len([f.continueRun(f"{key} expired") for f in futures])
        blocked[key]
    return res


redgrease.KeysReader().keys().map(unblock).register(
    eventTypes=["expired"], mode=redgrease.TriggerMode.Sync
)
