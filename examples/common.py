import redgrease


# # Cleanup
def cleanup(r: redgrease.RedisGears):

    # Unregister all registrations
    for reg in r.gears.dumpregistrations():
        r.gears.unregister(reg.id)

    # Remove all executions
    for exe in r.gears.dumpexecutions():
        r.gears.dropexecution(str(exe.executionId))

    # Clear all keys
    r.flushall()

    # Check that there are no keys
    return len(r.keys()) == 0
