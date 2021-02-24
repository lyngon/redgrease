import redgrease.client
import redgrease.data
import redgrease.reader
import redgrease.utils


def read_user_permissions(record) -> dict:
    return redgrease.cmd.hget(
        record.key,
        mapping={
            "active": bool,
            "permissions": redgrease.utils.list_parser(str),
        },
    )


# Partial Gear function, w. default run param:
active_users = (
    redgrease.reader.KeysOnlyReader("user:*")
    .map(redgrease.data.Record.from_redis)
    .map(read_user_permissions)
    .filter(lambda usr: usr["active"])
)

# Partial Gear function re-use:
active_user_count = active_users.count()

all_issued_permissions = active_users.flatmap(lambda usr: usr.permissions).distinct()

# Redis Client w. Gears
r = redgrease.client.RedisGears()

# Two ways of running:
count = r.gears.pyexecute(active_user_count.run())
permissions = all_issued_permissions.run().on(r)

print(f"Count: {count}")
print(f"Permissions: {permissions}")
