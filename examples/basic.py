import redgrease
import redgrease.utils

user_structure = {
    "active": bool,
    "permissions": redgrease.utils.list_parser(str),
}

# Partial Gear function, w. default run param:
active_users = (
    redgrease.KeysOnlyReader("user:*")
    .map(lambda key: redgrease.cmd.hmget(key, *user_structure.keys()))
    .map(
        lambda udata: redgrease.utils.to_dict(
            udata, keys=user_structure.keys(), val_transform=user_structure
        )
    )
    .filter(lambda usr: usr["active"])
)
# Partial Gear function re-use:
active_user_count = active_users.count()

all_issued_permissions = active_users.flatmap(lambda usr: usr["permissions"]).distinct()

# Redis Client w. Gears
r = redgrease.client.RedisGears()

# Two ways of running:
count = r.gears.pyexecute(active_user_count.run())
permissions = all_issued_permissions.run().on(r)

print(f"Count: {count.results}")
if count.errors:
    print(count.errors)

print(f"Permissions: {permissions.results}")
if permissions.errors:
    print(permissions.errors)
