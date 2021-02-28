import redgrease
import redgrease.utils

relevant_usr_fields = {
    "active": bool,
    "permissions": redgrease.utils.list_parser(str),
}

# Partial Gear function, w. default run param:
active_users = (
    redgrease.KeysOnlyReader("user:*")
    .map(lambda key: redgrease.cmd.hmget(key, *relevant_usr_fields.keys()))
    .map(
        lambda udata: redgrease.utils.to_dict(
            udata, keys=relevant_usr_fields.keys(), val_transform=relevant_usr_fields
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

# Result values are directly accessible
print(f"Count: {count}")
if count > 100:
    print("So many users!")
print(permissions)
if "root" in permissions:
    print("Someone has root permissions")

# Errors can be acceessed too
if count.errors:
    print(f"Errors counting users: {count.errors}")
if permissions.errors:
    print(f"Errors collecting permissions: {permissions.errors}")
