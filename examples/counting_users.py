import redis

# # Begin Example
import redgrease
import redgrease.utils

relevant_usr_fields = {
    "active": bool,
    "permissions": redgrease.utils.list_parser(str),
}

# # Open Gear function
# Extracting a dict for every 'active' user
active_users = (
    redgrease.KeysOnlyReader()
    .map(lambda key: redgrease.cmd.hmget(key, *relevant_usr_fields.keys()))
    .map(
        lambda udata: redgrease.utils.to_dict(
            udata, keys=relevant_usr_fields.keys(), val_transform=relevant_usr_fields
        )
    )
    .filter(lambda usr: usr["active"])
)
# # Open Gear function re-use
# Count the number of active users
active_user_count = active_users.count()

# Get all the distinct user permissions
all_issued_permissions = active_users.flatmap(lambda usr: usr["permissions"]).distinct()

# # Redis Client w. Gears-features can be created separately
r = redgrease.RedisGears()

# # Two ways of running:
# With 'pyexecute' ...
count = r.gears.pyexecute(active_user_count.run("user:*"))
# ... or using the 'on' argument
permissions = all_issued_permissions.run("user:*").on(r)

# Result values are directly accessible
print(f"Count: {count}")
if count > 100:
    print("So many users!")
print(permissions)
if "root" in permissions:
    print("Someone has root permissions")

# Errors can be accessed too
if count.errors:
    print(f"Errors counting users: {count.errors}")
if permissions.errors:
    print(f"Errors collecting permissions: {permissions.errors}")
# # End Example


red = redis.Redis()
for i, name in enumerate(
    ["Alice", "Bob", "Charlie", "Daniel", "Emma", "Felx", "Greta", "Henry", "Igrid"]
):
    perms = ["read"]
    perms.append("comment" if i % 3 else "edit")
    perms.append("share" if i % 7 else "root")
    red.hset(  # type: ignore
        f"user:{i}",
        mapping={
            "name": name,
            "active": i % 5,
            "permissions": str(perms),
            "data": "...",
        },
    )
