from typing import Callable, Dict

import redgrease
import redgrease.client
import redgrease.reader
import redgrease.runtime
import redgrease.utils
from redgrease.typing import Constructor

mapping = {
    "active": bool,
    "permissions": redgrease.utils.list_parser(str),
}


def hash_to_dict(mapping: Dict[str, Constructor]) -> Callable[[str], Dict]:
    def parser(hash_key: str) -> Dict:
        vals = redgrease.cmd.hmget(hash_key, mapping.keys())
        return redgrease.utils.dict_of(mapping)(vals, mapping.keys())

    return parser


# Partial Gear function, w. default run param:
active_users = (
    redgrease.reader.KeysOnlyReader("user:*")
    .map(hash_to_dict(mapping))
    .filter(lambda usr: usr["active"])
)

# Partial Gear function re-use:
active_user_count = active_users.count()

all_issued_permissions = active_users.flatmap(lambda usr: usr["permissions"]).distinct()

# Redis Client w. Gears
r = redgrease.client.RedisGears()
for usr_id in range(100):
    r.hset(
        f"user:{usr_id}",
        mapping={
            "name": f"mr {usr_id}",
            "id": usr_id,
            "active": int(bool(usr_id % 3)),
            "permissions": str(["eat", "sleep"]),
        },
    )

# Two ways of running:
count = r.gears.pyexecute(active_user_count.run())
permissions = all_issued_permissions.run().on(r)

print(f"Count: {count.results[0]}")
print(f"Permissions: {permissions.results}")
