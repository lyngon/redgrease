import redis


def load_users(server: redis.Redis, count=100):
    for usr_id in range(count):
        server.hset(
            f"user:{usr_id}",
            mapping={
                "name": f"mr {usr_id}",
                "id": usr_id,
                "active": int(bool(usr_id % 3)),
                "permissions": str(["eat", "sleep"]),
            },
        )


r = redis.Redis()

load_users(r, 200)
