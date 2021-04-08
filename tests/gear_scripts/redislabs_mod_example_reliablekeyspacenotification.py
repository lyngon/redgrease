from redgrease import KeysReader, cmd

KeysReader().foreach(
    lambda x: cmd.xadd(
        "notifications-stream", "*", *sum([[k, v] for k, v in x.items()], [])
    )
).register(prefix="person:*", eventTypes=["hset", "hmset"], mode="sync")
