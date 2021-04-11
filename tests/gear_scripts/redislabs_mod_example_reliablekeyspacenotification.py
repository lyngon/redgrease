from redgrease import KeysReader, cmd

KeysReader().foreach(lambda x: cmd.xadd("notifications-stream", x)).register(
    prefix="person:*", eventTypes=["hset", "hmset"], mode="sync"
)
