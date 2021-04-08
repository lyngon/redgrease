from redgrease import KeysReader, cmd

expire = KeysReader().keys().foreach(lambda x: cmd.expire(x, 3600))
expire.register("*", mode="sync", readValue=False)
