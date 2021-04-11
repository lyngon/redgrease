from redgrease import StreamReader, cmd

StreamReader().foreach(lambda x: cmd.hmset(x["id"], x)).register(  # write to Redis Hash
    "mystream"
)
