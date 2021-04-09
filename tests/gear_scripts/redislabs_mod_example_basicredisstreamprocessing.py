from redgrease import StreamReader, cmd

StreamReader().foreach(
    lambda x: cmd.hmset(x["streamId"], *x)  # write to Redis Hash
).register("mystream")
