from redgrease import StreamReader, cmd

proc = StreamReader().foreach(
    lambda x: cmd.hmset(x["streamId"], *x)  # write to Redis Hash
)

proc.register("mystream")
