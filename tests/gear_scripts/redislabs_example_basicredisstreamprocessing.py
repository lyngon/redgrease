gb = GearsBuilder("StreamReader")
gb.foreach(
    lambda x: execute("HMSET", x["id"], *sum([[k, v] for k, v in x.items()], []))
)  # write to Redis Hash
gb.register("mystream")
