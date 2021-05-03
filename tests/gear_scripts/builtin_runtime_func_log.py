from redgrease import GearsBuilder, log


def log_keys(key):
    log(f"There is a key: {key}")


gb = GearsBuilder("KeysOnlyReader")
gb.foreach(log_keys).run()
