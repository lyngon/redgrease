from redgrease import GB, execute


def counter(x):
    execute("INCR", "keycount")
    return x


gear = GB()
gear.map(counter)
gear.run()
