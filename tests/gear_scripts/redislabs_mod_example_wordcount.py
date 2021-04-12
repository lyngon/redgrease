from redgrease import KeysReader

KeysReader().values().flatmap(lambda x: x.split()).countby().run()
