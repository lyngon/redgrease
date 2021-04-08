from redgrease import KeysReader

KeysReader().values().flatmap(str.split).countby().run()
