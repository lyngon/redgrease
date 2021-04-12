from redgrease import KeysReader, cmd

delete_fun = KeysReader().keys().foreach(lambda k: cmd.delete(k)).count()
delete_fun.run("delete_me:*")
