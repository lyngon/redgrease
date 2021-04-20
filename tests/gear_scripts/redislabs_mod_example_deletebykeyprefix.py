from redgrease import KeysReader, cmd

delete_fun = KeysReader().keys().foreach(cmd.delete).count()
delete_fun.run("delete_me:*")
