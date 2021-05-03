import time

from redgrease import GB, atomic, execute


def transaction(_):
    with atomic():
        execute("INCR", "atomic_ops_executing")
        time.sleep(1)
        execute("DEL", "atomic_op_executing")


gb = GB("ShardsIDReader")
gb.foreach(transaction)
gb.run()
