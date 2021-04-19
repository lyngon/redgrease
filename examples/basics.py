from operator import add

import redgrease

# Create connection / client for single instance Redis
r = redgrease.RedisGears()

# # Normal Redis Commands
# Clearing the database
r.flushall()

# String operations
r.set("Foo-fighter", 2021)
r.set("Bar-fighter", 63)
r.set("Baz-fighter", -747)

# Hash Opertaions
r.hset("noodle", mapping={"spam": "eggs", "meaning": 8})
r.hincrby("noodle", "meaning", 34)

# Stream operations
r.xadd("transactions:0", {"msg": "First", "from": 0, "to": 2, "amount": 1000})


# # Redis Gears Commands
# Get Statistics on the Redis Gears Python runtime
gears_runtime_python_stats = r.gears.pystats()
print(f"Gears Python runtime stats: {gears_runtime_python_stats}")

# Get info on any registered Gear functions, if any.
registered_gear_functions = r.gears.dumpregistrations()
print(f"Registered Gear functions: {registered_gear_functions}")

# Execute nothing as a Gear function
empty_function_result = r.gears.pyexecute()
print(f"Result of nothing: {empty_function_result}")

# Execute a Gear function string that just iterates through and returns the key-space.
all_records_gear = r.gears.pyexecute("GearsBuilder('KeysReader').run()")
print("All-records gear results: [")
for result in all_records_gear:
    print(f"  {result}")
print("]")

# Gear function string to count all the keys
key_count_gearfun_str = "GearsBuilder('KeysReader').count().run()"
key_count_result = r.gears.pyexecute(key_count_gearfun_str)
print(f"Total number of keys: {int(key_count_result)}")


# # GearFunctions
# GearFunction object to count all keys
key_count_gearfun = redgrease.KeysReader().count().run()
key_count_result = r.gears.pyexecute(key_count_gearfun)
print(f"Total number of keys: {key_count_result}")


# Simple Aggregation
add_gear = redgrease.KeysReader("*-fighter").values().map(int).aggregate(0, add)
simple_sum = add_gear.run(on=r)
print(f"Multiplication of '-fighter'-keys values: {simple_sum}")
