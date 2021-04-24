import redgrease

# # Open Function
# Function that collecs the keys per type
get_keys_by_type = redgrease.KeysReader().aggregateby(
    lambda record: record["type"],
    set(),
    lambda _, acc, rec: acc | set([rec["key"]]),
    lambda _, acc, lacc: acc | lacc,
)
# #

# # Closed Functions
# Transform to a single dict from type to keys
get_keys_by_type_dict = (
    get_keys_by_type.map(lambda record: {record["key"]: record["value"]})
    .aggregate({}, lambda acc, rec: {**acc, **rec})
    .run()
)

# Find the most common key-type
get_commonest_type = (
    get_keys_by_type.map(lambda x: (x["key"], len(x["value"])))
    .aggregate((None, 0), lambda acc, rec: rec if rec[1] > acc[1] else acc)
    .run()
)
# #

# # Method 1
# Execute "closed" GearFunction object with a Gear clients' `pyexecute` method
r = redgrease.RedisGears()

result_1 = r.gears.pyexecute(get_keys_by_type_dict)
# #
"""result_1 =
{
    'hash': {'hash:1', 'hash:0', ...},
    'string': {'string:0', 'string:1', 'string:2', ...}
    ...
}
"""
print("result_1 =")
print(result_1)

# # Method 2
# Execute "closed" GearFunction object with its `on` method
result_2 = get_commonest_type.on(r)
# #
"""result_2 =
("string", 3)
"""
print("# result_2 =")
print(result_2)

# # Method 3
# Execute "open" GearFunction with a Gear clients' `pyexecute` method
result_3 = r.gears.pyexecute(get_keys_by_type)
# #
"""result_3 =
[
    {'key': 'hash', 'value': {'hash:1', 'hash:0', ...}},
    {'key': 'string', 'value': {'string:0', 'string:1', 'string:2', ...}},
    ...
]
"""
print("# result_3 =")
print(result_3)

# # Method 4
# Execute GearFunction using `on` argument in "closing" method
result_4 = get_keys_by_type.run(on=r)
# #
"""result_4 =
[
    {'key': 'hash', 'value': {'hash:1', 'hash:0', ...}},
    {'key': 'string', 'value': {'string:0', 'string:1', 'string:2', ...}},
    ...
]
"""
print("# result_4 =")
print(result_4)
