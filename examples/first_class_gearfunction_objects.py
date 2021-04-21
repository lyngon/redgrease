# # Begin Example
import redgrease


def schedule(record):
    status = record.value.get("status", "new")
    redgrease.log(f"Scheduling '{status}' record: {record.key}")
    if status == "new":
        record.value["status"] = "pending"
        redgrease.cmd.hset(record.key, "status", "pending")
        redgrease.cmd.xadd("to_be_processed", {"record": record.key})
    ...
    return record


def process(item):
    redgrease.log(f"processsing {item}")
    success = len(item["record"]) % 3  # Mock processing
    redgrease.cmd.hset(item["record"], "status", "success" if success else "failed")


def has_status(status):
    return lambda record: record.value.get("status", None) == status


key_pattern = "record:*"

records = redgrease.KeysReader().records(type="hash")

record_listener = records.foreach(schedule).register(key_pattern, eventTypes=["hset"])

get_failed = records.filter(has_status("failed"))

count_by_status = (
    records.countby(lambda r: r.value.get("status", "unknown"))
    .map(lambda r: {r["key"]: r["value"]})
    .aggregate({}, lambda a, r: dict(a, **r))
    .run(key_pattern)
)

process_records = (
    redgrease.StreamReader()
    .values()
    .foreach(process, requirements=["numpy"])
    .register("to_be_processed")
)

server = redgrease.RedisGears()

# Different ways of executing
server.gears.pyexecute(record_listener)
process_records.on(server)

failed = get_failed.run(key_pattern, on=server)
count = count_by_status.on(server)
# # End Example

server.hset("record:0", mapping={"foo": 1, "bar": 2})
server.hset("record:1", mapping={"foo": 3, "bar": 5})
server.hset("record:42", mapping={"foo": 19, "bar": 80})  # Will "fail"

print(count)
print(failed.key)

assert len(count) == 2
assert count["success"] == 2
assert count["failed"] == 1
assert failed.key == "record:42"
