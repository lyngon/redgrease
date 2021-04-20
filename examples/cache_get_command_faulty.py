import datetime
import timeit

import redgrease

# Bind / register the function on some Redis instance.
r = redgrease.RedisGears()


# CommandReader Decorator
# The `command` decorator tunrs the function to a CommandReader,
# registerered on the Redis Gears sever if using the `on` argument
@redgrease.command(on=r, requirements=["requests"], replace=False)
def cache_get(url, expiry=None, timeout=10):
    import requests

    # Keys
    value_key = f"cache:{{{url}}}:result"  #
    status_key = f"cache:{{{url}}}:status"

    # Atomically check if cache miss, and the number of requestss, this inclusive
    with redgrease.atomic():
        cache_miss = not redgrease.cmd.hexists(value_key, "data")
        requests_count = int(redgrease.cmd.hincrby(value_key, "requests_count", 1))

    redgrease.log(f"Cache request #{requests_count} for resource '{url}'")

    # If there was a cache miss but not the first request for the resource
    # Then it is likely being downloaded by another request:
    # => Block until we get have a status
    # If status is Ok, push the status back to unblock any other blocked requests.
    if cache_miss and requests_count > 1:
        redgrease.log(
            f"Cache miss #{requests_count} - "
            f"Waiting while other request downloads resource '{url}'."
        )
        status = redgrease.cmd.brpop(status_key, status_key, timeout)
        if status == 200:
            redgrease.cmd.lpush(status_key, status_key, timeout)

    # Get the current cached data, if any
    response_data = redgrease.cmd.hget(value_key, "data")

    # We don't blindly trust the status code.
    # If the cached ressource data still does not exist in cache:
    # => then get it frod the url, together with some additional meta-data.
    if response_data is None:
        redgrease.log(f"Cache miss #{requests_count} - Downloading resource '{url}'.")

        response = requests.get(url)

        status = response.status_code
        cache_fields = {
            "status_code": status,
            "timestamp": datetime.datetime.utcnow().isoformat(),
        }

        # We update the data only if the status is ok
        if response.status_code == 200:
            response_data = response.content
            cache_fields["data"] = response_data

        redgrease.cmd.hset(
            value_key,
            mapping=cache_fields,
        )

        redgrease.log(
            f"Cache update #{requests_count} - "
            f"Request status for resource '{url}': {status}"
        )

        # Push staus to unblock any other blocked requests.
        redgrease.cmd.lpush(status_key, status)

    # Update the cache expiry, if set
    if expiry:
        redgrease.cmd.expire(status_key, expiry)
        redgrease.cmd.expire(value_key, expiry)

    # Whatever the status is, push back to unblock any other blocked requests.
    redgrease.cmd.rpoplpush(status_key, status_key)

    # Return the data as raw binary data
    return bytes(response_data)


# Test caching on some images
some_image_urls = [
    "http://images.cocodataset.org/train2017/000000483381.jpg",
    "http://images.cocodataset.org/train2017/000000237137.jpg",
    "http://images.cocodataset.org/train2017/000000017267.jpg",
    "http://images.cocodataset.org/train2017/000000197756.jpg",
    "http://images.cocodataset.org/train2017/000000193332.jpg",
    "http://images.cocodataset.org/train2017/000000475564.jpg",
    "http://images.cocodataset.org/train2017/000000247368.jpg",
    "http://images.cocodataset.org/train2017/000000416337.jpg",
]


# Get all the images and write them to disk
def get_em_all():

    for image_url in some_image_urls:

        # This will invoke the cache_get function **on the Redis server**
        image_data = cache_get(image_url)

        # Quick and dirty way of getting the image file name.
        image_name = image_url.split("/")[-1]

        # Write to file
        with open(image_name, "wb") as img_file:
            img_file.write(image_data.value)


# Test it
# Time how long it takes to get images when the cache is empty.
t1 = timeit.timeit(get_em_all, number=1)
print(f"Cache-miss time: {t1:.3f} seconds")

# Time how long it takes to get the images when they are all in the cache.
t2 = timeit.timeit(get_em_all, number=1)
print(f"Cache-hit time: {t2:.3f} seconds")
print(f"That is {t1/t2:.1f} times faster!")


# Clean the database
def cleanup(r: redgrease.RedisGears):

    # Unregister all registrations
    for reg in r.gears.dumpregistrations():
        r.gears.unregister(reg.id)

    # Remove all executions
    for exe in r.gears.dumpexecutions():
        r.gears.dropexecution(str(exe.executionId))

    # Clear all keys
    r.flushall()

    # Check that there are no keys
    return len(r.keys()) == 0


# print(cleanup(r))
