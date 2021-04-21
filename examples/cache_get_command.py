import timeit

import redgrease

# Bind / register the function on some Redis instance.
r = redgrease.RedisGears()


# CommandReader Decorator
# The `command` decorator tunrs the function to a CommandReader,
# registerered on the Redis Gears sever if using the `on` argument
@redgrease.command(on=r, requirements=["requests"], replace=False)
def cache_get(url):
    import requests

    # Check if the url is already in the cache,
    # And if so, simply return the cached result.
    if redgrease.cmd.exists(url):
        return bytes(redgrease.cmd.get(url))

    # Otherwise fetch the url.
    response = requests.get(url)

    # Return nothing if request fails
    if response.status_code != 200:
        return bytes()

    # If ok, set the cache data and return.
    response_data = bytes(response.content)
    redgrease.cmd.set(url, response_data)

    return response_data


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
