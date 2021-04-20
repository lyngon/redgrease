.. _quick_example_command:

Cache Get Command
---------------

As a final example of this quickstart tutorial, let's look at how we can build caching into Redis as a new command, with the help of Redis Gears and RedGrease.

Assume that you have multiple worker clients that needs to read resources from urls, like for example images, and that it is a relatively high probability that more than one clients need to read the same image at roughly the same time.

This is a typical scenario where Redis might be used to cache the images, and have each client first check if the resource is already cached, and if so get it from Redis, and if not get it from the source location and then store it into Redis.

This kind of caching can help save a lot to time fetching resources, as we only need to fetch each resource from the external source if it hasn't been fetched ant cached, which ideally only would happen once per image, the firs time it is requested by a client. 

This is great but it suffers from a couple of flaws:

.. _quick_caching_issues:

#. **Unecessary Downloads** There is a possilility that more than one client downloads the resource from the remote source at the same time. This would happen to any requests starting after the first requests but before the resource has been uploaded back to the cache. Such duplicate downloads are not only unecessary, they also compete an parasitize on the network bandwidth to the source.

#. **Caching Latency** - The latency of *inserting* a downloaded resources into the cache is dependnent on the network connection between the client in question and the cache, which may not be ideal, further increasing the probability of unnecessary duplicate downloads of resources from the remote source. 

#. **Logic Duplication** - It requires that all clients to implement the caching logic, which is not ideal if theree are different clients are implemented in different programming languages, as this would mean duplication of code and higher risk of bugs that may affect the whole system.

So ideally, you'd want to have the caching logic in a single place (mitigates 3.), ideally as close to the cache storage as possible (mitigates 2.) and aviod having multiple simultaneous downloads of the same resource from external source (mitigate 1.).

Luckily this is something we can do with Redis Gears, and RedGrease makes it quite easy.

.. container:: toggle

    .. container:: header

        Full code.

    It may look a bit intimidating at first, but theres actually not not that much to it. 
    Most of it is just comments, logging or testing code. 

    .. literalinclude:: ../../examples/cache_get_command_faulty.py
        :caption: Simple Caching command:

|br|
Let's go throug it step by step, and it will hopefully be very clear.

.. literalinclude:: ../../examples/cache_get_command_faulty.py
    :end-before: CommandReader Decorator
    :caption: Instantiation as usual:
    :lineno-match:

The instantiatoion of the client / connection is busienss as usual.

Cache-Get function
~~~~~~~~~~~~~~~~~~
Lets now go for the meat of the solution; The code that we want to run on Redis for each resource request.

.. literalinclude:: ../../examples/cache_get_command_faulty.py
    :lines: 14-
    :end-before: Test caching on some images
    :caption: Cache handling function:
    :emphasize-lines: 1, 29, 37, 47, 72
    :lineno-match:


Ignore the details and just look at the highligthed lines, and notice:

- The logic of handling requests with caching is simply put in a normal function, much like we would if the caching logic was handled by each client.
- The arguments of the function are the ones we could expect, the ``url`` to the resource to get, a cache ``expiry`` duration and a request ``timeout`` duration.
- The function return value is either:
    - The contents of the response to requests to the URL (line 60), or
    - A cached value (line 42)

Which is exactly what you would expect from a cached feching function. 

.. note::
    As mentioned, most of the complexity of the function comes from addressing :ref:`issue 1 <quick_caching_issues>`. If we did not want to address that issue the function might simply look like this:

    .. code-block:: python

        def cache_get(url):
            if redgrease.cmd.exists(url):
                return bytes(redgrease.cmd.get(url))
            
            response = requests.get(url)
            
            if response.status_code != 200:
                return bytes()
            
            response_data = bytes(response.content)
            redgrease.cmd.set(url, response_data)
            
            return response_data

The rest of the logic is mostly for ensuring that only one of the requests trigger the download and storing in the cache, while blocking new requests until the data is stored (or timed out or failed).

The really intersting part, however, is this little line. 

.. literalinclude:: ../../examples/cache_get_command_faulty.py
    :start-after: CommandReader Decorator
    :lines: -10
    :caption: CommandReader function decorator:
    :emphasize-lines: 3
    :lineno-match:

All the Redis Gears magic is hidden in this function decorator, and it does a couple of important things:

- It embeds the function in a ``CommandReader`` Gear function.
- It ensures that the function is redgistered on our Redis server(s).
- It captures the relevant requirements, for the function to work.
- It ensures that we only register this function once. 
- It creates a new function, with the same name that, when called, triggers the corresponding registered Gear function, and returns the result from the server.

This means that you can now call the decorated function, just as if it was a local function:

.. code-block:: python

    result = cache_get("http://images.cocodataset.org/train2017/000000169188.jpg")

This may look like it is actually executing the function locally, but the ``cache_get`` function is actually executed on the server.

This means that the registered ``cache_get`` Gear function can not only be triggered by the client that defined the decorated function, but **can be triggered by any client** by invoking the Redis Gear `RG.TRIGGER <https://oss.redislabs.com/redisgears/commands.html#rgtrigger>`_ command with the the functions' trigger name and arguments. 

In our case, using `redis-cli` as an example:

.. code-block:: console

    > RG.TRIGGER cache_get http://images.cocodataset.org/train2017/000000169188.jpg

The arguments for the :func:`redgrease.command <.command>` decorator, are the same as to the :method:``register <.CommandReader.register>`` method of the :class:`CommandReader <.CommandReader>` class.

.. note:: 

    In the design choices of this particular cache implementation may not be ideal for all use-cases. Fore example:
    - Only the response content data is returned, not response status or headers. 
    - Cache expiry is reset on each redquest.
    - The entire response contents is copied into memory before writing to cache.
    - ... etc ... 

    Naturally, the solution could easily be modified to other behaviors.


Testing the Cache
~~~~~~~~~~~~~~~~~

To test the caching, we create a very simple function that iterates through some URLs and tries to get them from the cache and saving the contents to local files.

.. literalinclude:: ../../examples/cache_get_command_faulty.py
    :start-after: Test caching on some images
    :end-before: Clean the database
    :caption: Test function:
    :emphasize-lines: 19
    :lineno-match:

Calling the this function twice reveals that the caching does indeed seem to work.

.. code-block:: console

    Cache-miss time: 10.954 seconds
    Cache-hit time: 0.013 seconds
    That is 818.6 times faster!

We can also inspect the logs of the Redis node to confirm that the cache function was indeed executed on the server.

.. code-block:: console

    docker logs --tail 100

And you should indeed see that the expected log messages appear:

.. code-block:: console

    1:M 06 Apr 2021 08:58:06.314 * <module> GEARS: Cache request #1 for resource 'http://images.cocodataset.org/train2017/000000416337.jpg'
    1:M 06 Apr 2021 08:58:06.314 * <module> GEARS: Cache miss #1 - Downloading resource 'http://images.cocodataset.org/train2017/000000416337.jpg'.
    1:M 06 Apr 2021 08:58:07.855 * <module> GEARS: Cache update #1 - Request status for resource 'http://images.cocodataset.org/train2017/000000416337.jpg': 200

    ...

    1:M 06 Apr 2021 08:58:07.860 * <module> GEARS: Cache request #2 for resource 'http://images.cocodataset.org/train2017/000000416337.jpg'


.. note:: 

    It is left as an exercise to confirm, or disprove, that the above caching mechanism indeed avoids duplicate downloads of resources requested within a short time window.


The last piece of code is jut to clean up the database by unregistering the ``cache_get`` Gear function, cancel and drom any ongoing Gear function executions and flush the key-space.

.. literalinclude:: ../../examples/cache_get_command_faulty_faulty.py
    :start-after: Clean the database
    :caption: Clean up the database: 
    :lineno-match: