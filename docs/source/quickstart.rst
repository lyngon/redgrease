.. include :: banner.rst

.. _quickstart:

Qickstart Guide
===============

.. include :: wip.rst

.. _quick_running_gears_server:

Running Redis Gears
-------------------

.. _quick_installation:

RedGrease Installation
----------------------

.. _quick_basics:

Basics
------

.. _quick_example_query:

Keyspace Batch Query
--------------------

.. _quick_example_event_proc:

Event Processing
----------------

.. _quick_example_command:

Custom Commands
---------------

.. code-block::

    import redgrease

    # This will run on the Redis Gears server
    @redgrease.trigger(convertToStr=False, requirements=["requests"])
    def cache_get(url):
        
        # If the cached value exists, return it
        if redgrease.cmd.exists(url):
            return bytes(redgrease.cmd.get(url))
        
        # Otherwise fetch it remotely, store in cache, and return
        response = requests.get(url)

        if response.status_code != 200:
            return bytes()
        
        response_data = bytes(response.content)
        redgrease.cmd.set(url, response_data)
        
        return response_data

    # Bind / register the function on some Redis instance.
    r = redgrease.RedisGears()
    cache_get.on(r) 

    # When a client attempts to get some resource from url, it need only to do one call
    # This will invoke the cache_get function **on the Redis server**
    resource = cache_get(some_url)

    # Alternatively
    resource = r.gears.trigger("cache_get", some_url)

.. include :: footer.rst