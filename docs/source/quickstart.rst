.. include :: banner.rst

.. _quickstart:

Qickstart Guide
===============

This section aims to get you started within a few couple of minutes, while still explaining what is going on, so that someone with only limited experience with Python can follow along.

**TL;DR**

.. code-block:: console

    docker run --name redis_gears --rm -d -p 127.0.0.1:6379:6379 redislabs/redisgears:1.0.6

    python3.7 -m venv .venv
    source .venv/bin/activate

    pip install redgrease[all]

:ref:`Jump straight to the first code examples <quick_examples>`.
    

.. _quick_running_gears_server:

Running Redis Gears
-------------------

The easiest way to run a Redis Engine with Redis Gears is by running one of the offical Docker images, so firstly make sure that you have `Docker engine <https://docs.docker.com/engine/install/>`_ installed.

(Eh? I've been living under a rock. `What the hedge is Docker? <https://docker-curriculum.com/>`_) 

With Docker is installed, open a terminal or command prompt and enter:

.. code-block:: console

    docker run --name redis_gears --rm -d -p 127.0.0.1:6379:6379 redislabs/redisgears:1.0.6

This will run a single Redis engine, with the Redis Gears module loaded, inside a Docker container.
The first time you issue the command it may take a little time to lauch, as Docker needs to fetch the container image from `Docker Hub <https://hub.docker.com/r/redislabs/redisgears>`_.


Lets break the command down:

- ``docker run`` is the base command telling Docker that we want to run a new containerized process.
- ``--name redis_gears`` gives the name "redis_gears" to the container for easier identification. You can change it for whatever you like or omit it to assign a randomized name.
- ``--rm`` instucts Docker that we want the container to be removed, in case it stops. This is optional but makes starting and stopping easer, although the state (and stored data) will be lost between restarts.
- ``-d`` instructs Docker to run the contaioner in the background as a 'daemon'. You can omit this too, but your terminal / command prompt will be hijacked by the output / logs from the container. Which could be interesting enough.
- ``-p 127.0.0.1:6379:6379`` instructs Docker that we want your host computer to locally (127.0.0.1) expose port 6379 and route it to 6379 in the container. This is the default port for Redis communication and this argument is necessary for application on your computer to be able to talk to the Redis engine inside the Docker container.
- ``redislabs/redisgears:1.0.6`` is the name and version of the Docker image that we want to run. This specific image is prepared by RedisLabs and has the Gears module pre-installed and configured.

.. _quick_running_gears_cluster:

.. note:: 

    If its your first time trying Redis Gears, stick to the command above, but if you want to try runing a `cluster image <https://hub.docker.com/r/redislabs/rgcluster>`_ instead, you can issue the following command:

    .. code-block:: console

        docker run --name redis_gears_cluster --rm -d -p 127.0.0.1:3001:30001 -p 127.0.0.1:3002:30002 -p 127.0.0.1:3003:30003 redislabs/rgcluster:1.0.6

    This will run a 3-shard cluster exposed locally on ports 30001-30003.


Refer to the `official documentation <https://docs.redislabs.com/latest/modules/redisgears/installing-redisgears/>`_ for more information and details on how how to install Redis Gears.


Checking Logs:
~~~~~~~~~~~~~~~~~~

You can confirm that the container is running by inspecting the logs / output of the Redis Gears container by issuing the command:

.. code-block:: console

    docker logs redis_gears

- You can optionally add the argument ``--follow`` to continously follow the log output.

- You can optionally add the argument ``--tail 100`` to start showing the logs from the 100 most recent entries only.

If you just started the single instance engine, the logs should hold 40 odd lines starting and ending something like this:

.. code-block:: console

    1:C 03 Apr 2021 07:41:37.250 # oO0OoO0OoO0Oo Redis is starting oO0OoO0OoO0Oo
    1:C 03 Apr 2021 07:41:37.251 # Redis version=6.0.1, bits=64, commit=00000000, modified=0, pid=1, just started
    ... 
    ... 
    ...
    1:M 03 Apr 2021 07:41:37.309 * Module 'rg' loaded from /var/opt/redislabs/lib/modules/redisgears.so
    1:M 03 Apr 2021 07:41:37.309 * Ready to accept connections


Stopping
~~~~~~~~
You can stop the container by issuing:

.. code-block:: console

    docker stop redis_gears

If successful, it should simply output the name of the stopped container: ``redis_gears``


.. _quick_installation:

RedGrease Installation
----------------------

For the client application environment, it is strongly recommended that you set up a virtual Python environment, with `Python 3.7 <https://www.python.org/downloads/release/python-379/>`_ specifically.


.. note:: 

    The Redis Gears containers above use Python 3.7 for executing Gear functions, and using the same version the client application will enable more features.


.. warning::

    The RedGrease client package works with any Python version from 3.6 and later, but :ref:`execution of dynamically created GearFunction objects <exe_gear_function_obj>` is only possible when the client Python version match the Pyhon version on the Redis Gears server runtime.

    If the versions mismatch, Gear function execution is limited to :ref:`execution by string <exe_gear_function_str>` or :ref:`execution of script files <exe_gear_function_file>`

With Python 3.7 installed on your system:

#. Create a virtual environment

    .. code-block:: console

        python3.7 -m venv .venv

    Python packages, including RedGrease that you install within this virtual environment will not interfere with the rest of your system.

#. Activate the environment 

    .. tabs::

        .. tab:: Linux, BSD, OSX and POSIX

            .. code-block:: console

                source .venv/bin/activate

        .. tab:: The Windows

            .. code-block:: console

                .venv\Scripts\activate.bat

        
#. Install redgrease

    .. code-block:: console

        pip install redgrease[all]

    .. note:: 

        The ``[all]`` portion is important, as it will include all the Redgrease extras, and include the dependencies for the RedisGears client module as well as the RedGrease Command Line Intreface (CLI).

        See :ref:`here <adv_extras>` for more details on the various extras options.


.. _quick_examples:
.. _quick_basics:

Basic Commands
--------------

In this section we'll walk through some of the basic commands and interactions with the RedGrease Gears client, including executing some very basic Gear functions.

The next chapter ":ref:`Redgrease Client <client>`", goes into each command in more details, but for now we'll just look at the most important thing.

You can take a sneak-peek at the full code we'll walk through in this section, by expanding the block below (click "▶ Show").

If you find this rather selfself-explanatory, then you can probably jump directly to the next section where we :ref:`compare examples of Gear functions, with and without using RedGrease <quick_example_gears_comparisons>`.

Otherwise just contiue reading and well, go it through step-by-step.

.. container:: toggle

    .. container:: header

        Full code of this section.

    .. literalinclude::  ../../examples/basics.py
        :caption: From examples/basics.py on the official `GitHub repo <https://github.com/lyngon/redgrease>`_:
        :linenos:

|br|
Let's look at some code examples of how to use RedGrease, warming up with the basics.

Instantiation
~~~~~~~~~~~~~

Naturally, the first thing is to import of the RedgGrease package and instantiate  Redis Gears client / connection object

.. literalinclude:: ../../examples/basics.py
    :end-before: # Normal Redis Commands
    :caption: Package import and client / connection instantiation:
    :lineno-match:

This will attempt to connect to a Redis server using the default port (6379) on "localhost", which, if you followed the instructions above should be exactly what you set up and have running. There are of course arguments to set other targets, but more on that later.

The imported ``add`` function from the ``operator`` module is not part of the RedgGrease package, but we will use it later in one of the examples.

.. note:: 

    If you created :ref:`a Redis cluster <quick_running_gears_cluster>` above then you have to specify the initial master nodes you want to connect to:

    .. code-block:: python
        :linenos:

        import redgrease

        r = redgrease.RedisGears(
                startup_nodes=[
                {"host":"localhost", "port":30001},
                {"host":"localhost", "port":30002},
                {"host":"localhost", "port":30003},
            ]
        )


Redis Commands
~~~~~~~~~~~~~~

The instantiated client / connection, ``r``, accepts all the normal Redis commands, exactly as expected. 
The subsequent lines populate the Redis instance it with some data.

.. literalinclude:: ../../examples/basics.py
    :start-after: # Normal Redis Commands
    :end-before: # Redis Gears Commands
    :caption: Some normal Redis commands:
    :lineno-match:


Gears Commands
~~~~~~~~~~~~~~

The client / connection also has a ``gears`` attribute that gives access to Redis Gears specific commands.

.. literalinclude:: ../../examples/basics.py
    :language: python
    :lineno-match:
    :start-after: # Redis Gears Commands
    :end-before: # GearFunctions
    :caption: Some Redis Gears commands:    
    :emphasize-lines: 2,6,10,14,21-22

The highligthed lines show the commands ``pystats``, ``dumpregistrations`` and ``pyexecute`` respectively and the output sould look something like this:

.. code-block:: console
    
    Gears Python runtime stats: PyStats(TotalAllocated=41275404, PeakAllocated=11867779, CurrAllocated=11786368)
    Registered Gear functions: []
    Result of nothing: True
    All-records gear results: [
        b"{'event': None, 'key': 'Baz-fighter', 'type': 'string', 'value': '-747'}"
        b"{'event': None, 'key': 'Bar-fighter', 'type': 'string', 'value': '63'}"
        b"{'event': None, 'key': 'transactions:0', 'type': 'unknown', 'value': None}"
        b"{'event': None, 'key': 'Foo-fighter', 'type': 'string', 'value': '2021'}"
        b"{'event': None, 'key': 'noodle', 'type': 'hash', 'value': {'meaning': '42', 'spam': 'eggs'}}"
    ]
    Total number of keys: 5

The command ``pystats`` gets some memory usage statistics about the Redis Gears Python runtime environment on the server.

The command ``dumpregistrations`` gets information about any registered Gears functions, in this cas none.

And finally, the commmand ``pyexecute`` is the most important command, which sends a Gears function to the server for execution or registration.
In the above example, we are invoking it three times:

- Firstly (line 31) - We pass nothing, i.e. no function at all, which naturally doesn't do anything, but is perfectly valid, and the call thus just returns ``True``.

- Secondly (line 35) - We pass a Gear function string, that reads through the Redis keys (indicated by the ``KeysReader``) and just returns the result by running the function as a batch job (indicated by the ``run()`` operation). The result is consequently a list of dicts, representing the keys and their respective values and types in the Redis keyspace, I.e. the keys we added just before.

- Thirdly (lines 42-43) - We pass a very similar function, but with an additional ``count()`` operation, which is a Gear operation that simply aggregates and counts the incoming records, in this case all key-space records on the server. The result is simply the number or keys in the database: ``5``.

There are other Gears commands too, and the next chapter, ":ref:`Redgrease Client <client>`", will run through them, and the full specifications can be found in the API reference documentation on the :class:`Gears <.Gears>` class.

GearFunctions
~~~~~~~~~~~~~

Composing Gear functions by using strings is not at all very practical, so RedGrease provides a more convenient way of constructing Gear funtctions programmatically, using ``GearFunction`` objects. 


.. literalinclude:: ../../examples/basics.py
    :start-after: # GearFunctions
    :end-before: Simple Aggregation 
    :caption: GearFunction objects instead of strings:
    :lineno-match:
    :emphasize-lines: 2

This Gear function does the same thing as the last function of the previous example, but instead of being composed by a string, it is composed programatically using RedGrease's GearFunction objects, in this case using the :class:`KeysReader <.KeysReader>` class.

The output is, just as expected:

.. code-block:: console

    Total number of keys: 5

.. warning:: 
    Note that execution of GearFunction objects only work if your local Python environment version matches the version on the Redis Gear server, i.e. Python 3.7.

    If the versions mismatch, Gear function execution is limited to :ref:`execution by string <exe_gear_function_str>` or :ref:`execution of script files <exe_gear_function_file>`

The final basic example shows a GearFunction that has a couple of operations stringed together.

.. literalinclude:: ../../examples/basics.py
    :start-after: Simple Aggregation 
    :caption: Simple aggreation - Add keyspace values:
    :lineno-match:

This Gear function adds the values of all the simple keys, with names ending in "-fighter", which were the first three keys created in the example.

And indeed, the result is:

.. code-block:: console

    Sum of '-fighter'-keys values: 1337

Here is a quick run down of how it works: 

- Firstly, the :class:`KeysReader <.KeysReader>` is parameterized with a key pattern ``*-fighter`` meaning it will only read the matching keys.
- Secondly, the ``map()`` operation uses a simple lambda function, to lift out the ``value`` and ensure it is an integer, on each of the keys.
- Thirdly, the ``aggregate()`` operation is used to add the values together, using the imporded ``add`` function, starting with the value 0.
- Lastly, the ``run()`` operation is used te run the function as a batch job. The ``on`` argument states that we want to run it immediately on our client / connection, ``r``.


The chapter :ref:`Readers <readers>` will go through the various types of readers, ant the chapter :ref:`Operations <operations>` will go through the various types of operations, and how to use them.


.. _quick_example_gears_comparisons:

RedgGrease Gear Function Comparisons
------------------------------------

Now let's move on to some more examles of  smaller Gear functions, before we move on to some more elaborate examples.

The examples in this section are basically comparisons of how the examples in the `official Gears documentation <https://oss.redislabs.com/redisgears/master/examples.html>`_, could be simplified by using RedGrease.

.. note::
    RedGrease is backwards compatible with the "vanilla" syntax and structure, and all versions below are still perfectly valid Gear functions when executing using RedGrease.


Word Count
~~~~~~~~~~
Counting of words.

Assumptions
...........
All keys store Redis String values. Each value is a sentence. 

Vanilla Version
...............

This is the the `'Word Count' example from the official RedisGears documentation <https://oss.redislabs.com/redisgears/master/examples.html#word-count>`_.

.. literalinclude:: ../../tests/gear_scripts/redislabs_example_wordcount.py
    :caption: Vanlilla - Word Count

RedGrease Version
.................

This is an example of how the same Gear function could be rewritten using RedgGrease.

.. literalinclude:: ../../tests/gear_scripts/redislabs_mod_example_wordcount.py
    :caption: RedGrease - Word Count


Delete by Key Prefix
~~~~~~~~~~~~~~~~~~~~
Deletes all keys whose name begins with a specified prefix and return their count.

Assumptions
...........
There may be keys in the database. Some of these may have names beginning with the "delete_me:" prefix.

Vanilla Version
...............

This is the the `'Delete by Key Prefix' example from the official RedisGears documentation <https://oss.redislabs.com/redisgears/master/examples.html#delete-by-key-prefix>`_.

.. literalinclude:: ../../tests/gear_scripts/redislabs_example_deletebykeyprefix.py
    :caption: Vanlilla - Delete by Key Prefix

RedGrease Version
.................

This is an example of how the same Gear function could be rewritten using RedgGrease.

.. literalinclude:: ../../tests/gear_scripts/redislabs_mod_example_deletebykeyprefix.py
    :caption: RedGrease - Delete by Key Prefix


Keyspace Notification Processing
~~~~~~~~~~~~~~~~~~~~
Copy every new message from a Redis Stream to a Redis Hash key. 

Assumptions
...........
An input Redis Stream is stored under the "mystream" key. 

Vanilla Version
...............

This is the the `'Keyspace Notification Processing' example from the official RedisGears documentation <https://oss.redislabs.com/redisgears/master/examples.html#basic-redis-stream-processing>`_.

.. literalinclude:: ../../tests/gear_scripts/redislabs_example_basicredisstreamprocessing.py
    :caption: Vanlilla - Basic Redis Stream Processing

RedGrease Version
.................

This is an example of how the same Gear function could be rewritten using RedgGrease.

.. literalinclude:: ../../tests/gear_scripts/redislabs_mod_example_basicredisstreamprocessing.py
    :caption: RedGrease - Basic Redis Stream Processing



Automatic Expiry
~~~~~~~~~~
Sets the time to live (TTL) for every updated key to one hour. 

Assumptions
...........
None. 

Vanilla Version
...............

This is the the `'Automatic Expiry' example from the official RedisGears documentation <https://oss.redislabs.com/redisgears/master/examples.html#automatic-expiry>`_.

.. literalinclude:: ../../tests/gear_scripts/redislabs_example_automaticexpiry.py
    :caption: Vanlilla - Automatic Expiry

RedGrease Version
.................

This is an example of how the same Gear function could be rewritten using RedgGrease.

.. literalinclude:: ../../tests/gear_scripts/redislabs_mod_example_automaticexpiry.py
    :caption: RedGrease - Automatic Expiry


Keyspace Notification Processing
~~~~~~~~~~
This example demonstrates a two-step process that:

#. Synchronously captures distributed keyspace events
#. Asynchronously processes the events' stream


Assumptions
...........
The example assumes there is a ``process`` funtion defined, that does the actual processing of the deleted records. For the purpose of the exaple we can assume that it just outputs the name of the expired keys to the Redis logs, as follows:

.. literalinclude:: ../../tests/gear_scripts/redislabs_example_keyspacenotificationprocessing.py
    :lines: -9

Vanilla Version
...............

This is the the `'Keyspace Notification Processing' example from the official RedisGears documentation <https://oss.redislabs.com/redisgears/master/examples.html#keyspace-notification-processing>`_.

.. literalinclude:: ../../tests/gear_scripts/redislabs_example_keyspacenotificationprocessing.py
    :caption: Vanlilla - Keyspace Notification Processing
    :lines: 11-

RedGrease Version
.................

This is an example of how the same Gear function could be rewritten using RedgGrease.

.. literalinclude:: ../../tests/gear_scripts/redislabs_mod_example_keyspacenotificationprocessing.py
    :caption: RedGrease - Keyspace Notification Processing
    :lines: 1, 2, 14-


Reliable Keyspace Notification
~~~~~~~~~~
Capture each keyspace event and store to a Stream.

Assumptions
...........
...

Vanilla Version
...............

This is the the `'Reliable Keyspace Notification' example from the official RedisGears documentation <https://oss.redislabs.com/redisgears/master/examples.html#reliable-keyspace-notification>`_.

.. literalinclude:: ../../tests/gear_scripts/redislabs_example_reliablekeyspacenotification.py
    :caption: Vanlilla - Reliable Keyspace Notification

RedGrease Version
.................

This is an example of how the same Gear function could be rewritten using RedgGrease.

.. literalinclude:: ../../tests/gear_scripts/redislabs_mod_example_reliablekeyspacenotification.py
    :caption: RedGrease - Reliable Keyspace Notification
    :emphasize-lines: 4


.. Distributed Monte Carlo to Estimate *π*
.. ~~~~~~~~~~
.. Estimate pi by throwing darts at a carefully-constructed dartboard.

.. .. warning::
..     **There are fare better way to get the value of π**

..     This example is intended for educational purposes only. For all practical purposes, you'd be better off using the constant value 3.14159265359.

.. Assumptions
.. ...........

.. The following two functions are defined, as they are the same in both versions:

.. .. literalinclude:: ../../tests/gear_scripts/redislabs_example_distributedmontecarlotoestimatepi.py
..     :caption: Common - Distributed Monte Carlo to Estimate *π*
..     :lines: 1-11, 28-34

.. Vanilla Version
.. ...............

.. This is the the `'Distributed Monte Carlo to Estimate pi' example from the official RedisGears documentation <https://oss.redislabs.com/redisgears/master/examples.html#distributed-monte-carlo-to-estimate-pi>`_.

.. .. literalinclude:: ../../tests/gear_scripts/redislabs_example_distributedmontecarlotoestimatepi.py
..     :caption: Vanlilla - Distributed Monte Carlo to Estimate *π*
..     :lines: 12-26, 36-

.. RedGrease Version
.. .................

.. This is an example of how the same Gear function could be rewritten using RedgGrease.

.. .. literalinclude:: ../../tests/gear_scripts/redislabs_mod_example_distributedmontecarlotoestimatepi.py
..     :caption: RedGrease - Distributed Monte Carlo to Estimate *π*
..     :lines: 1, 12-26, 36-
..     :emphasize-lines: 8


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

    .. literalinclude:: ../../examples/cache_get_command.py
        :caption: Simple Caching command:

|br|
Let's go throug it step by step, and it will hopefully be very clear.

.. literalinclude:: ../../examples/cache_get_command.py
    :end-before: CommandReader Decorator
    :caption: Instantiation as usual:
    :lineno-match:

The instantiatoion of the client / connection is busienss as usual.

Cache-Get function
~~~~~~~~~~~~~~~~~~
Lets now go for the meat of the solution; The code that we want to run on Redis for each resource request.

.. literalinclude:: ../../examples/cache_get_command.py
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

.. literalinclude:: ../../examples/cache_get_command.py
    :start-after: CommandReader Decorator
    :lines: -10
    :caption: CommandReader function decorator:
    :emphasize-lines: 3
    :lineno-match:

All the Redis Gears magic is hidden in this function decorator, and it does two important things:
- It ensures that the function contents is redgistered as a ``CommandReader`` Gear function on Redis.
- That the resulting decorated function, when called, triggers the corresponding registered Gear function, and returns the result.

This means that you can now call the decorated function, just as if it was a local function:

.. code-block:: python

    result = cache_get("http://images.cocodataset.org/train2017/000000169188.jpg")

This may look like it is actually executing the function locally, but the ``cache_get`` function is actually executed on the server.

Note that the registered ``cache_get`` Gear function can not only be triggered by the client that defined the decorated function, but by any client by invoking the Redis Gear ``RG.TRIGGER <https://oss.redislabs.com/redisgears/commands.html#rgtrigger>``_ command with the the functions' trigger name and arguments.

The arguments for the :function:`redgreaes.trigger <.trigger>` decorator, are the same as to the :method:``register <.CommandReader.register>`` method of the :class:`CommandReader <.CommandReader>` class.

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

.. literalinclude:: ../../examples/cache_get_command.py
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

.. literalinclude:: ../../examples/cache_get_command.py
    :start-after: Clean the database
    :caption: Clean up the database: 
    :lineno-match:

That wraps up the Quickstart Guide! Good luck building Gears! 

.. include :: footer.rst

.. |br| raw:: html

    <br />