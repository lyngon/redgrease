.. include :: banner.rst

.. _intro:

Introduction
============

RedGrease is a Python client and runtime package attempting to make it as easy as possible to create and execute :ref:`RedisGears <intro_redis_gears>` functions on :ref:`Redis <intro_redis>` engines with the :ref:`RedisGears Module <intro_redis_gears>` loaded.

.. figure:: ../images/RedisGears_simple.png
    :width: 400

    Overview

:ref:`RedGrease <intro_redgrease>` makes it easy  to write concise but expressive Python functions to query and/or react to data in Redis in realtime. The functions are automatically distributed and run across the shards of the Redis cluster (if any), providing an excellent balance of performance of distributed computations and expressiveness and power of Python.

It may help you create:

- Advanced analytical queries,
- Event based and streaming data processing,
- Custom Redis commands and interactions,
- And much, much more...

... all written in Python and running distributed ON your Redis nodes.

The Gear functions may include and use third party dependencies like for example ``numpy``, ``requests``, ``gensim`` or pretty much any other Python package distribution you may need for your use-case.


If you are already familiar with Redis and RedisGears, then you can jump directly to the :ref:`intro_redgrease` overview or the :ref:`quickstart`, otherwise you can read on to get up to speed on these technologies.

.. _intro_redis:

What is Redis?
--------------

`Redis`_ is a popular in-memory data structure store, used as a distributed, in-memory, key–value database, cache and message broker, with optional durability, horizontal scaling and high availability.
Redis supports different kinds of abstract data structures, such as strings, lists, maps, sets, sorted sets, HyperLogLogs, bitmaps, streams, and spatial indexes. The project is developed and maintained by `RedisLabs`_. 
It is `open-source <Redis GitHub>`_ software released under a BSD 3-clause license.


.. _intro_redis_gears:

What is Redis Gears?
--------------------

`RedisGears`_  is an official extension module for Redis, also developed by `RedisLabs`_, which allows for distributed Python computations on the Redis server itself.

From the official `RedisGears`_ site:

| *"RedisGears is a dynamic framework that enables developers to write and execute functions that implement data flows in Redis, while abstracting away the data’s distribution and deployment. These capabilities enable efficient data processing using multiple models in Redis with infinite programmability, while remaining simple to use in any environment."*

When the Redis Gears module is loaded onto the Redis engines, the Redis engine command set is extended with new commands to register, distribute, manage and run so called :ref:`Gear Functions <intro_gear_functions>`, written in Python, across across the shards of the Redis database. 

Client applications can define and submit such Python Gear Functions, either to run immediately as 'batch jobs', or to be registered to be triggered on events, such as Redis keyspace changes, stream writes or external triggers. The Redis Gears module handles all the complexities of distribution, coordination, scheduling, execution and result collection and aggregation, of the Gear Functions.

.. figure:: ../images/Gear_Function6_white.png
    :width: 512

    Redis Gears Processing Pipeline Overview


.. _intro_gear_functions:

What are Gear Functions?
~~~~~~~~~~~~~~~~~~~~~~~~

Gear Functions are composed as a sequence of steps, or operations, such as for example Map, Filter, Aggregate, GroupBy and more. 

These operations are parameterized with Python functions, that you define according to your needs.

The steps / operations are 'piped' together by the Redis Gears runtime such that the output of of one step / operation becomes the input to the subsequent step / operation, and so on. 

The first step / operation of any Gear Function is always one of six available "Readers", defining the source of the input to the first step / operation:

- :ref:`gearfun_reader_keysreader` : Redis keys and values.
- :ref:`gearfun_reader_keysonlyreader` : Redis keys.
- :ref:`gearfun_reader_streamreader` : Redis Stream messages.
- :ref:`gearfun_reader_pythonreader` : Arbitrary Python generator.
- :ref:`gearfun_reader_shardsidreader` : Shard ID. 
- :ref:`gearfun_reader_commandreader` : Command arguments from application client.

Readers can be parameterized to narrow down the subset of data it should operate on, for example by specifying a pattern for the keys or streams it should read. 

Depending on the reader type, Gear Functions can either be run immediately, on demand, as batch jobs or in an event-driven manner by registering it to trigger automatically on various types of events.

Each shard of the Redis Cluster executes its own 'instance' of the Gear Function in parallel on the relevant local shard data, unless explicit collected, or until it is implicitly reduced to its final global result at the end of the function.

You can find more details about the internals of Gear Functions in the `official Documentation <https://oss.redislabs.com/redisgears/master/functions.html>`_.


.. _intro_redgrease:

What is RedGrease?
------------------

The RedGrease package provides a number of functionalities that facilitates writing and executing Gear Functions:


#. :ref:`Redis / Redis Gears client(s) <client>`.

    Extended versions of the `redis Python client`_ and `redis-py-cluster Python client`_ clients, but with additional pythonic functions, mapping closely (1-to-1) to the :ref:`Redis Gears command set <client_gears_commands>` (e.g. ``RG.PYEXECUTE``, ``RG.GETRESULTS``, ``RG.TRIGGER``, ``RG.DUMPREGISTRATIONS`` etc), outlined in the `official Gears documentation <https://oss.redislabs.com/redisgears/commands.html>`_.

    .. code-block:: python
        :emphasize-lines: 6

        import redgrease

        gear_script = ... # Gear function string, a GearFunction object or a script file path.

        rg = redgrease.RedisGears()
        rg.gears.pyexecute(gear_script)  # <-- RG.PYEXECUTE

#. :ref:`Runtime functions <runtime>` wrappers. 

    The RedisGears server `runtime environment <https://oss.redislabs.com/redisgears/runtime.html>`_ automatically loads a number of special functions into the top level scope (e.g. :class:`.GearsBuilder`, :func:`.execute`, :func:`.log` etc). 
    RedGrease provides placeholder versions that provide **docstrings**, **auto completion** and **type hints** during development, and does not clash with the actual runtime.

    .. image:: ../images/basic_usage_hints.jpg


#. :ref:`Server-side Redis commands <red_commands>`.

    Allowing for *most* Redis (v.6) commands to be executed in the server-side function, against the local shard, as if using a Redis 'client' class, instead of *explicitly* invoking the corresponding command string using :func:`execute() <redgrease.runtime.execute>`. 
    It is basically the `redis Python client`_, but with ``redis.Redis.execute_command()`` rewired to use the Gears-native :func:`redgrease.runtime.execute` instead under the hood. 

    .. literalinclude:: ../../examples/serverside_redis_commands.py
        :start-after: # # Begin Example
        :end-before: # # End Example
        :emphasize-lines: 8, 12, 14

#. First class :ref:`gearfun` objects.

    Inspired by the "remote builders" of the official `redisgears-py <https://github.com/RedisGears/redisgears-py>`_ client, but with some differences, eg:

    * Supports reuse of :ref:`gearfun_open`, i.e. partial or incomplete Gear functions.

    * Can be :ref:`created without a Redis connection <exe_gear_function_obj_pyexecute>`.

    * :ref:`Requirements can be specified per step <gearfun_open>`, instead of only at execution.

    * Can be :ref:`executed in a few different convenient ways <execution>`.
    
    

    |br|

    .. literalinclude:: ../../examples/first_class_gearfunction_objects.py
        :start-after: # # Begin Example
        :end-before: # # End Example
        :emphasize-lines: 29, 31, 34, 43, 47, 50, 51, 53, 54

#. :ref:`A Command Line Tool <cli>`.

    Helps running and/or loading of Gears script files onto a RedisGears instance. 
    Particularly useful for "trigger-based" CommandReader Gears.

    It also provides a simple form of 'hot-reloading' of RedisGears scripts, by continuously monitoring directories containing Redis Gears scripts and automatically 'pyexecute' them on a Redis Gear instance if it detects modifications. 

    The purpose is mainly to streamline development of 'trigger-style' Gear scripts by providing a form of hot-reloading functionality.

    .. code-block:: console
        
        redgrease --server 10.0.2.21 --watch scripts/


#. A bunch of helper functions and methods for common boilerplate tasks. 
    
    * A :mod:`redgrease.utils` module full of utils such as parsers etc.

    * Various :ref:`Syntactic sugar <sugar>` and enum-like objects for common keywords etc.

    * A :ref:`command_decorator`, that makes creation and execution of :class:`redgrease.reader.CommandReader` :ref:`gearfun` trivial, and providing a straight forward way of adding bespoke server-side Redis commands.

    * Reader-specific sugar operators, like :class:`KeysReader.values <.KeysReader>` that automatically lifts out the values.

    * And more...



.. _intro_example_use_cases:

Example Use-Cases
-----------------

The possible use-cases for Redis Gears, and subsequently RedGrease, is virtually endless, but some common, or otherwise interesting use-cases include:

* Automatic Cache-miss handling.

    Make Redis automatically fetch and cache the requested resource, so that clients do not have to handle cache-misses.

* Automatic batched write-through / write-behind.
    
    Make Redis automatically write back updates to slower, high latency datastore, efficiently using batch writes. Allowing clients to write high velocity updates uninterrupted to Redis, without bothering with the slow data store.

    .. figure:: ../images/Gears_Example_2_white.png

        Write-Through / Write-Behind example

* Advanced Data Queries and Transforms.
    
    Perform "Map-Reduce"-like queries on Redis datasets.
    
* Stream event processing.
    
    Trigger processes automatically when data enters Redis.

* Custom commands.
    
    Create custom Redis commands with arbitrarily sophisticated logic, enabling features to virtually any platform with a Redis client implementation. 

Glossary
--------

.. glossary::

    Gear Function
        Gear Function, written as two separate words, refer to any valid `Gear function, as defined in the Redis Gears Documentation <https://oss.redislabs.com/redisgears/master/functions.html>`_, regardless if it was constructed as a pure string, loaded from a file, or programmatically built using RedGrease's ``GearFunction`` constructors.
    

    GearFunction
        GearFunction, written as one word, refers specifically to RedGrease objects of type ``redgrease.GearFunction``.
        
        These are constructed programmatically using either ``redgrease.GearsBuilder``, any of the Reader classes such as ``redgrease.KeysReader``, ``redgrease.StreamReader``, ``redgrease.CommandReader`` etc, or function decorators such as ``redgrease.trigger`` and so on.
        
        It does **not** refer to Gear Functions that are loaded from strings, either explicitly or from files.

.. include :: footer.rst

.. |br| raw:: html

    <br />

.. _Redis: https://redis.io/

.. _RedisLabs: https://redislabs.com/

.. _Redis GitHub: https://github.com/redis/redis

.. _RedisGears: https://redislabs.com/modules/redis-gears/

.. _redis Python client:  https://pypi.org/project/redis/

.. _redis-py-cluster Python client: https://github.com/Grokzen/redis-py-cluster
