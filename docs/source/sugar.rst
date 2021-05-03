.. include :: banner.rst

.. _sugar:

Syntactic Sugar
===============

Various minor things to make life easier, prettier, more concise and/or less error prone.

.. _command_decorator:

Command Function Decorator
--------------------------

The :func:`@redgrease.command <redgrease.command>` decorator can be put on functions to immediately turn them into :ref:`gearfun_reader_commandreader` GearFunctions. 

The decorator takes all the same parameters as the :meth:`CommandReader.register() <.OpenGearFunction.register>`, but the ``trigger`` argument is optional, and the name of the function is used as default.

The decorator also takes a boolean keyword argument ``replace`` which indicates what should be done if a function with the same trigger has already been registered, as follows:

- ``None`` - Default - A :class:`redgrease.exceptions.DuplicateTriggerError` error is raised.
- ``True`` - The registered function is replaced.
- ``False`` - Nothing. No error is raised, but the existing registered function remains.

The decorator also takes the keyword ``on``, which if provided with a Redis Gears client connection, attempts to register the function as a command reader on the server.

The GearFunction can be triggered by simply locally calling the decorated function, just as if it was a local function. 
Under the hood, however, this will send the trigger and any passed arguments to the server, which in turn runs the registered CommandReader function on the Redis shards, and then retuns the function results to the caller.


.. literalinclude:: ../../examples/cache_get_command.py
    :caption: examples/cache_get_command.py
    :start-after: import timeit
    :end-before: # Test caching on some images
    :lineno-match:


.. note::

    The ``on`` argument is actually optional, but in that case the decorated function cannot be called locally as described above. 

    Instead the function name becomes a :ref:`"Closed" <gearfun_open_closed>` :ref:`gearfun_reader_commandreader` :ref:`gearfun`, which can be turned into a function as per above again, by calling the :meth:`on() <.ClosedGearFunction.on>` method::

        import redgrease

        # ``on`` argument **not** provided => ``foo`` becomes a Closed GearFunction
        @redgrease.command()
        def foo(arg1, arg2):
            redgrease.cmd.log(f"Pretending to do something with {arg1} and {arg2}")


        r = redgrease.RedisGears()

        # Call ``on`` on foo with a Gears client 
        # => result is a local triggering function
        do_foo = foo.on(r)

        # Call / trigger the function
        do_foo("this", "that")

        
.. autofunction:: redgrease.command


.. _sugar_keywords

Keywords
--------

Moderately useful symbols that can be used instead of strings for various RedisGears Keywords.

Reader Types
~~~~~~~~~~~~

.. autoattribute:: redgrease.ReaderType.KeysReader
.. autoattribute:: redgrease.ReaderType.KeysOnlyReader
.. autoattribute:: redgrease.ReaderType.StreamReader
.. autoattribute:: redgrease.ReaderType.PythonReader
.. autoattribute:: redgrease.ReaderType.ShardsIDReader
.. autoattribute:: redgrease.ReaderType.CommandReader

Example::

    from redgrease import GearsBuilder, ReaderType

    gb = GearsBuilder(ReaderType.KeysReader).run()



Trigger Modes
~~~~~~~~~~~~~

.. autoattribute:: redgrease.TriggerMode.Async
.. autoattribute:: redgrease.TriggerMode.AsyncLocal
.. autoattribute:: redgrease.TriggerMode.Sync

Example::

    from redgrease import GearsBuilder, KeysReader, TriggerMode

    fun = KeysReader().register(mode=TriggerMode.AsyncLocal)


Key types
~~~~~~~~~

.. autoattribute:: redgrease.KeyType.String
.. autoattribute:: redgrease.KeyType.Hash
.. autoattribute:: redgrease.KeyType.List
.. autoattribute:: redgrease.KeyType.Set
.. autoattribute:: redgrease.KeyType.ZSet
.. autoattribute:: redgrease.KeyType.Stream
.. autoattribute:: redgrease.KeyType.Module

Example::

    from redgrease import KeysReader, KeyType

    fun = KeysReader().register(keyTypes=[KeyType.List, KeyType.Set, KeyType.ZSet])


Failure Policies
~~~~~~~~~~~~~~~~

.. autoattribute:: redgrease.FailurePolicy.Continue
.. autoattribute:: redgrease.FailurePolicy.Abort
.. autoattribute:: redgrease.FailurePolicy.Retry

Example::

    from redgrease import StreamReader, FailurePolicy

    fun = StreamReader().register(onFailedPolicy=FailurePolicy.Abort)
    

Log Levels
~~~~~~~~~~

.. autoattribute:: redgrease.LogLevel.Debug
.. autoattribute:: redgrease.LogLevel.Verbose
.. autoattribute:: redgrease.LogLevel.Notice
.. autoattribute:: redgrease.LogLevel.Warning

Example::

    from redgrease import KeysOnlyReader, LogLevel

    fun = KeysOnlyReader().map(lambda k: log(f"Processing key: {k}", level=LogLevel.Debug).run() 

.. include :: footer.rst