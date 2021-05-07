
.. include :: banner.rst

.. _runtime:

Builtin Runtime Functions
=========================

The RedisGears Python server runtime automagically expose a number of functions into the scope of any Gear functions being executed.
These "builtin" runtime functions can be used in Gear Functions without importing any module or package:

   - :ref:`runtime_execute`
   - :ref:`runtime_atomic`
   - :ref:`runtime_configGet`
   - :ref:`runtime_gearsConfigGet`
   - :ref:`runtime_hashtag`
   - :ref:`runtime_log`
   - :ref:`runtime_gearsfuture`
   - :ref:`runtime_gearsbuilder`

.. note::
   With the exception of the :ref:`runtime_gearsbuilder` neithe these functions **cannot** be used in normal application code outside Gear functions running in the RedisGears server runtime.

RedGrease expose its own wrapped versions of these RedisGears runtime functions which, for the most part, behave exactly like the originals, but require you to import them, either from the top level :mod:`redgrease` package, or from the :mod:`redgrease.runtime` module.

But if these are the same, why would you bother with them?

The main reason to use the RedGrease versions, is that they aid during develpent by enabling most Integrated Development Environment (IDE) access to the doc-strings and type-annotations that the RedGrease versions are providing. 

This alone can help greatly in developing gears faster (e.g. through auto-complete) and with less errors (e.g. with type checking).

.. note::

   If you are **only** using these wrapped runtime functions in your Gear Functions, and **no other** RedGrease features, then you actually don't need RedGrease to be installed on the RedisGears server runtime. Explicitly setting ``enforce_redgrease`` argument to ``False`` when executing a function script with to :meth:`.Gears.pyexecute`,  will not add any redgrease requirement to the function and simply ignore any explicit runtime imports.

   The section, :ref:`adv_extras`, goes deeper into the detals of the various RedGrease extras options, and their limitations.

Another reason to use the functions from RedGrease :mod:`runtime`, is that it contains some slightly enhanced variants of the defaults, like for example, the :ref:`runtime_log`, or have alternative versions, like :ref:`runtime_hashtag3`. 

And if you are going to use other RedGrease features, then you will have to load the top level ``redgrease`` namespace anyway, which automatically expose the runtime functions.


The RedGrease runtime functions can be imported in a few ways:

- Directly from the package top-level, e.g::

   from redgrease import GearsBuilder, log, atomic, execute

- Explicitly from the :mod:`redgrease.runtime` module::

   from redgrease.runtime import GearsBuilder, log, atomic, execute

- By importing the :mod:`redgrease.runtime` module::

   import redgrease.runtime


It is possible to load all symbols, using ``*``, although it's generally not a recomended practice, particularly not for the top level ``redgrease`` package.


.. _runtime_execute:

execute
-------

RedGrease's version of :func:`.runtime.execute` behaves just like the default.

This function executes an arbitrary Redis command insire Gear functions. 

.. note::

   For more information about Redis commands refer to:

      - `Redis commands <https://redis.io/commands>`_

Arguments

   - command : the command to execute
   - args : the command's arguments

Example::

   from redgrease import execute

   # Pings the server (reply should be 'PONG')
   reply = execute('PING')

In most cases, a more convenient approach is to use :ref:`red_commands` to execute Redis Commands inside Gear Functions.

Longoer Example::

   from redgrease import GearsBuilder, execute

   def age(x):
      ''' Extracts the age from a person's record '''
      return int(x['value']['age'])

   def cas(x):
      ''' Checks and sets the current maximum '''
      k = 'age:maximum'
      v = execute('GET', k)   # read key's current value
      v = int(v) if v else 0  # initialize to 0 if N
      if x > v:               # if a new maximum found
      execute('SET', k, x)  # set key to new value

   # Event handling function registration
   gb = GearsBuilder()
   gb.map(age)
   gb.foreach(cas)
   gb.register('person:*')

   
``execute`` API Reference
~~~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: redgrease.runtime.execute



.. _runtime_atomic:

atomic
------

RedGrease's version of :func:`.runtime.atomic` behaves just like the default.

Atomic provides a `context manager <https://book.pythontips.com/en/latest/context_managers.html>`_  that ensures that all operations in it are executed atomically by blocking the main Redis process.

Example::

   from redgrease import atomic, GB, hashtag

   # Increments two keys atomically
   def transaction(_):
      with atomic():
         execute('INCR', f'{{{hashtag()}}}:foo')
         execute('INCR', f'{{{hashtag()}}}:bar')

   gb = GB('ShardsIDReader')
   gb.foreach(transaction)
   gb.run()


``atomic`` API Reference
~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: redgrease.runtime.atomic



.. _runtime_configGet:

configGet
---------

RedGrease's version of :func:`.runtime.configGet` behaves just like the default.

This function fetches the current value of a RedisGears `configuration <https://oss.redislabs.com/redisgears/1.0/configuration.html>`_ options. 

Example::

   from redgrease import configGet

   # Gets the current value for 'ProfileExecutions'
   foo = configGet('ProfileExecutions')

``configGet`` API Reference
~~~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: redgrease.runtime.configGet



.. _runtime_gearsConfigGet:

gearsConfigGet
--------------

RedGrease's version of :func:`.runtime.gearsConfigGet` behaves just like the default.

This function fetches the current value of a RedisGears `configuration <https://oss.redislabs.com/redisgears/1.0/configuration.html>`_ options, and returns a defualt value if that key does not exist.

Example::

   from redgrease import gearsConfigGet

   # Gets the 'foo' configuration option key and defaults to 'bar'
   foo = gearsConfigGet('foo', default='bar')


``gearsConfigGet`` API Reference
~~~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: redgrease.runtime.gearsConfigGet



.. _runtime_hashtag:

hashtag
-------

RedGrease's version of :func:`.runtime.hashtag` behaves just like the default.

This function returns a hashtag that maps to the lowest hash slot served by the local engine's shard. Put differently, it is useful as a hashtag for partitioning in a cluster. 

``hashtag`` API Reference
~~~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: redgrease.runtime.hashtag



.. _runtime_hashtag3:

hashtag3
--------

This function, :func:`.runtime.hashtag3`, is not part of the default RedisGears runtime scope, and is introduced by RedGrease.
It is a slightly modified version of version of :func:`.runtime.hashtag` but adds enclosing curly braces (``"{"`` and ``"}"``) to the hashtag, so it can be used directly inside Python `f-strings <https://realpython.com/python-f-strings/>`_.

``hashtag3`` API Reference
~~~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: redgrease.runtime.hashtag3



.. _runtime_log:

log
---

RedGrease's version of :func:`.runtime.log` behaves almost like the default.
It prints a message to Redis' log, but forces the the argment to a string before logging it. 

(The built in default throws an error if the argument is not a string.)

Example::

   from redgrease import GB, log

   # Dumps every datum in the DB to the log for "debug" purposes
   GB().foreach(lambda x: log(str(x), level='debug')).run()


``log`` API Reference
~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: redgrease.runtime.log

.. _runtime_gearsfuture:

gearsFuture
-----------

The :class:`.gearsFuture` object allows another thread/process to process the
record.

Returning this object from a step's operation tells RedisGears to suspend execution
until background processing had finished/failed.

The gearsFuture object provides two control methods: :meth:`.gearsFuture.continueRun` and
:meth:`.gearsFuture.continueFailed`. Both methods are thread-safe and can be called at any time to
signal that the background processing has finished.

:meth:`.gearsFuture.continueRun` signals success and its argument is a record for the main process.
:meth:`.gearsFuture.continueFailed` reports a failure to the main process and its argument is a
string describing the failure.

Calling gearsFuture() is supported only from the context of the following
operations:

* :ref:`op_map`
* :ref:`op_flatmap`
* :ref:`op_filter`
* :ref:`op_foreach`
* :ref:`op_aggregate`
* :ref:`op_aggregateby`

An attempt to create a :class:`.gearsFuture` object outside of the supported contexts
will result in an exception.

.. note::

   :class:`.gearsFuture` was introduced in RedisGears 1.0.2.

Example:
~~~~~~~~

.. literalinclude:: ../../tests/gear_scripts/builtin_runtime_func_gearsFuture.py


gearsFuture with Python Async Await
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:class:`.gearsFuture` is also integrated seamlessly with Python's async/await syntax,
so it possible to do the following:

.. literalinclude:: ../../tests/gear_scripts/builtin_runtime_func_await.py


``gearsFuture`` API Reference
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: redgrease.runtime.gearsFuture
   :members:  


.. _runtime_gearsbuilder:

GearsBuilder
------------

The :class:`.runtime.GearsBuilder` (as well as its short-form alias ``GB``), behaves exactly like the default RedisGears version, with a couple of exceptions:

#. It has a property  ``gearfunction`` which gives access to the constructed :ref:`gearfun` object at that that point in the builder pipeline.

#. Any additional arguments passed to its constructor, will be passed as defaults to the :meth:`run() <redgrease.gears.OpenGearFunction.run>` or  :meth:`register() <redgrease.gears.OpenGearFunction.register>` action that terminates the build.

.. note::

   The :class:`.runtime.GearsBuilder` objects are mutable with respect to the :ref:`operations`, whereas :ref:`gearfun` objects are immutable and returns a new function when an operation is applied. 

   This means that::

      fun = GearsBuilder()
      fun.map(...)
      fun.aggreagateby(...)

   Creates one single function equivalent to::

      fun = KeysReader().map(...).aggreagateby(...)

   Whereas::

      sad = KeysReader()
      sad.map(...)
      sad.aggreagateby(...)

   Creates three functions; One named ``sad`` that is just the :ref:`gearfun_reader_keysreader`, one which is ``sad`` with a :ref:`op_map` and one which is ``sad`` with a :ref:`op_aggregateby`. The latter two functions are also not bound to any varibles in this example.

:class:`.GearsBuilder` API Reference
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: redgrease.runtime.GearsBuilder
   :members:
   :undoc-members:
   :inherited-members:



Now we are finally ready to start building some Gear Functions.


.. include :: footer.rst