
.. include :: banner.rst

.. _runtime:

Builtin Runtime Functions
=========================

.. include :: wip.rst

You can load the default redisgears symbols (e.g. `GearsBuilder`, `GB`, `atomic`, `execute`, `log` etc) from the `redgrease.runtime` package. 

During development this will give you auto-completion and type hints
In the Redis Gears Python runtime, all the `redgrease.runtime` are mapped directly to the normal ones wihtout side-effects.

``
from redgrease import GearsBuilder, log, atomic, execute
``

This will enable auto completion in your IDE, for Redis Gears stuff. Example from Redis Gears Introduction:

RedGrease's `runtime` package will detect when it is imported inside an actual RedisGears Python runtime environment and will then load the default redis gears symbols, avoiding conflict with the built-in Redis Gears Python environment.
If it is loaded outside a Redis Gears Python runtime environment, i.e. a development environment, the `redgrease.runtime` package will instead load placeholder symbols with decent (hopefully) doc-strings and type-hints, for easier development.

It is possible to load all symbols, using `*`, but it's generally not a recomended practice.


.. _runtime_gearsbuilder:

GearsBuilder
------------

.. _runtime_execute:

execute
-------

Example
~~~~~~~
Below is an example::

   from redgrease.runtime import GearsBuilder, execute

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

.. _runtime_atomic:

atomic
------

.. _runtime_configGet:

configGet
---------

.. _runtime_gearsConfigGet:

gearsConfigGet
--------------

.. _runtime_hashtag:

hashtag
-------

.. _runtime_log:

log
---

.. _runtime_gearsFuture:

gearsFuture
-----------

.. include :: footer.rst