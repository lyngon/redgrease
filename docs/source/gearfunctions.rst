.. include :: banner.rst

.. _gearfun:

GearFunction
============

GearFunction objects are RedGrease's representation of RedisGears Gear functions.
There are a couple of different classes of GearFunctions to be aware of.

.. figure:: ../images/GearFunctions_UML.png

    UML Relationships between different GearFunction 

These function objects can be dynamically constructed in either your application code or in separate script files. They are constructed using either the :ref:`gearfun_builder` (blue) or any of the :ref:`gearfun_readers` (green):

- :ref:`gearfun_reader_keysreader`
- :ref:`gearfun_reader_keysonlyreader`
- :ref:`gearfun_reader_streamreader`
- :ref:`gearfun_reader_pythonreader`
- :ref:`gearfun_reader_shardsidreader`
- :ref:`gearfun_reader_commandreader`

These are responsible for reading data from different sources into records, either in batch on-demand or when some event occurs. 

GearFunctions are created by "attaching" :ref:`operations`, such as :ref:`op_map`, :ref:`op_groupby`, :ref:`op_aggregate` etc, to a :ref:`gearfun_builder`, a:ref:`gearfun_readers`  or another GearFunction.

With the exception of the :ref:`gearfun_builder`, "attaching" an operation on a GearFunction, doesn't actually modify it, but instead creates a new function with that operation added. 

All Gear functions therefore consists of a chains of zero or more :ref:`operations`, with some reader at the begining. 

Some Readers allow for some additional reader-specific operations, that can be invoked ony as as the first operation.

Some Readers support both "batch-mode" and "event-mode", but some only support one of the modes.

Gear functions are "terminated" by either one of two special :ref:`op_action`. Either :ref:`op_action_run`, for immediate "batch-mode" execution, or :ref:`op_action_register`, for registering the function for "event-mode" execution.

GearFunctions that **have not** been "terminated" by any :ref:`op_action`, are referred to as :ref:`gearfun_open`, as they **can be extended** with more operations, creating new GearFunctions. 

GearFunctions that **have** been "terminated" by any :ref:`op_action`, are referred to as :ref:`gearfun_closed`, as they **cannot be extended** with more operations, creating new GearFunctions, but they can be executed. 

Every :ref:`gearfun_open`, including the :ref:`gearfun_builder`, implement the default set of :ref:`operations`.

When a GearFuctions is executed, the Reader reads its data, and pass each record to its first operation, which modifies, filters or aggregates the records into some new output records, which in turn are passed to the next operation and so on, until the last operation.

The output of the final operation is then either, returned to the caller if it was a "batch-mode" execution **and** ``unblocking`` was not set to ``True`` in :meth:`.Gears.pyexecute`, or stored for later retrieval oterwise.


.. _gearfun_open:

OpenGearFunction 
~~~~~~~~~~~~~~~~~~~

You would never instatiate a :class:`.gears.OpenGearFunction` yourself, but all "open" :ref:`GearFuctions <gearfun>` that has not yet been "closed" with the :ref:`op_action_run` or :ref:`op_action_register` :ref:`op_action`, inherits from this class. 

It is this class that under the hood is responsible for "attaching" :ref:`opertations` and :ref:`Actions <op_action>` and creating new GearFuctions.

This includes both the :class:`.runtime.GearsBuilder` as well as the :ref:`gearfun_readers`:


.. autoclass:: redgrease.gears.OpenGearFunction()
    :members:
    :member-order: bysource


.. _gearfun_closed:

ClosedGearFunction
~~~~~~~~~~~~~~~~~~

.. autoclass:: redgrease.gears.ClosedGearFunction()
    :members:
    :undoc-members:
    :member-order: bysource


.. _gearfun_builder:

GearsBuilder
------------

If you are familiar with RedisGears from before, then the :class:`.runtime.Gearsbuilder` should be very familiar. In fact the RedGrease version is designed to be backwards compatible with the `RedisGears's Context Builder <https://oss.redislabs.com/redisgears/1.0/functions.html#context-builder>`_, with the same name.


The the :ref:`runtime_gearsbuilder` is technically a part of on the :ref:`runtime` and is exposed both through  :class:`redgrease.runtime.GearsBuilder` as well as :class:`redgrease.GearsBuilder`. 

Check out the :ref:`runtime`  and specifically the section on the :ref:`runtime_gearsbuilder` for more details.

.. _gearfun_readers:

Readers
-------


.. _gearfun_reader_keysreader:

KeysReader
~~~~~~~~~~

.. autoclass:: redgrease.reader.KeysReader
    :members:
    :undoc-members:
    :member-order: bysource


.. _gearfun_reader_keysonlyreader:

KeysOnlyReader
~~~~~~~~~~~~~~

.. autoclass:: redgrease.reader.KeysOnlyReader
    :members:
    :undoc-members:
    :member-order: bysource



.. _gearfun_reader_streamreader:

StreamReader
~~~~~~~~~~~~

.. autoclass:: redgrease.reader.StreamReader
    :members:
    :undoc-members:
    :member-order: bysource



.. _gearfun_reader_pythonreader:

PythonReader
~~~~~~~~~~~~

.. autoclass:: redgrease.reader.PythonReader
    :members:
    :undoc-members:
    :member-order: bysource



.. _gearfun_reader_shardsidreader:

ShardsIDReader
~~~~~~~~~~~~~~

.. autoclass:: redgrease.reader.ShardsIDReader
    :members:
    :undoc-members:
    :member-order: bysource



.. _gearfun_reader_commandreader:

CommandReader
~~~~~~~~~~~~~

.. autoclass:: redgrease.reader.CommandReader
    :members:
    :undoc-members:
    :member-order: bysource


.. include :: footer.rst

