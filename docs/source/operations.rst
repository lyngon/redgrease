.. include :: banner.rst

.. _operations:

Operations
==========

This section goes through the various opertions available to :ref:`gearfun_open` in more detail.

.. _op_map:

Map 
---

.. autoclass:: redgrease.gears.Map



.. _op_flatmap:

FlatMap
-------

.. autoclass:: redgrease.gears.FlatMap



.. _op_foreach:

ForEach
-------

.. autoclass:: redgrease.gears.ForEach



.. _op_filter:

Filter
------

.. autoclass:: redgrease.gears.Filter



.. _op_accumulate:

Accumulate
----------

.. autoclass:: redgrease.gears.Accumulate



.. _op_localgroupby:

LocalGroupBy
------------

.. autoclass:: redgrease.gears.LocalGroupBy



.. _op_limit:

Limit
-----

.. autoclass:: redgrease.gears.Limit



.. _op_collect:

Collect
-------

.. autoclass:: redgrease.gears.Collect



.. _op_repartition:

Repartition
-----------

.. autoclass:: redgrease.gears.Repartition



.. _op_aggregate:

Aggregate
---------

.. autoclass:: redgrease.gears.Aggregate



.. _op_aggregateby:

AggregateBy
-----------

.. autoclass:: redgrease.gears.AggregateBy



.. _op_groupby:

GroupBy 
-------

.. autoclass:: redgrease.gears.GroupBy



.. _op_batchgroupby:

BatchGroupBy
------------

.. autoclass:: redgrease.gears.BatchGroupBy



.. _op_sort:

Sort
----

.. autoclass:: redgrease.gears.Sort



.. _op_distinct:

Distinct
--------

.. autoclass:: redgrease.gears.Distinct



.. _op_count:

Count
-----

.. autoclass:: redgrease.gears.Count



.. _op_countby:

CountBy
-------

.. autoclass:: redgrease.gears.CountBy



.. _op_avg:

Avg
---

.. autoclass:: redgrease.gears.Avg



.. _op_action:

Actions
=======

Actions closes open GearFunctions, and indicates the running mode of the function, as follows:

==========================  ==============
Action                      Execution Mode
==========================  ==============
:ref:`op_action_run`        "batch-mode"
:ref:`op_action_register`   "event-mode"
==========================  ==============

.. _op_action_run:

Run 
---

.. autoclass:: redgrease.gears.Run



.. _op_action_register:

Register
--------

.. autoclass:: redgrease.gears.Register



Operation Callback Types
========================

This section runs through the various type signatures that the function callbacks used in the :ref:`operations` must follow.

Registrator
-----------

.. autodata:: redgrease.typing.Registrator



Extractor
---------

.. autodata:: redgrease.typing.Extractor


Mapper
------

.. autodata:: redgrease.typing.Mapper


Expander
--------

.. autodata:: redgrease.typing.Expander


Processor
---------

.. autodata:: redgrease.typing.Processor


Filterer
--------

.. autodata:: redgrease.typing.Filterer


Accumulator
-----------

.. autodata:: redgrease.typing.Accumulator


Reducer
-------

.. autodata:: redgrease.typing.Reducer


BatchReducer
------------

.. autodata:: redgrease.typing.BatchReducer


.. include :: footer.rst
