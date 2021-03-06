.. include :: banner.rst

.. _client:

RedGrease Client
================

The RedGrease client / connection object gives access to the various commands defined by the RedisGears module.

.. _client_gears_instantiation:

Instantiation
-------------

The RedGrease client can be instantiated in a few different ways.

As a Redis Client
~~~~~~~~~~~~~~~~~

This instantiation method is useful if you are working with Redis from scratch and are not working on a legacy application that already has a bunch of Redis objects instantiated.

.. code-block::

    import redgrease

    r = redgrease.RedisGears(host="localhost", port=6379)


This instantiates a Redis client object that is a subclass of the ``Redis`` class of the popular `redis <https://github.com/andymccurdy/redis-py>`_ python client, but with an additional property ``gears`` through which the :ref:`RedisGears commands can be accessed <client_gears_commands>`. 

The constructor takes all the same arguments as the normal Redis client, and naturally it exposes all the same Redis command methods as the original.

It also means that if you are running a Redis instance on the default Redis port (6379) locally, as above, then you don't need any arguments to instantiate the client object: ``r = redgrease.RedisGears()``.

.. note::

    RedGrease also supports cluster-mode. Instantiating ``RedisGears`` will automatically try to figure out if it is against a cluster, and in that case instead  instantiate an object which is a subclass of ``RedisCluster`` from the `redis-py-cluster <https://github.com/Grokzen/redis-py-cluster>`_ package (which in turn also is a subclass of ``Redis`` from the `redis <https://github.com/andymccurdy/redis-py>`_ package).

    The constructor takes all the same arguments as the normal Redis client, and naturally it exposes all the same Redis command methods as well.

    If you want to be explicit which class to instantiate, then you can use :class:`redgrease.Redis` and :class:`redgrease.RedisCluster` for single and cluster mode respectively.

As a Gears object
~~~~~~~~~~~~~~~~~

If you already have code with instantiated Redis client objects, and you don't want to create more connections, then you can instantiate only the ``redgrease.Gears`` object directly, using your existing Redis connection.

.. code-block::

    import redis  # Alt. `rediscluster`
    import redgrease

    # Legacy code 
    r = redis.Redis()  # Alt. `rediscluster.RedisCluster()`
    ...
    
    # New code
    gears = redgrease.Gears(r)


This instantiates a Gears object, which only exposes the :ref:`RedisGears commands can be accessed <client_gears_commands>`, and not the normal Redis commands.
This object is the same object that the above RedisGears client exposes through its ``gears`` property.



.. _client_gears_commands:

RedisGears Commands
-------------------

The commands introduced by the RedisGears Redis module can be invoked through the ``Gears`` object, instantiates as per above. 
This section gives a run-down of the various commands and what they do, in the order of usefulness to most people. 

You can find all the methods and functions in the :ref:`API Reference <api_reference>`, and in this section, we specifically look at the :class:`redgrease.Gears` class.

Command Descriptions:
    - :ref:`client_gears_command_pyexecute`
    - :ref:`client_gears_command_dumpexecutions`
    - :ref:`client_gears_command_getresults`
    - :ref:`client_gears_command_dumpregistrations`
    - :ref:`client_gears_command_unregister`
    - :ref:`client_gears_command_trigger`
    - :ref:`client_gears_command_abortexecution`
    - :ref:`client_gears_command_dropexecution`
    - :ref:`client_gears_command_pydumpreqs`
    - :ref:`client_gears_command_pystats`
    - :ref:`client_gears_command_infocluster`
    - :ref:`client_gears_command_refreshcluster`
    - :ref:`client_gears_command_getexecution`
    - :ref:`client_gears_config`


.. _client_gears_command_pyexecute:

Executing Gear Functions
~~~~~~~~~~~~~~~~~~~~~~~~

:meth:`redgrease.Gears.pyexecute`


This is the most important RedGrease command, as it is the command for executing Gear functions. There are other ways, as we will go through in the next section :ref:`Executing Gear Functions <execution>`, but under the hood they all call this method.

**RedisGears Command:**  `RG.PYEXECUTE <https://oss.redislabs.com/redisgears/1.0/commands.html#rgpyexecute>`_

.. automethod:: redgrease.Gears.pyexecute


.. _client_gears_command_dumpexecutions:

Get List of Executions
~~~~~~~~~~~~~~~~~~~~~~

:meth:`redgrease.Gears.dumpexecutions`

**RedisGears Command:**  `RG.DUMPEXECUTIONS <https://oss.redislabs.com/redisgears/1.0/commands.html#rgdumpexecutions>`_

.. automethod:: redgrease.Gears.dumpexecutions



.. _client_gears_command_getresults:

Get Result of Asynchronous Execution
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:meth:`redgrease.Gears.getresults`

**RedisGears Command:**  `RG.GETRESULTS <https://oss.redislabs.com/redisgears/1.0/commands.html#rggetresults>`_

.. automethod:: redgrease.Gears.getresults



.. _client_gears_command_dumpregistrations:

Get List of Registered Event-Based Functions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:meth:`redgrease.Gears.dumpregistrations`

**RedisGears Command:**  `RG.DUMPREGISTRATIONS <https://oss.redislabs.com/redisgears/1.0/commands.html#rgdumpregistrations>`_

.. automethod:: redgrease.Gears.dumpregistrations



.. _client_gears_command_unregister:

Un-register an Event-Based Function
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:meth:`redgrease.Gears.unregister`

**RedisGears Command:**  `RG.UNREGISTER <https://oss.redislabs.com/redisgears/1.0/commands.html#rgunregister>`_

.. automethod:: redgrease.Gears.unregister



.. _client_gears_command_trigger:

Trigger a 'Command-Event'
~~~~~~~~~~~~~~~~~~~~~~~~~

:meth:`redgrease.Gears.trigger`

**RedisGears Command:**  `RG.TRIGGER <https://oss.redislabs.com/redisgears/1.0/commands.html#rgtrigger>`_

.. automethod:: redgrease.Gears.trigger



.. _client_gears_command_abortexecution:

Abort a Running Execution
~~~~~~~~~~~~~~~~~~~~~~~~~

:meth:`redgrease.Gears.abortexecution`

**RedisGears Command:**  `RG.ABORTEXECUTION <https://oss.redislabs.com/redisgears/1.0/commands.html#rgabortexecution>`_

.. automethod:: redgrease.Gears.abortexecution



.. _client_gears_command_dropexecution:

Remove an Execution
~~~~~~~~~~~~~~~~~~~

:meth:`redgrease.Gears.dropexecution`

**RedisGears Command:**  `RG.DROPEXECUTION <https://oss.redislabs.com/redisgears/1.0/commands.html#rgdropexecution>`_

.. automethod:: redgrease.Gears.dropexecution



.. _client_gears_command_pydumpreqs:

Get List of Registered Dependencies
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:meth:`redgrease.Gears.pydumpreqs`

**RedisGears Command:**  `RG.PYDUMPREQS <https://oss.redislabs.com/redisgears/1.0/commands.html#rgpydumpreqs>`_

.. automethod:: redgrease.Gears.pydumpreqs



.. _client_gears_command_pystats:

Get Python Runtime Statistics
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:meth:`redgrease.Gears.pystats`

**RedisGears Command:**  `RG.PYSTATS <https://oss.redislabs.com/redisgears/1.0/commands.html#rgpystats>`_

.. automethod:: redgrease.Gears.pystats



.. _client_gears_command_infocluster:

Get Cluster Information
~~~~~~~~~~~~~~~~~~~~~~~

:meth:`redgrease.Gears.infocluster`

**RedisGears Command:**  `RG.INFOCLUSTER <https://oss.redislabs.com/redisgears/1.0/commands.html#rginfocluster>`_

.. automethod:: redgrease.Gears.infocluster



.. _client_gears_command_refreshcluster:

Refresh Cluster Topology
~~~~~~~~~~~~~~~~~~~~~~~~

:meth:`redgrease.Gears.refreshcluster`

**RedisGears Command:**  `RG.REFRESHCLUSTER <https://oss.redislabs.com/redisgears/1.0/commands.html#rgrefreshcluster>`_

.. automethod:: redgrease.Gears.refreshcluster



.. _client_gears_command_getexecution:

Get a Detailed Execution Plan
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:meth:`redgrease.Gears.getexecution`

**RedisGears Command:**  `RG.GETEXECUTION <https://oss.redislabs.com/redisgears/1.0/commands.html#rggetexecution>`_

.. automethod:: redgrease.Gears.getexecution



.. _client_gears_config:

Get and Set Gears Configurations
--------------------------------

The above mentioned :ref:`Gears <client_gears_commands>` object contains a property ``config`` of type :class:`redgrease.config.Config`, through which various pre-defined and custom configuration setting can be read an written.

This object contains readable, and for certain options also writable, properties for the predefined RedisGears configurations.
It also contains :meth:`getter <redgrease.config.Config.get>` and :meth:`setter <redgrease.config.Config.set>` methods that allows access to both the pre-defined as well as user defined configurations, in bulk.


**RedisGears Commands:** `RG.CONFIGGET <https://oss.redislabs.com/redisgears/1.0/commands.html#rgconfigget>`_ and `RG.CONFIGSET <https://oss.redislabs.com/redisgears/1.0/commands.html#rgconfigset>`_

.. autoclass:: redgrease.config.Config
    :members:
    :undoc-members:


.. include :: footer.rst
