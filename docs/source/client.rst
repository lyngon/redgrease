.. include :: banner.rst

.. _client:

RedGrease Client
================

The RedGrease client / connection object gives access to the various commands defineed by the RedisGears module.

.. _client_gears_instantiation:

Instatiation
------------

The RedGrease client can be instantiated in a few different ways.

As a Redis Client
~~~~~~~~~~~~~~~~~

This instantiation method is usuful if you are working with Redis from scratch and are not working on a legacy application that already has a bunch of Redis objects instantiated.

.. code-block::

    import redgrease

    r = redgrease.RedisGears(host="localhost", port=6379)


This instaniates a Redis client object that is a subclass of the popular `redis <https://github.com/andymccurdy/redis-py>`_ python client, but with an additional property `gears` through the :ref:`RedisGears commands can be accessed <client_gears_commands>`. 

The constructor takes all the same arguments as the normal Redis client, and naturally it expodes all the same mehtods.

It also means that if you are running a Redis instance on the default Redis port 6379 locally, as above, then you dont need any arguments to instantiate the client object: ```r = redgreas.RedisGears()```.

.. note::

    RedGrease also supports cluster-mode. Instantiating ```RedisGears``` will automatically try to figure out if it is against a cluster, and in that case instead  instantiate an object which is a subtype of `redis-py-cluster <https://github.com/Grokzen/redis-py-cluster>`_ (which in turn also is a subclass of the original client).

    The constructor takes all the same arguments as the normal Redis client, and naturally it expodes all the same mehtods.

    If you want to be explicit which one to instaniate, then you can use ```redgrease.Redis``` and ``redgrease.RedisCluster``` for single and cluster mode respectively.

As a Gears object
~~~~~~~~~~~~~~~~~

If you alreay have code with intantiated Redis client objects, then you can just instaniate a ```redgrease.Gears``` object instead.

.. code-block::

    import redis  # Alt. `rediscluster`
    import redgrease

    # Legacy code 
    r = redis.Redis()  # Alt. `rediscluster.RedisCluster()`
    ...
    
    # New code
    gears = redgrease.Gears(r)


This instaniates a Gears object, which only exposes the :ref:`RedisGears commands can be accessed <client_gears_commands>`, and not the normal Redis commands.
This object is the same object that the above RedisGears client exposes through its ```gears``` property.

.. warning::

    There is currently a bug (`issue  #105 <https://github.com/lyngon/redgrease/issues/105>`_) with this approach, but it should hopefully shortly be fixed.


.. _client_gears_commands:

RedisGears Commands
--------------

The the commands introduced by the RedisGears module can be accessed through the ```Gears``` object. 
This section gives a short run-down of the various commands and what they do, in the order of usefulnes to most people. 

The API Reference contains more details about the :class:`redgrease.Gears` class.

.. include:: wip.rst

gears.pyexecute
~~~~~~~~~~~~~~~

gears.dumpexecutions
~~~~~~~~~~~~~~~~~~~~

gears.getersults
~~~~~~~~~~~~~~~~

gears.dumpregistrations
~~~~~~~~~~~~~~~~~~~~~~~

gears.unregister
~~~~~~~~~~~~~~~~

gears.trigger
~~~~~~~~~~~~~

gears.abortexecution
~~~~~~~~~~~~~~~~~~~~

gears.dropexecution
~~~~~~~~~~~~~~~~~~~

gears.getexecution
~~~~~~~~~~~~~~~~~~

gears.pydumpreqs
~~~~~~~~~~~~~~~~

gears.pystats
~~~~~~~~~~~~~

gears.infocluster
~~~~~~~~~~~~~~~~~

gears.refreshcluster
~~~~~~~~~~~~~~~~~~~~

.. _client_gears_config:

Gears Configuration
-------------------

gears.configget
gears.configset

.. include :: footer.rst