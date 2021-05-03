.. include :: banner.rst

.. _red_commands:

Serverside Redis Commands
=========================

With the RedGrease :ref:`runtime package installed on the RedGrease server <adv_extras>`, you can execute Redis commands to the local shards using "serverside commands" instead of using :func:`redgrease.runtime.execute` inside your Gear functions.

Serverside Redis Commands, behaves almost identical to a normal Redis client, except that you do not have to instantiate it.

Inside any Gear function, you can simply invoke Redis commands just as you would in your client using ``redgrease.cmd``:

Example::

    redgrease.cmd.set("Foo", "Bar")


.. warning::

    Serverside Redis Commands have the following limitations:

    A. Only executes commands against the local shard. 
        This is also the case for both the :func:`redgrease.runtime.execute` function as well as the the RedisGears default builtin `execute()` function too.

    B. Blocking commands, such as "BRPOP" or "BLPOP" ect, are not supported. 
        This is also the case for both the :func:`redgrease.runtime.execute` function as well as the the RedisGears default builtin `execute()` function too.




.. include :: footer.rst