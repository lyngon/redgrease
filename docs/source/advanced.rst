.. include :: banner.rst

.. _advanced:

Advanced Concepts
=================

.. include :: wip.rst

.. _adv_extras:

Redgrease Extras Options
------------------------

It is always recommendede to install either the redgrease package with either ``redgrease[client]``, ``redgrease[cli]`` or ``redgrease[all]`` package opthions on your clients. 

However for the RedisGears server runtime, you may want to be more prudent with what you install. Therefore, RedGrease strives to give you visibility and control of control of how much of RedGrease you want to install on on your RedisGear server.

For the server you may want to consider these different options:

- ``redgrease[runtime]``

    This is the default and recommended package is to install on your server as this gives you access to every runtime feature in RedGrease, including all the :ref:`gearfun` constructs, the :ref:`red_commands`, as well as the :ref:`runtime`.

    This is what :meth:`.Gears.pyexecute` is going to automatically assume for you when you pass any dynamic :ref:`gearfun` to it, or if you set the ``enforce_redgrease`` argment to eiter ``True`` or just a verison string (e.g. ``"0.3.12"``).

    This option installs all the dependecies that are needed for any of the RedGrease runtime features, but none of the dependecies that are only required for the RedGrease client features. 

- ``redgrease``

    You can also install just the bare ``redgrease`` package without any dependecies. This limits the RedGrease functionalities that you can use to the ones provided by the :ref:`adv_extras_cleanmod`. 

    Most notably this will prevent you from using the :ref:`red_commands`.

    To enforce this option, ensure that any calls to :meth:`.Gears.pyexecute` explicitly set the ``enforce_redgrease`` argument to ``"redgrease"`` (without extras). Version qualifiers are supported (e.g. ``"redgrease>0.3"``).

- Nothing

   Yes, In some cases RedGrease may be of value even if it is **not** installed in the RedisGears runtime environment. If your are only executing GearFunctions by :ref:`exe_gear_function_file`, and the script itself is either:

      **A.** Not importing ``redgrease`` at all (obviously), or 

      **B.** Only using explictly imported :ref:`runtime`, I.e. only importing RedGrease with::
        
            from redgrease.runtime import ...

    If this is the case, you can enforce that RedGrease is **not** added to the runtime requirements at all by ensuring that any calls to :meth:`.Gears.pyexecute` explicitly set the ``enforce_redgrease`` argument to ``False``. This option will not add any redgrease requirement to the function and simply ignore any explicit runtime imports.

    .. Note::
    
        This only applies to explcit imports of symbols in the :mod:`runtime` module, and not to imports of the moule itself.

        I.e, imports of the form::

            from redgrease.runtime import GB, hashtag

        Or::

            from redgrease.runtime import *

        But not::

            import redgrease.runtime

        Nor::

            from redgrease import GB, hashtag



Dependency Packages per Option
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The dependecies of the different extras options are as follows:

- ``redgrease``

    - Clean. No dependencies. See :ref:`adv_extras_cleanmod` for a list of RedGrease modules that can be used.

- ``redgrease[runtime]``

    - `attrs <https://pypi.org/project/attrs/>`_ - This dependency may be removed in future versions.
    - `cloudpickle <https://pypi.org/project/cloudpickle/>`_ - This dependency may be replaced with `dill <https://pypi.org/project/dill/>`_ in future versions.
    - `redis <https://pypi.org/project/redis/>`_
    - `packaging <https://pypi.org/project/packaging/>`_ - This dependency may be moved to the ``client`` extra in future versions.
    - `wrapt <https://pypi.org/project/wrapt/>`_  - This dependency may be removed in future versions.

- ``redgrease[client]`` 

    - All the dependecies of ``redgrease[runtime]``, plus
    - `typing-extensions <https://pypi.org/project/typing-extensions/>`_
    - `redis-py-cluster <https://pypi.org/project/redis-py-cluster/>`_ - This dependency may be moved to a new ``cluster`` extra in future versions.

.. - ``redgrease[client,cluster]`` - All the dependecies of `client`, plus
..     - `redis-py-cluster <https://pypi.org/project/redis-py-cluster/>`_

- ``redgrease[cli]``

    - All the dependecies of ``redgrease[client]``, plus
    - `watchdog <https://pypi.org/project/watchdog/>`_ 
    - `ConfigArgParse <https://pypi.org/project/ConfigArgParse/>`_
    - `pyyaml <https://pypi.org/project/PyYAML/>`_

.. - ``redgrease[cli,cluster]`` - 

- ``redirease[all]``

    - All dependecies above


.. _adv_extras_cleanmod:

"Clean" RedGrease Modules
~~~~~~~~~~~~~~~~~~~~~~~~~

The "clean" RedGrease modules, that can be used without extra dependencies are:

- :mod:`redgrease.runtime` - Wrapped versions of the built-in runtime functions, but with docstrings and type hints.

- :mod:`redgrease.reader` - GearFunction constructors for the various Reader types.

- :mod:`redgrease.func` - Function decorator for creating ``CommandReader`` functions.

- :mod:`redgrease.utils` - A bunch of helper functions.

- :mod:`redgrease.sugar` - Some trivial sugar for magic strings and such.

- :mod:`redgrease.typing` - A bunch of type helpers, typically not needed to be imported in application code.

- :mod:`redgrease.gears` - The core internals of RedGrease, rarely needed to be imported in application code.

- :mod:`redgreas.hysteresis` - A helper module, specifically for the RedGrease CLI. Not intended to be imported in application code.
    
.. _adv_pyver:

Python 3.6 and 3.8+ 
-------------------

.. _adv_pure:

Pure Gear Scripts
-----------------


.. include :: footer.rst