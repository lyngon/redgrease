.. include :: banner.rst

.. _execution:

Executing Gear Functions
========================

Gear functions are either defined as raw strings, in separate python script files, as or programatically constructed GearFunction objects. 


.. _exe_gear_function_obj:

GearFunction objects
--------------------

You dynamically create a GearFunction object, directly in the same application as your Gears client object, by using any of the constructs defined in the :ref:`gearfun` section.

Let's for example assume, for the sake of this section, that we have instanitated a Gear client and created a very simple "partial" Gear function as follows.

.. code-block:: python

    import redgrease 

    r = redgrease.RedisGears()

    gear_fun = KeysReader().records().countby(lambda r: r.type)

In this example `r` is our Redis client with Gears features, and ``gear_fun`` is our simple "partial" Gear function. We call it "partial" because it does not have an :meth:`run <redgrease.gears.PartialGearsFunction.run>`  or  :meth:`registret <redgrease.gears.PartialGearsFunction.register>` operation.

RedGrease allows for reusing partial Gear functions in multiple funtions.

Anyway, such objects can be executed in a number of different ways:

.. code-block:: python

    results = r.gears.pyexecute(r.run())

    results = r.gears.pyexecute(r.register())

    results = r.gears.pyexecute(r)  # `run` is assumed

    results = gear_fun.run().on(r)

    results = gear_fun.register().on(r)

    results = gear_fun.run(on=r)

    results = gear_fun.register(on=r)


.. note::

    There are two drawbacks of executing GearFunction objects, compared to executing Gear functions using either raw string, or by script file:

    #. The Python version of local application must match the the Python version in the RedisGears runtime (Python 3.7, at the time of writing this).

        When executing Gear functions using either raw string, or by script file, it doesnt matter which version of Python the application is using, as long as it is Python 3.6 or later and the the code in the raw string is compatible with the Python version in the RedisGears runtime, 

    #. The ``redgrease[runtime]`` package must be installed on the RedisGears Runtime environment.

        If you pass a GearFunction to :meth:`redgrease.Gears.pyexecute`, it will attempt to install the latest stable version of ``redgrease[runtime]`` on the server, unless already installed, or if explicitly told otherwise using the ``enforce_redgrease`` argument.

        When executing Gear functions using either raw string, or by script file, `redgrease` only have to be installed if the function is importing any redgrease modules, of course.




.. _exe_gear_function_file:

Script files
------------

A more practical way of defining Gear Functions is by putting them in a separate script file, and executing it by passing the path to the :meth:`redgrease.Gears.pyexecute` method:

.. code-block:: python

    import redgrease

    gear_script_path = "./path/to/some/gear/script.py"

    rg = redgrease.RedisGears()

    result = rg.gears.pyexecute(gear_script_path)


These scripts may be plain vanilla RedisGears functions that only use the :ref:`built-in runtime functions <runtime>`, and does not import `redgrease` or use any of its features. 
In this case the `redgrease` package does not need to be installed on the runtime.

If the function is importing and using any RedGrease construct from the ``redgreas`` package, then when calling :meth:`redgrease.Gears.pyexecute` method, the ``enforce_redgrease`` must be set in order to ensure that the package is installed on the RedisGears runtime.

In most cases you would just set it to ``True`` to get the latest stable RedGrease runtime package, but you may specify a specific version or even redpository.

A notable special case is when functions in the script are only importing RedGrease modules that do not require any 3rd party dependencies. 
If this is the case then you may want to set `enforce_redgrease="redgrease"` (without the extras `"[runtime]"`), when calling :meth:`redgrease.Gears.pyexecute`, as this is a version of redgrease without any external dependencies. 

.. Note::

    By default all Gear functions run in a shared runtime environment, and as a consequence all requirements / dependencies from differnt Gear functions are all installed in the same Python enviornment.


The "clean" RedGrease modules, whithout dependencies are:

- :mod:`redgrease.runtime` - Wrapped versions of the built-in runtime functions, but with docstrings and type hints.

- :mod:`redgrease.reader` - GearFunction constructors for the various Reader types.

- :mod:`redgrease.func` - Function decorator for creating ``CommandReader`` functions.

- :mod:`redgrease.utils` - A bunch of helper functions.

- :mod:`redgrease.sugar` - Some trivial sugar for magic strings and such.

- :mod:`redgrease.typing` - A bunch of type helpers, typically not needed to be imported in application code.

- :mod:`redgrease.gears` - The core internals of RedGrease, rarely needed to be imported in application code.

- :mod:`redgreas.hysteresis` - A helper module, specifically for the RedGrease CLI. Not intended to be imported in application code.





.. _exe_gear_function_str:

Function Strings
----------------

The most basic way of creating and executing Gear Functions is by passing a raw function string to the :meth:`redgrease.Gears.pyexecute` method:

.. code-block:: python

    import redgrease

    raw_gear_fun = "GearsBuilder('KeysReader').map(lambda x: x['type']).countby().run()"

    rg = redgrease.RedisGears()

    result = rg.gears.pyexecute(raw_gear_fun)

You would rarely construct Gear functions this way, but it is fundamentally what happens under the hood for all the other methods of exetution, and corresponds directly to the underlying RedisGears protocol, so it is good to know.

.. include :: footer.rst