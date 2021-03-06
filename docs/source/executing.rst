.. include :: banner.rst

.. _execution:

Executing Gear Functions
========================

Gear functions can either be defined as a `Raw Function String`_, in `Script File Path`_, as or dynamically constructed `GearFunction Object`_. 
There are some subtleties and variations the three types that we'll go through in their respective section, but either type can be executed using the :meth:`redgrease.Gears.pyexecute` method.

.. code-block:: python
    :emphasize-lines: 7

    import redgrease

    gear_fun = ...  # Either a function string, script file path or GearFunction object

    connection = redgrease.RedisGears()  

    result = connection.gears.pyexecute(gear_fun)

    print(result.value)
    print(result.errors)


.. # TODO: Should talk about the ExecutionResult


.. _exe_gear_function_str:
    
Raw Function String
-------------------

The most basic way of creating and executing Gear Functions is by passing a raw function string to the :meth:`redgrease.Gears.pyexecute` method:

.. code-block:: python

    import redgrease

    raw_gear_fun = "GearsBuilder('KeysReader').map(lambda x: x['type']).countby().run()"

    rg = redgrease.RedisGears()

    result = rg.gears.pyexecute(raw_gear_fun)

.. note:: 

    You would rarely construct Gear functions this way, but it is fundamentally what happens under the hood for all the other methods of execution, and corresponds directly to the underlying RedisGears protocol.

.. _exe_gear_function_file:

Script File Path
----------------

A more practical way of defining Gear Functions is by putting them in a separate script file, and executing it by passing the path to the :meth:`redgrease.Gears.pyexecute` method:

.. code-block:: python

    import redgrease

    gear_script_path = "./path/to/some/gear/script.py"

    rg = redgrease.RedisGears()

    result = rg.gears.pyexecute(gear_script_path)


These scripts may be plain vanilla RedisGears functions that only use the :ref:`built-in runtime functions <runtime>`, and does not import `redgrease` or use any of its features. 
In this case the `redgrease` package does not need to be installed on the runtime.

If the function is importing and using any RedGrease construct from the ``redgrease`` package, then when calling :meth:`redgrease.Gears.pyexecute` method, the ``enforce_redgrease`` must be set in order to ensure that the package is installed on the RedisGears runtime.

In most cases you would just set it to ``True`` to get the latest stable RedGrease runtime package, but you may specify a specific version or even repository.

A notable special case is when functions in the script are only importing RedGrease modules that do not require any 3rd party dependencies (see list in the :ref:`adv_extras` section). 
If this is the case then you may want to set ``enforce_redgrease="redgrease"`` (without the extras `"[runtime]"`), when calling :meth:`redgrease.Gears.pyexecute`, as this is a version of redgrease without any external dependencies. 

Another case is when you are only using explicitly imported :ref:`runtime` (e.g. ``from redgrease.runtime import GB, logs, execute``) , and nothing else, as you in this case do not need any version of RedGrease on your RedisGears server runtime. In this case you can actually set ``enforce_redgrease=False``.

More details about the various runtime installation options, which modules and functions are impacted, as well as the respective 3rd party dependencies can be found in the :ref:`adv_extras` section.

.. Note::

    By default all Gear functions run in a shared runtime environment, and as a consequence all requirements / dependencies from different Gear functions are all installed in the same Python environment.


.. _exe_gear_function_obj:

GearFunction Object
-------------------

You can dynamically create a GearFunction object, directly in the same application as your Gears client / connection, by using any of the constructs well talk about in the :ref:`gearfun` section, such as for example :ref:`gearfun_builder` or the "Reader" classes such as :ref:`gearfun_reader_keysreader` and :ref:`gearfun_reader_streamreader` etc.

GearFunction objects can be executed in three different ways; Using :ref:`pyexecute <exe_gear_function_obj_pyexecute>`, the :ref:`on-method <exe_gear_function_obj_on_meth>` or directly in :ref:`run or execute <exe_gear_function_obj_on_closing>`.

.. note::

    There are two drawbacks of executing GearFunction objects, compared to executing Gear functions using either raw string, or by script file:

    #. The Python version of local application must match the the Python version in the RedisGears runtime (Python 3.7, at the time of writing this).

        When executing Gear functions using either raw string, or by script file, it doesn't matter which version of Python the application is using, as long as it is Python 3.6 or later and the the code in the raw string is compatible with the Python version in the RedisGears runtime, 

    #. The ``redgrease[runtime]`` package must be installed on the RedisGears Runtime environment.

        If you pass a GearFunction to :meth:`redgrease.Gears.pyexecute`, it will attempt to install the latest stable version of ``redgrease[runtime]`` on the server, unless already installed, or if explicitly told otherwise using the ``enforce_redgrease`` argument.

        When executing Gear functions using either raw string, or by script file, `redgrease` only have to be installed if the function is importing any redgrease modules, of course.


As an example let's assume, that we have instantiated a Gear client and created a very simple "open" Gear function as follows:

.. literalinclude:: ../../examples/exe_example.py
    :start-after: # # Open Function
    :end-before: # #


In this example ``get_keys_by_type`` is our simple "open" Gear function, which groups all the keys by their type ("string", "hash", "stream" etc) , and within each group collects the keys in a set. 
    
We call it "open" because it has not been "closed" by a :meth:`run() <redgrease.gears.OpenGearFunction.run>` or  :meth:`register() <redgrease.gears.OpenGearFunction.register>` action.
The output from the last operation, here :meth:`countby() <redgrease.gears.OpenGearFunction.countby>`, can therefore be used as input for a subsequent operations, if we'd like. The chain of operations of the function is "open-ended", if you will.

Once an "open" function is terminated with either a :meth:`run() <redgrease.gears.OpenGearFunction.run>`  or  :meth:`register() <redgrease.gears.OpenGearFunction.register>` action, it is considered "closed", and it can be executed, but not further extended.

The :ref:`gearfun` section, goes into more details of these concepts and the different classes of GearFunctions.


RedGrease allows for open Gear functions, such as ``key_counter`` to be used as a starting point for other Gear functions, so lets create two "closed" functions from it:

.. literalinclude:: ../../examples/exe_example.py
    :start-after: # # Closed Functions
    :end-before: # #
    :emphasize-lines: 5, 12

These two new functions, ``get_keys_by_type_dict`` and ``get_commonest_type``, both extend the earlier ``get_keys_by_type`` function with more operations.
The former function collates the results in a dictionary.
The latter finds the key-type that is most common in the keyspace.

Note that both functions end with the :meth:`run() <redgrease.gears.OpenGearFunction.run>` action, which indicates that the functions will run as an on-demand batch-job, but also that it is 'closed' and cannot be extended further. 

Let's execute these functions in some different ways.


.. _exe_gear_function_obj_pyexecute:

Execute with :meth:`.Gears.pyexecute` Client method
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The most idiomatic way of executing GearFunction objects is just to pass it to :meth:`.Gears.pyexecute`:

.. literalinclude:: ../../examples/exe_example.py
    :start-after: # # Method 1
    :end-before: # #
    :emphasize-lines: 4

The result might look something like:

.. literalinclude:: ../../examples/exe_example.py
    :start-after: """result_1 =
    :end-before: """


If you pass an "open" gear function, like our initial ``get_keys_by_type``,  to :meth:`.Gears.pyexecute`, it will still try its best to execute it, by assuming that you meant to close it with an empty :meth:`run() <redgrease.gears.OpenGearFunction.run>` action in the end:

.. literalinclude:: ../../examples/exe_example.py
    :start-after: # # Method 3
    :end-before: # #
    :emphasize-lines: 2

The result from our function might look something like:

.. literalinclude:: ../../examples/exe_example.py
    :start-after: """result_3 =
    :end-before: """


.. _exe_gear_function_obj_on_meth:

Execute with :meth:`.ClosedGearFunction.on` GearFunction method
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Another short-form way of running a closed GearFunction is to call its its :meth:`on() <.ClosedGearFunction.on>` method.

.. literalinclude:: ../../examples/exe_example.py
    :start-after: # # Method 2
    :end-before: # #
    :emphasize-lines: 4

This approach only works with "closed" functions, but works regardless if the function has been closed with the :meth:`run() <redgrease.gears.OpenGearFunction.run>` or :meth:`register() <redgrease.gears.OpenGearFunction.register>` action.

The result for our specific function might look something like:

.. literalinclude:: ../../examples/exe_example.py
    :start-after: """result_2 =
    :end-before: """

The API specification is as follows:

.. automethod:: redgrease.gears.ClosedGearFunction.on

.. _exe_gear_function_obj_on_closing:

Execute directly in :meth:`run() <.OpenGearFunction.run>` or :meth:`register() <.OpenGearFunction.register>`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

An even more succinct way of executing GearFunction objects is to specify the target connection directly in the action that closes the function. I.e the :meth:`run() <redgrease.gears.OpenGearFunction.run>`  or  :meth:`register() <redgrease.gears.OpenGearFunction.register>` action.

RedGrease has extended these methods with a couple additional arguments, which are not in the standard RedisGears API:

- ``requirements`` - Takes a list of requirements / external packages that the function needs installed.

- ``on`` - Takes a Gears (or RedisGears) client and immediately executes the function on it.

.. literalinclude:: ../../examples/exe_example.py
    :start-after: # # Method 4
    :end-before: # #
    :emphasize-lines: 4

This approach only works with "closed" functions, but works regardless if the function has been closed with the :meth:`run() <redgrease.gears.OpenGearFunction.run>`  or  :meth:`register() <redgrease.gears.OpenGearFunction.register>` action.

The result for our specific function should be identical to when we ran the function using :ref:`pyexecute <exe_gear_function_obj_pyexecute>`:

.. literalinclude:: ../../examples/exe_example.py
    :start-after: """result_4 =
    :end-before: """


``pyexecute`` API Reference
---------------------------

.. automethod:: redgrease.Gears.pyexecute


``on`` API Reference
---------------------------

.. automethod:: redgrease.ClosedGearFunction.on


.. include :: footer.rst

