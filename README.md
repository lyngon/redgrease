# RedGrease
Simple package to facilitate development of Redis Gears Python scripts.

RedGrease consists of the followinig:
- A helper package `redgrease.runtime` that contains the standard redisgears script functions (e.g. `GearsBuilder`, `GB`, `atomic`, `execute`, `log` etc), but that provide auto completion and type hints during development, and does not clash with the actual runtime.
- Syntactic sugar for various things like 'magic' values and strings, like the different reader names (e.g `redgrease.Reader.CommandReader`), trigger modes (e.g. `redgrease.TriggerMode.AsyncLocal`) and log levels (e.g. `redgrease.LogLevel.Notice`). 
- **[WIP]** A simple Redis client `redgrease.client.Redis` extended with pythonic functions, mapping closely (1-to-1) to the Redis Gears command set (e.g. `RG.PYEXECUTE`, `RG.GETRESULT`, `RG.TRIGGER`, `RG.DUMPREGISTRATIONS` etc)
- **[Comming Later]** A remote GearsBuilder, inspired by the official [redisgears-py](https://github.com/RedisGears/redisgears-py) client, but with some differences.
- **[Maybe Sometime]** Other useful functions. Suggestions appriciated. 

Note that the RedGrease package is primarily intended to aid development of Gears scripts, and can be useful even if it is not installed in the Redis Gears runtime environment, although it may be the most convenient approach.

There is also **[soon]** a 'watcher' script / Docker container providing a simple form of 'hot-reloading' of Redis Gears scripts, by continously monitoring directories containing Redis Gears scripts and automatically 'pyexecute' them on a Redis Gear instance if it detects modifications. 
The purpose is mainly to streamline development of 'trigger-style' Gear scripts by providing a form of hot-reloading functionality.

## Installation
### Development Environment
In the environment where you develop your Gears scripts, simply install 'redgrease' with pip3, as usual:
```
python3 -m pip install redgrease
```

### Redis Gears Runtime Environment
The RedGrease package is NOT required to be installed in the Redis Gears Python Runtime environment, in order to run conventional Gears scripts that only use the standard commands, even if RedGrease was used for development.

If you remove the redgrease import clause from your script, or wrap it in a check as outlined on the "Slightly more Advanced Usage" section, such scripts will still run perfectly fine without redgrease in Redis Gears Environment.

However, it is also perfectly safe to install the `redgrease` package on the Redis Gears server, and leave the redgrease import clauses in your scripts. It is probably more convenient.

In this case, you would simply load the scripts with 'redgrease' as a requirement the conventional way:
```
redis-cli RG.PYEXCUTE "$(cat yourscript.py)" REQUIRE redgrease
```
Alternative you can use the RedGrease watcher or loader to automate loading your scripts as well as requirements from a normal 'requirements.txt' files, as outlined [here](https://github.com/lyngon/redgrease) 

## Usage
### Basic Development 
You can load the default redisgears symbols (e.g. `GearsBuilder`, `GB`, `atomic`, `execute`, `log` etc) from the `redgrease.runtime` package. 

During development this will give you auto-completion and type hints
In the Redis Gears Python runtime, all the `redgrease.runtime` are mapped directly to the normal ones wihtout side-effects.

```
from redgrease.runtime import GearsBuilder, log, atomic, execute
```

This will enable auto completion in your IDE, for Redis Gears stuff. Example from Redis Gears Introduction:

RedGrease's `runtime` package will detect when it is imported inside an actual RedisGears Python runtime environment and will then load the default redis gears symbols, avoiding conflict with the built-in Redis Gears Python environment.
If it is loaded outside a Redis Gears Python runtime environment, i.e. a development environment, the `redgrease.runtime` package will instead load placeholder symbols with decent (hopefully) doc-strings and type-hints, for easier development.

It is possible to load all symbols, using `*`, but it's generally not a recomended practice.

#### Example
Below is an ex
```
from redgrease.runtime import GearsBuilder, execute

def age(x):
    ''' Extracts the age from a person's record '''
    return int(x['value']['age'])

def cas(x):
    ''' Checks and sets the current maximum '''
    k = 'age:maximum'
    v = execute('GET', k)   # read key's current value
    v = int(v) if v else 0  # initialize to 0 if None
    if x > v:               # if a new maximum found
    execute('SET', k, x)  # set key to new value

# Event handling function registration
gb = GearsBuilder()
gb.map(age)
gb.foreach(cas)
gb.register('person:*')

```

### Slightly more Advanced Usage
If you don't want to have the redgrease package in your Redis Gears Python Runtime Environment, you can  simply load it conditionally as follows:
```
try:
    from redgrease.runtime import GearsBuilder, execute
except ModuleNotFoundError:
    pass
```
Then you will have access to all the auto completion and type hins etc during development, but not have to install redgrease in the Redis Gears Python Runtime Environment.
