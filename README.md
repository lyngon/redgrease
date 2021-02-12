# RedGrease
RedGrease is a Python package and set of tools to facilitate development against [Redis](https://redis.io/) in general and [Redis Gears](https://redislabs.com/modules/redis-gears/) in particular.


RedGrease consists of the followinig, components:

- [A Redis / Redis Gears client](#client) (`redgrease.client.RedisGears`), which is an extended version of the [redis](https://pypi.org/project/redis/) client, but with additional pythonic functions, mapping closely (1-to-1) to the Redis Gears command set (e.g. `RG.PYEXECUTE`, `RG.GETRESULT`, `RG.TRIGGER`, `RG.DUMPREGISTRATIONS` etc), outlined [here](https://oss.redislabs.com/redisgears/commands.html)
```
from redgrease.client import RedisGears

rg = RedisGears()
rg.gears.pyexecute("GB().run()")  # <--
```

- Some [helper runtime functions](#builtin-runtime-functions) defined in(#builtin-runtime-functions) `redgrease.runtime`, but also exposed at 'top-level' (`redgrease`), exposing placeholders for the built-in Redis Gears functions (e.g. `GearsBuilder`, `GB`, `atomic`, `execute`, `log` etc) that are automatically loaded into the server [runtime environment](https://oss.redislabs.com/redisgears/runtime.html). These placeholder versions provide auto completion and type hints during development, and does not clash with the actual runtime, i.e does not require redgrease to be installed on the server.
![basic hints](docs/images/basic_usage_hints.jpg)

- [Syntactic sugar](#syntactic-sugar) for various things like 'magic' values and strings, like the different reader names (e.g `redgrease.Reader.CommandReader`), trigger modes (e.g. `redgrease.TriggerMode.AsyncLocal`) and log levels (e.g. `redgrease.LogLevel.Notice`). 
```
from redgrease import GB, execute, hashtag, Reader, TriggerMode


cap = GB(Reader.StreamReader)  # <--
cap.foreach(lambda x:
            execute('XADD', f'expired:{hashtag()}', '*', 'key', x['key']))
cap.register(prefix='*',
             mode=TriggerMode.Async,  # <-- 
             eventTypes=['expired'],
             readValue=False)
```

- [Servers-side Redis commands](#serverside-redis-commands), allowing for all Redis (v.6) commands to be executed on serverside as if using a Redis 'client' class, instead of 'manually' invoking the `execute()`. It is basically the [redis](https://pypi.org/project/redis/) client, but with `execute_command` rewired to use the Gears-native `execute` instead under the hood. 
```
import redgrease


def double(record):
    try:
        key = record["key"]
        val = redgrease.cmd.get(key)  # <--
        newval = float(val)
    except Exception as ex:
        err_msg = f"The value of the key {key} did not float very well"
        redgrease.log(err_msg + f": {ex}. Blubb, blubb!")
        raise TypeError(err_msg) from ex
    else:
        val *= 2
        redgrease.cmd.set(key, val)  # <--


redgrease.GB(redgrease.Reader.KeysReader).foreach(double).run()
```

- [Loader CLI](#loader-cli) and Docker image, for automatic loading of Gears scripts, mainly "trigger-based" CommandReader Gears, into a Redis Gears cluster. It also provides a simple form of 'hot-reloading' of Redis Gears scripts, by continously monitoring directories containing Redis Gears scripts and automatically 'pyexecute' them on a Redis Gear instance if it detects modifications. 
The purpose is mainly to streamline development of 'trigger-style' Gear scripts by providing a form of hot-reloading functionality.


- **[Work-in-Progress]** A [remote GearsBuilder](#remote-gears), inspired by the official [redisgears-py](https://github.com/RedisGears/redisgears-py) client, but with some differences.
```
from redgrease.client import RedisGears
from redgrease.reader import CommandReader
from redgrease import log

rg = RedisGears()

def process(x):
    log(f"Processing '{x}')
    return x

my_command = CommandReader().flatmap(lambda x: x)
my_command.register(rg, trigger="bang")

rg.gears.trigger("bang")
```

- **[Soonish]** Other useful and/or boilerplate functions, commonly used in gears. Suggestions appriciated. 

# Why?

Note that the RedGrease package is primarily intended to aid development of Gears scripts, and can be useful even if it is not installed in the Redis Gears runtime environment, although it may be the most convenient approach.

# Installation
### Development Environment
In the environment where you develop your Gears scripts, simply install 'redgrease' with pip3, as usual:
```
python3 -m pip install redgrease[all]
```
This installs all the dependencies, allowing for the full features set.


`reagrease[cli]' : Installs dependencies for the cli

## Runtime Install
A minimal install, with no 3-rd party dependencies, which is pretty much only the syntactic sugar and runtime placeholders, can be installed usig the bare 'redgrease' package. This migth be useful if you want a clean server runtime but still want the sugar.

Otherwise it is recomendede to use the `redgreese[runtime]' package as a serverside dependency. 
This installs dependencies for the all the serverside features such as serverside Redis commands and the runtime for gears constructed with the 'Remote Gears Builder'

```
from redgrease.client import RedisGears

rg = RedisGears()
rg.gears.pyexecute("GB().run()", requirements=["redgrease[runtime]"])
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

# Usage

## Client
Documentation is Work-in-Progress.
Comming soon, hopefully. 
## Builtin Runtime Functions
You can load the default redisgears symbols (e.g. `GearsBuilder`, `GB`, `atomic`, `execute`, `log` etc) from the `redgrease.runtime` package. 

During development this will give you auto-completion and type hints
In the Redis Gears Python runtime, all the `redgrease.runtime` are mapped directly to the normal ones wihtout side-effects.

```
from redgrease import GearsBuilder, log, atomic, execute
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

## Syntactic Sugar
Documentation is Work-in-Progress.
Comming soon, hopefully. 

## Serverside Redis Commands
Documentation is Work-in-Progress.
Comming soon, hopefully. 

## Loader CLI
`redgrease` can be invoked from the CLI:
```
redgrease --help
usage: redgrease [-h] [-c PATH] [--index-prefix PREFIX] [-r] [--script-pattern PATTERN] [--requirements-pattern PATTERN] [--unblocking-pattern PATTERN] [-i PATTERN] [-w [SECONDS]] [-s [SERVER]] [-p PORT] [-l LOG_CONFIG] dir_path [dir_path ...]

Scans one or more directories for Redis Gears scripts, and executes them in a Redis Gears instance or cluster. Can optionally run continiously, montoring and re-loading scripts whenever changes are detected. Args that start with '--' (eg. --index-prefix) can also be set in a config file
(./*.conf or /etc/redgrease/conf.d/*.conf or specified via -c). Config file syntax allows: key=value, flag=true, stuff=[a,b,c] (for details, see syntax at https://goo.gl/R74nmi). If an arg is specified in more than one place, then commandline values override environment variables which override
config file values which override defaults.

positional arguments:
  dir_path              One or more directories containing Redis Gears scripts to watch

optional arguments:
  -h, --help            show this help message and exit
  -c PATH, --config PATH
                        Config file path [env var: CONFIG_FILE]
  --index-prefix PREFIX
                        Redis key prefix added to the index of monitored/executed script files. [env var: INDEX_PREFIX]
  -r, --recursive       Recursively watch subdirectories. [env var: RECURSIVE]
  --script-pattern PATTERN
                        File name pattern (glob-style) that must be matched for scripts to be loaded. [env var: SCRIPT_PATTERN]
  --requirements-pattern PATTERN
                        File name pattern (glob-style) that must be matched for requirement files to be loaded. [env var: REQUIREMENTS_PATTERN]
  --unblocking-pattern PATTERN
                        Scripts with file paths that match this regular expression, will be executed with the 'UNBLOCKING' modifier, i.e. async execution. Note that the pattern is a 'search' pattern and not anchored to thestart of the path string. [env var: UNBLOCKING_PATTERN]
  -i PATTERN, --ignore PATTERN
                        Ignore files matching this pattern. [env var: IGNORE]
  -w [SECONDS], --watch [SECONDS]
                        If set, the directories will be continously montiored for updates/modifications to scripts and requirement files, and automatically loaded/rerun. The flag takes an optional value specifying the duration, in seconds, to wait for further updates/modifications to files,
                        before executing. This 'hysteresis' period is to prevent malformed scripts to be unnecessarily loaded during coding. If no value is supplied, the duration is defaulting to 5 seconds. [env var: WATCH]
  -s [SERVER], --server [SERVER]
                        Redis Gears host server IP or hostname. [env var: SERVER]
  -p PORT, --port PORT  Redis Gears host port number [env var: PORT]
  -l LOG_CONFIG, --log-config LOG_CONFIG
                        [env var: LOG_CONFIG]
```

## Remote Gears
Documentation is Work-in-Progress.
Comming soon, hopefully. 

# Testing
Tests are separate from the package, but are available in the GitHub repo. 
PyTest, a number of add-ons, as well as tox, and docker is required to run the entire test suite.
Note that tests takes excrutiatingly long as a new fresh Redis instance is spun up for each test, to ensure no risk of cross-contamination.
