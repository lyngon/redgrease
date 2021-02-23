[![Redis Gears](https://img.shields.io/badge/Redis-Gears-DC382D?logo=redis)](https://redislabs.com/modules/redis-gears/)
[![License](https://img.shields.io/github/license/lyngon/redgrease)](https://mit-license.org/)
[![PyPI Version](https://img.shields.io/pypi/v/redgrease)](https://pypi.org/project/redgrease/#history)
[![PyPI - Status](https://img.shields.io/pypi/status/redgrease)](https://pypi.org/project/redgrease)
[![PyPI Downloads](https://img.shields.io/pypi/dw/redgrease)](https://pypi.org/project/redgrease)
[![PyPI PythonVersion](https://img.shields.io/pypi/pyversions/redgrease)](https://www.python.org/)
[![Language grade: Python](https://img.shields.io/lgtm/grade/python/g/lyngon/redgrease.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/lyngon/redgrease/context:python)
[![Total alerts](https://img.shields.io/lgtm/alerts/g/lyngon/redgrease.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/lyngon/redgrease/alerts/)
[![Build Status](https://travis-ci.com/lyngon/redgrease.svg?branch=main)](https://travis-ci.com/lyngon/redgreaseanders)
[![codecov](https://codecov.io/gh/lyngon/redgrease/branch/main/graph/badge.svg?token=pQZbBVxxmm)](https://codecov.io/gh/lyngon/redgrease)
[![Dependencies status](https://img.shields.io/librariesio/github/lyngon/redgrease)](https://libraries.io/pypi/redgrease)
[![GitHub last commit](https://img.shields.io/github/last-commit/lyngon/redgrease)](https://github.com/lyngon/redgrease/pulls?q=is%3Apr+is%3Aclosed)
[![GitHub Open bugs](https://img.shields.io/github/issues-raw/lyngon/redgrease/bug?label=open%20bugs)](https://github.com/lyngon/redgrease/issues?q=is%3Aissue+is%3Aopen+label%3Abug)
[![GitHub Closed issues](https://img.shields.io/github/issues-closed-raw/lyngon/redgrease?color=informational)]()
[![Lines of Code](https://img.shields.io/tokei/lines/github/lyngon/redgrease?label=LOC)](https://github.com/lyngon/redgrease/pulse)
[![Language Python](https://img.shields.io/github/languages/top/lyngon/redgrease)](https://www.python.org/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Docker Image](https://img.shields.io/docker/v/lyngon/redisgears?label=lyngon%2Fredisgears&logo=docker)](https://hub.docker.com/r/lyngon/redisgears)

[![mushroom, mushroom!](https://img.shields.io/badge/mushroom-mushroom!-green)](https://www.youtube.com/watch?v=hGlyFc79BUE)

<!-- Hopefuully true soon.
```python
import redgrease
import redgrease.client
import redgrease.data
import redgrease.reader
import redgrease.utils


def read_user_permissions(record) -> dict:
    return redgrease.cmd.hget(
        record.key,
        mapping={
            "active": bool,
            "permissions": redgrease.utils.list_parser(str),
        },
    )


# Partial Gear function, w. default run param:
active_users = (
    redgrease.reader.KeysOnlyReader("user:*")
    .map(redgrease.data.Record.from_redis)
    .map(read_user_permissions)
    .filter(lambda usr: usr["active"])
)

# Partial Gear function re-use:
active_user_count = active_users.count()

all_issued_permissions = active_users.flatmap(lambda usr: usr.permissions).distinct()

# Redis Client w. Gears
r = redgrease.client.RedisGears()

# Two ways of running:
count = r.gears.pyexecute(active_user_count.run())
permissions = all_issued_permissions.run().on(r)
```
-->
# RedGrease
RedGrease is a Python package and set of tools to facilitate development against [Redis](https://redis.io/) in general and [Redis Gears](https://redislabs.com/modules/redis-gears/) in particular.


RedGrease consists of the followinig, components:

1. [A Redis / Redis Gears client](https://redgrease.readthedocs.io) (`redgrease.client.RedisGears`), which is an extended version of the [redis](https://pypi.org/project/redis/) client, but with additional pythonic functions, mapping closely (1-to-1) to the Redis Gears command set (e.g. `RG.PYEXECUTE`, `RG.GETRESULT`, `RG.TRIGGER`, `RG.DUMPREGISTRATIONS` etc), outlined [here](https://oss.redislabs.com/redisgears/commands.html)
```python
from redgrease.client import RedisGears

gear_script = ... # Some Gear script

rg = RedisGears()
rg.gears.pyexecute(gear_script)  # <--
```

2. Some [helper runtime functions](https://redgrease.readthedocs.io) defined in `redgrease.runtime`, but also exposed at 'top-level' (`redgrease`), exposing placeholders for the built-in Redis Gears functions (e.g. `GearsBuilder`, `GB`, `atomic`, `execute`, `log` etc) that are automatically loaded into the server [runtime environment](https://oss.redislabs.com/redisgears/runtime.html). These placeholder versions provide auto completion and type hints during development, and does not clash with the actual runtime, i.e does not require redgrease to be installed on the server.
![basic hints](docs/images/basic_usage_hints.jpg)

3. [Syntactic sugar](https://redgrease.readthedocs.io) for various things like 'magic' values and strings, like the different reader names (e.g `redgrease.Reader.CommandReader`), trigger modes (e.g. `redgrease.TriggerMode.AsyncLocal`) and log levels (e.g. `redgrease.LogLevel.Notice`). 
```python
from redgrease import GB, execute, hashtag, Reader, TriggerMode, cmd


cap = GB(Reader.StreamReader)  # <--
cap.foreach(lambda x:
            cmd.xadd(f'expired:{hashtag()}', '*', 'key', x['key']))
cap.register(prefix='*',
             mode=TriggerMode.Async,  # <-- 
             eventTypes=['expired'],
             readValue=False)
```

4. [Servers-side Redis commands](https://redgrease.readthedocs.io), allowing for **all** Redis (v.6) commands to be executed on serverside as if using a Redis 'client' class, instead of 'manually' invoking the `execute()`. It is basically the [redis](https://pypi.org/project/redis/) client, but with `execute_command()` rewired to use the Gears-native `execute()` instead under the hood. 
```python
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

4. [Loader CLI](https://redgrease.readthedocs.io) and Docker image, for automatic loading of Gears scripts, mainly "trigger-based" CommandReader Gears, into a Redis Gears cluster. It also provides a simple form of 'hot-reloading' of Redis Gears scripts, by continously monitoring directories containing Redis Gears scripts and automatically 'pyexecute' them on a Redis Gear instance if it detects modifications. 
The purpose is mainly to streamline development of 'trigger-style' Gear scripts by providing a form of hot-reloading functionality.
```
redgrease --server 10.0.2.21 --watch scripts/
```
This will 'pyexecute' the gears scripts in the 'scripts' directory against the server. It will also watch the directors for changes and re-execute the scripts if they have been modified.

5. **[Work-in-Progress]** A [remote GearsBuilder](https://redgrease.readthedocs.io), inspired by the official [redisgears-py](https://github.com/RedisGears/redisgears-py) client, but with some differences.
```python
from redgrease.client import RedisGears
from redgrease.reader import CommandReader
from redgrease import log

def process(x):
    log(f"Processing '{x}'")
    return x

my_command = CommandReader().flatmap(lambda x: x)
my_command.register(trigger="bang")

rg = RedisGears()
my_command.on(rg)

rg.gears.trigger("bang")
```

6. **[In Backlog]** Other boilerplate or otherwise functions, that may commonly be used in gears. e.g:
    - A simple `records()` function  that can be used to transform the default `KeysReader` dict to an `Records` object with the appropriate attributes. Maybe even sertter for the value (?)
    ```python
    KeysReader().map(redgrease.record).foreach(lambda r: log(f"The key '{r[.key]}' is of type '{r.type}' with value '{r.value}'")
    ```
    - Helpers to aid debugging and/ or testing of gears. 
    - ...

Suggestions appriciated.

# Installation
Redgrease may be installed either as a developmet tool only, a client library and/or as a runtime library on the Redis Gears server.
It can be installed with different 'extras' dependencies depending on preferred usage. 

## Development / Client Environment Install
In the environment where you develop your Gears scripts, simply install 'redgrease' with pip3, as usual:
```
python3 -m pip install redgrease[all]
```
This installs all the dependencies, allowing for the full features set.


`reagrease[cli]` : Installs dependencies for the CLI

## Runtime / Server Environment Install
It is recomendede to use the `redgreese[runtime]` package as a serverside dependency. 
This installs dependencies for the all the serverside features such as serverside Redis commands and the runtime for gears constructed with the 'Remote Gears Builder'

```python
from redgrease.client import RedisGears

rg = RedisGears()
rg.gears.pyexecute("", requirements=["redgrease[runtime]"])
```

The current 3rd party packages in the `runtime` extras are:
- [attrs](https://pypi.org/project/attrs/) - For parsing composite Gears response structures into attrs objects
- [redis](https://pypi.org/project/redis/) - Needed for server-side redis commands
- [cludpickle](https://pypi.org/project/cloudpickle/) - Needed for the "Remote" gears (similarly to the official [redisgears-py](https://github.com/RedisGears/redisgears-py) client)

### Note on the runtime environment
If the only thing you need is the redgrease client and/or docstings, typhints and the loader CLI for development of conventional Gears scripts (i.e that only use the standard commands and loaded as strings), **then the RedGrease package is not strictly required to be installed** in the Redis Gears Python Runtime environment (i.e. on the server)  

You can in this case simply remove the redgrease import clause from your script, after development but before `pyexecuting` them as per the [Redis Gears documentation](https://oss.redislabs.com/redisgears/intro.html). Such scripts will still run perfectly fine without redgrease in Redis Gears Environment.

A minimal install, without any  3-rd party dependencies, which is pretty much only the syntactic sugar and runtime placeholders, can be installed usig the bare 'redgrease' package. 

This migth be useful if you really don't want the 3rd party packages in the server runtime but still want to use the redgrese sugar.

You can also use the RedGrease watcher or loader CLI to automate loading your scripts as well as requirements from a normal 'requirements.txt' files, as outlined [here](https://github.com/lyngon/redgrease) 

# Usage / Documentation
The Documenttion is work-in-progress, but the latest and greatest version is available here: 
## https://redgrease.readthedocs.io

Go read the docs!
# Testing
Tests are separate from the package, but are available in the [GitHub repo](https://github.com/lyngon/redgrease).
```
git clone https://github.com/lyngon/redgrease
``` 

In order to run the tests, [Docker](https://docs.docker.com/get-docker/) is required to be installed in order to spin up fresh Redis instances, on demand for the tests. 

PyTest and quite a number of its add-ons, as well as Tox, is also needed to run the tests properly. All test (and dev) requirements is best installed through:
```
cd redgrease/
python3 -m venv .venv
source .venv/bin/activate
pip install -r src/requirements-dev.txt
```
Then the tess can be run with PyTest as per usual:
```
pytest 
```

**Note:** Running the tests takes excruciatingly long, sas a new fresh Redis instance is spun up **for each test**, just to ensure no risk of cross-contamination. This may be optimized, as many tests are actually independent, but it's left like this for now.

# Why This?
The need for this arose from wanting to prototype the concepts for a new Redis module, using Gears CommandReaders and triggers instead of having to write full fledged module in C.

Intitially RedGrease was just a very simple module, with placeholders for the default Redis gears runtime functions, with type hints and docstrings, just to make it more convenient and less error prone to write Gears functions.

Then the loader cli was created, in order to furthure speed up the rapid development cycle. 

Then the server-side Redis 'client' commands function was addes to minimize errors (E.g. mispelled command strings).

Then the client was added ... and before long it started to get a life of its own. 

Note that this means RedGrease package is primarily intended to be an aid for **development** of Gears scripts, and was not originally intended to be used in any "production" software. 

This intent has now changed, and the new goal is now to make Redgrease a production grade package for Redis Gears.
Granted, there is stilll quite some way to go to get there, so your support and feedback is greatly appriciated.

If you like this project, or want professional support, please consider [sponsoring](https://github.com/sponsors/lyngon).