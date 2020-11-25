# RedGrease
Simple helper package for auto completion of RedisGears stuff, like for example 'GearsBuilder' and its functions.

The package is mainly intended to aid development of Gears scripts, and it is not required to be installed in the Redis Gears Runtime, although it may be the most convenient approach.

## Installation
### Development Environment
In the environment where you develop your Gears scripts, simply install 'redgrease' with pip3, as usual:
```
python3 -m pip install redgrease
```

### Redis Gears Runtime Environment
The RedGrease package is NOT required to be installed in the Redis Gears Python Runtime environment, in order to run conventional Gears scripts, even if RedGrease was used for development.
If you remove the redgrease import clause from your script, or wrap it in a check as outlined on the "Slightly more Advanced Usage" section, the scripts will still run perfectly fine without redgrease in Redis Gears Environment.

However, it is also perfectly safe to leave the redgrease import clause in your script and add redgrease as a requirement to your script.

RedGrease will detect when it is in an actual RedisGears environment and will not load any symbols that conflict with the built-in Redis Gears Python environment. 

In this case, you would simply load the scripts with 'redgrease' as a requirement the conventional way:
```
redis-cli RG.PYEXCUTE "$(cat yourscript.py)" REQUIRE redgrease
```
Alternative you can use the RedGrease watcher or loader to automate loading requirements from a normal'requirements.txt' file, as outlined [here](https://github.com/lyngon/redgrease) 

## Usage
### Basic Development 
Simply import the whole RedGrease package in your Gears script to load the symbols as if in the Redis Gears environment:

```
from redgrease.runtime import *
```
This will enable auto completion in your IDE, for Redis Gears stuff. Example from Redis Gears Introduction:
```
from redgrease.runtime import *

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
    from redgrease.runtime import *
except ModuleNotFoundError:
    pass
```
Then you will have access to all the autocompletion etc during development, but not have to install redgrease in the Redis Gears Python Runtime Environment.
