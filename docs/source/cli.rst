.. include :: banner.rst

.. _cli:

Command Line Tool
=================

.. include :: wip.rst

`redgrease` can be invoked from the CLI::

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


.. include :: footer.rst