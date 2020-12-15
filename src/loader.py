import logging
from time import sleep
from fnmatch import fnmatch
from datetime import datetime
from os.path import isfile
from pathlib import Path
import re

from redis.exceptions import ResponseError

from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler

from hysteresis import HysteresisHandlerIndex

from redgrease import client
from redgrease import requirements

import configargparse

args = configargparse.ArgParser(
    description="Scans one or more directories for Redis Gears scripts, and "
    "executes them in a Redis Gears instance or cluster, "
    "potentially continiousl montoring and re-loading whenever changes are "
    "detected.",
    default_config_files=['./*.conf', '/etc/redgrease/conf.d/*.conf']
)

args.add_argument(
    'directories',
    nargs='+',
    type=Path,
    help="One or more directories containing Redis Gears scripts to watch",
    )

args.add_argument(
    '-c',
    '--config',
    env_var='CONFIG_FILE',
    metavar="PATH",
    is_config_file=True,
    help="Config file path",
)
args.add_argument(
    '--index-prefix',
    env_var='INDEX_PREFIX',
    metavar="PREFIX",
    required=False,
    type=str,
    default="/redgrease/scripts",
    help="Redis key prefix added to the index of monitored/executed script "
    "files.",
    )
args.add_argument(
    '-r',
    '--recursive',
    env_var='RECURSIVE',
    action='store_true',
    help="Recursively watch subdirectories.",
)
args.add_argument(
    '--script-pattern',
    env_var='SCRIPT_PATTERN',
    metavar="PATTERN",
    required=False,
    type=str,
    default="*.py",
    help="File name pattern (glob-style) that must be matched for scripts to "
    "be loaded.",
)
args.add_argument(
    '--requirements-pattern',
    env_var="REQUIREMENTS_PATTERN",
    metavar="PATTERN",
    required=False,
    type=str,
    default="*requirements*.txt",
    help="File name pattern (glob-style) that must be matched for requirement "
    "files to be loaded.",
)
args.add_argument(
    "--unblocking-pattern",
    env_var="UNBLOCKING_PATTERT",
    metavar="PATTERN",
    required=False,
    type=re.compile,
    default="unblock",
    help="Scripts with file paths that match this regular expression, "
    "will be executed with the 'UNBLOCKING' modifier, i.e. async execution."
)
args.add_argument(
    '-i',
    '--ignore',
    env_var="IGNORE",
    metavar='PATTERN',
    action='append',
    required=False,
    help="Ignore files matching this pattern.",
)
args.add_argument(
    '-w',
    '--watch',
    env_var='WATCH',
    metavar="SECONDS",
    nargs='?',
    type=float,
    default=0.0,
    const=5.0,
    help="If set, the directories will be continously montiored for updates/"
    "modifications to scripts and requirement files, and automatically loaded/"
    "rerun. The flag takes an optional value specifying the  duration, "
    "in seconds, to wait for further updates/modifications to files, before "
    "executing. "
    "This 'hysteresis' period is to prevent malformed scripts to be "
    "unnecessarily loaded during coding. "
    "If no value is supplied, the duration is defaulting to 5 seconds."
)
args.add_argument(
    '-s',
    '--server',
    env_var="SERVER",
    type=str,
    const='redis',
    default='localhost',
    nargs='?',
    help="Redis Gears host server IP or hostname."
)
args.add_argument(
    '-p',
    '--port',
    env_var='PORT',
    type=int,
    default=6379,
    help="Redis Gears host port number"
)
args.add_argument(
    '-log-name',
    env_var='LOG_NAME',
    metavar="NAME",
    type=str,
    help="Log name. Default: '__main__'"
)
args.add_argument(
    '--log-file',
    env_var='LOG_FILE',
    metavar="PATH",
    type=str,
    help="Log to this file name."
)
args.add_argument(
    '--log-no-stdout',
    env_var="LOG_NO_STDOUT",
    dest='log_to_stdout',  # Note: arg is negative, but var is positive
    action='store_false',
    help="Do not log to stdout, if set"
)
args.add_argument(
    '--log-level',
    env_var='LOG_LEVEL',
    metavar="LEVEL",
    choices=['CRITICAL', 'ERROR', 'WARNINING', 'INFO', 'DEBUG', 'NOTSET'],
    default='DEBUG',
    help="Logging level",
)

config = args.parse_args()
print(config)

# Logging matters
iso8601_format = "%Y-%m-%dT%H:%M:%S.%f"


class UTC_ISO8601_Formatter(logging.Formatter):
    converter = datetime.utcfromtimestamp

    def formatTime(self, record, datefmt=None):
        ct = self.converter(record.created)
        if datefmt:
            s = ct.strftime(datefmt)
        else:
            s = ct.strftime(iso8601_format)
        return s


log_fmt = UTC_ISO8601_Formatter(
    fmt="%(asctime)s %(levelname)s %(name)s: %(message)s"
)
log = logging.getLogger(config.log_name)
log.setLevel(config.log_level)

if config.log_to_stdout:
    stdout_log = logging.StreamHandler()
    stdout_log.setLevel(config.log_level)
    stdout_log.setFormatter(log_fmt)
    log.addHandler(stdout_log)

if config.log_file:
    file_log = logging.FileHandler(config.log_file)
    file_log.setLevel(config.log_level)
    file_log.setFormatter(log_fmt)
    log.addHandler(file_log)
# End of Logging matters

redis = client.RedisGears(
    host=config.server,
    port=config.port,
    # TODO: Add more redis options configurable parameters
)


def fail(exception, *messages):
    """Convenience function for raising exceptions

    Args:
        exception ([Exception]): Exception to raise

        *messages (str): Any number of messages.
        Will be separated by full stop/period (.).

    Raises:
        exception: Exception
    """
    error_message = ". ".join(messages)
    log.error(error_message)
    raise exception(error_message)


# Execute / Register script in redis
# TODO: Should be integrated into redgrease.RedisGears class
# TODO: Handle multiple registrations per script.
def register_script(script_path):
    """Execute / Register a gear script in redis using 'RG.PYEXECUTE'

    Args:
        script_path (str): Path to the script.

        Note: paths that maches the 'unblocking-pattern', will be executed
        with the 'UNBLOCKING' modifier.
    """
    log.debug(f"Registering script '{script_path}'")
    general_failure_message = f"Unable to register script file '{script_path}'"

    if not isfile(script_path):
        fail(FileNotFoundError, general_failure_message, "File not found")

    with open(script_path) as script_file:
        script_content = script_file.read()

    if not script_content:
        fail(IOError, general_failure_message, "File is empty")

    # Unregister script if already present
    unregister_script(script_path)

    unblocking = config.unblocking_pattern.match(str(script_path)) is not None

    try:
        log.debug(
            f"Running/registering Gear script '{script_path}' on Redis server"
        )

        # This is a quite unsafe way of checking for registrations
        # Probabls Ok for dev situations in non-shared environments
        pre_reg = redis.dumpregistrations()
        exec_res = redis.pyexecute(script_content, unblocking=unblocking)
        ret = f"with return code '{exec_res}'."
        post_reg = redis.dumpregistrations()
        pre_reg = set([reg.id for reg in pre_reg])
        log.debug(f"Pre regs: {list(pre_reg)}")
        post_reg = set([reg.id for reg in post_reg])
        log.debug(f"Post regs: {list(post_reg)}")
        diff_reg: set = post_reg-pre_reg
        if len(diff_reg) > 0:
            reg_id = diff_reg.pop()
            redis.hset(
                f"{config.index_prefix}{script_path}",
                mapping={
                    'registration_id': str(reg_id),
                    'last_updated': datetime.utcnow().strftime(iso8601_format),
                }
            )
            log.debug(f"Script '{script_path}' registered as '{reg_id}' {ret}")
        else:
            log.debug(f"Script '{script_path}' executed {ret}")

        if len(diff_reg) > 0:
            log.warning(
                "Multiple registrations occured? "
                "Index migth be corrupt. "
                "Is this a shared environment?"
            )

    except Exception as ex:
        log.error(f"Something went wrong: {ex}")


# TODO: Should be integrated into redgrease.RedisGears class
# TODO: Handle multiple registrations per script.
def unregister_script(script_path):
    """Check if a given script is registered, and if so,
    unregister it using 'RG.UNREGISTER'

    Args:
        script_path (str): Script path
    """
    log.debug(f"Unregistering script: '{script_path}'")
    reg_key = f"{config.index_prefix}{script_path}"
    reg_id = redis.hget(reg_key, 'registration_id')
    if reg_id is not None:
        log.debug(
            f"Removing registration for script '{script_path}' "
            f"with id '{reg_id}'"
        )
        try:
            redis.unregister(reg_id)
        except ResponseError as err:
            log.warn(
                "Unregistration failed. "
                "Index migth be corrupt. "
                "Is this a shared environment?"
            )
            log.error({err})
    redis.delete(reg_key)


def update_dependencies(requirements_file_path):
    """Update (add only) package dependencies on the Redis instance

    Args:
        requirements_file_path (str): File path of the 'requirements.txt' file.
    """
    log.debug(f"Updating dependecies as per '{requirements_file_path}'")
    try:
        requirements_list = list(requirements.read(requirements_file_path))
        log.debug(f"Requirements to load: {', '.join(requirements_list)}")
        redis.pyexecute("GB().run()", requirements=requirements_list)
    except Exception as ex:
        log.error(f"Something went wrong: {ex}")


# File Event Handling

file_index = HysteresisHandlerIndex(config.watch)


def on_deleted(event):
    """Watchdog event handler for events signalling that a
    script or requirement file has been deleted.
    Script files are unregistered (if present) and re-run,
    after applying some hystersis.
    Removed requirement files does not remove any installed packages.

    Args:
        event (watchdog.FileDeletedEvent): Event data for deleted file
    """
    file = event.src_path
    if fnmatch(file, config.script_pattern):
        log.debug(
            f"Gears script '{file}' deleted. "
            "Scheduling unregistration of script."
        )
        # Apply hysteresis in case additional events shortly follow,
        # before we actually unregister
        file_index.signal(file, unregister_script, file)
    elif fnmatch(file, config.requirements_pattern):
        log.info(
            f"Gears requirements file '{file}' deleted. "
            "Requirement removal not Implemented. "
            "Ignoring."
        )
    else:
        log.warn(
            f"Unknown file type '{file}' deleted. "
            "Ignoring."
        )


def on_modified(event):
    """Watchdog event handler for events signalling that a
    script or requirement file has been modified.

    Args:
        event ([type]): [description]
    """
    file = event.src_path
    if fnmatch(file, config.script_pattern):
        log.debug(f"Gears script '{file}' modified. Regestering script.")
        # Apply hysteresis in case additional events shortly follow,
        # before we actually (re-)run/register the script.
        file_index.signal(file, register_script, file)
    elif fnmatch(event.src_path, config.requirements_pattern):
        log.debug(
            f"Gears requirements file '{file}' modified. "
            "Updating dependencies."
        )
        # Apply hysteresis in case additional events shortly follow,
        # before we actually update requirements.
        file_index.signal(file, update_dependencies, file)
    else:
        log.warn(f"Unknown file type '{event.src_path}' modified. Ignoring.")


def on_moved(event):
    """Watchdog event handler for events signalling that a
    script or requirement file has been moved.
    Script files are unregistered (if present) and re-run,
    after applying some hystersis.
    Removed requirement files does not remove any installed packages.

    Args:
        event ([type]): [description]
    """
    old_file = event.src_path
    new_file = event.dest_path
    # Handle, i.e. unregister, the old / source file.
    if fnmatch(old_file, config.script_pattern):
        log.debug(
            f"Gears script '{old_file}' moved to '{new_file}'. "
            "Unregistering old script."
        )
        # Apply hysteresis in case additional events shortly follow,
        # before we actually unregister the script.
        file_index.signal(old_file, unregister_script, old_file)
    elif fnmatch(old_file, config.requirements_pattern):
        log.debug(
            f"Gears requirements file '{old_file}' moved to '{new_file}'. "
            "Requirement removal not Implemented. "
            "Ignoring."
        )

    # TODO: Check that the new file is in the watch directory
    # Handle, i.e. run/register, the new / destination file
    if fnmatch(new_file, config.script_pattern):
        log.debug(
            f"File '{old_file}' moved to Gears script '{new_file}'. "
            "Registering new script."
        )
        # Apply hysteresis in case additional events shortly follow,
        # before we actually (re-)run/register the script
        file_index.signal(new_file, register_script, new_file)
    elif fnmatch(new_file, config.requirements_pattern):
        log.debug(
            f"File '{old_file}' moved to requirements file '{new_file}'. "
            "Updating dependencies"
        )
        # Apply hysteresis in case additional events shortly follow,
        # before we actually update requirements.
        file_index.signal(new_file, update_dependencies, new_file)
    else:
        log.warn(
            f"File '{old_file}' moved to unknonwn type '{new_file}'. "
            "Ignoring."
        )


# Create the file change event listener
event_handler = PatternMatchingEventHandler(
    patterns=[config.script_pattern, config.requirements_pattern],
    ignore_patterns=config.ignore,
    ignore_directories=False,
    case_sensitive=False
)

event_handler.on_deleted = on_deleted
event_handler.on_modified = on_modified
event_handler.on_moved = on_moved

observer = Observer()

for directory in config.directories:
    log.info(f"Adding event handlers for {directory}")

    # select either recursive or non-recursive glob function for the dir
    globber = directory.rglob if config.recursive else directory.glob

    # find and run/register all requirements files in the watch directories
    [update_dependencies(f) for f in globber(config.requirements_pattern)]

    # find and run/register all script files in the watch directory
    [register_script(f) for f in globber(config.script_pattern)]

    observer.schedule(event_handler, directory, config.recursive)


if config.watch > 0:
    log.info("Starting watcher!")
    observer.start()
    running = True

    try:
        while running:
            sleep(1)
    except KeyboardInterrupt:
        running = False
        print("Interrupted by user. Ending!")
    finally:
        observer.stop()

redis.close()
