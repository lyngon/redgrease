import logging
from time import sleep
from fnmatch import fnmatch
from datetime import datetime
from os.path import isfile

from redis.exceptions import ResponseError

from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler

from hysteresis import HysteresisHandlerIndex

from redgrease import client
from redgrease import requirements

# Config Parameters
# TODO: Make these command-line arguments and env-var configurable
scripts_index_prefix = "/redgrease/scripts"

watch_directories = [
    "./examples"
]
recursive = True
script_pattern = "*.py"
requirements_pattern = "*requirements*.txt"
ignore_patterns = None

hysteresis_duration = 3.0

redis_host = "localhost"
redis_port = 6379

log_name = None
log_file_path = None
log_to_stdout = True
log_level = "DEBUG"
# End Config Params


# Logging matters
# TODO: break out to separate file.
iso8601_format = "%Y-%m-%dT%H:%M:%S.%f"


class UTC_ISO8601_Formatter(logging.Formatter):
    converter = datetime.utcfromtimestamp

    def formatTime(self, record, datefmt=None):
        ct = self.converter(record.created)
        if datefmt:
            s = ct.strftime(datefmt)
        else:
            t = ct.strftime(iso8601_format)
            s = "%s,%03d" % (t, record.msecs)
        return s


if not log_name:
    log_name = __name__

log_fmt = UTC_ISO8601_Formatter(
    fmt="%(asctime)s %(levelname)s %(name)s: %(message)s"
)
log = logging.getLogger(log_name)
log.setLevel(log_level)

if log_to_stdout:
    stdout_log = logging.StreamHandler()
    stdout_log.setLevel(log_level)
    stdout_log.setFormatter(log_fmt)
    log.addHandler(stdout_log)

if log_file_path:
    file_log = logging.FileHandler(log_file_path)
    file_log.setLevel(log_level)
    file_log.setFormatter(log_fmt)
    log.addHandler(file_log)
# End of Logging matters

redis = client.RedisGears(
    host=redis_host,
    port=redis_port,
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
def register_script(script_path, unblocking=False):
    """Execute / Register a gear script in redis using 'RG.PYEXECUTE'

    Args:
        script_path (str): Path to the script.
        unblocking (bool, optional): Execute / Register as unblocking.
        Defaults to False.
    """
    log.debug(f"Registering script '{script_path}'")
    general_failure_message = f"Unable to register script file '{script_path}'"

    if not isfile(script_path):
        fail(FileNotFoundError, general_failure_message, "File not found")

    with open(script_path) as script_file:
        script_content = script_file.read()

    if not script_content:
        fail(IOError, general_failure_message, "File is empty")

    unblocking = ['UNBLOCKING'] if unblocking else []

    # Unregister script if already present
    unregister_script(script_path)
    try:
        log.debug(
            f"Running/registering Gear script '{script_path}' on Redis server"
        )

        # This is a quite unsafe way of checking for registrations
        # Probabls Ok for dev situations in non-shared environments
        pre_reg = redis.execute_command('RG.DUMPREGISTRATIONS')
        exec_res = redis.execute_command(
            'RG.PYEXECUTE',
            f'''{script_content}''',
            *unblocking
        )
        ret = f"with return code '{exec_res}'."
        post_reg = redis.execute_command('RG.DUMPREGISTRATIONS')
        pre_reg = set([reg[1] for reg in pre_reg])
        log.debug(f"Pre regs: {list(pre_reg)}")
        post_reg = set([reg[1] for reg in post_reg])
        log.debug(f"Post regs: {list(post_reg)}")
        diff_reg: set = post_reg-pre_reg
        if len(diff_reg) > 0:
            reg_id = diff_reg.pop()
            redis.hset(
                f"{scripts_index_prefix}{script_path}",
                mapping={
                    'registration_id': reg_id,
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
        log.error(f"Someting went wrong: {ex}")


# TODO: Should be integrated into redgrease.RedisGears class
# TODO: Handle multiple registrations per script.
def unregister_script(script_path):
    """Check if a given script is registered, and if so,
    unregister it using 'RG.UNREGISTER'

    Args:
        script_path (str): Script path
    """
    log.debug(f"Unregistering script: '{script_path}'")
    reg_key = f"{scripts_index_prefix}{script_path}"
    reg_id = redis.hget(reg_key, 'registration_id')
    if reg_id is not None:
        log.debug(
            f"Removing registration for script '{script_path}' "
            "with id '{reg_id}'"
        )
        try:
            redis.execute_command('RG.UNREGISTER', reg_id)
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
        requirements_list = requirements.read(requirements_file_path)
        redis.execute_command(
            "RG.PYEXECUTE", "GB().run()", "REQUIREMENTS", *requirements_list
        )
    except Exception as ex:
        log.error(f"Someting went wrong: {ex}")


# File Event Handling

running = True
file_index = HysteresisHandlerIndex(hysteresis_duration)


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
    if fnmatch(file, script_pattern):
        log.debug(
            f"Gears script '{file}' deleted. "
            "Scheduling unregistration of script."
        )
        # Apply hysteresis in case additional events shortly follow,
        # before we actually unregister
        file_index.signal(file, unregister_script, file)
    elif fnmatch(file, requirements_pattern):
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
    if fnmatch(file, script_pattern):
        log.debug(f"Gears script '{file}' modified. Regestering script.")
        # Apply hysteresis in case additional events shortly follow,
        # before we actually (re-)run/register the script.
        file_index.signal(file, register_script, file)
    elif fnmatch(event.src_path, requirements_pattern):
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
    if fnmatch(old_file, script_pattern):
        log.debug(
            f"Gears script '{old_file}' moved to '{new_file}'. "
            "Unregistering old script."
        )
        # Apply hysteresis in case additional events shortly follow,
        # before we actually unregister the script.
        file_index.signal(old_file, unregister_script, old_file)
    elif fnmatch(old_file, requirements_pattern):
        log.debug(
            f"Gears requirements file '{old_file}' moved to '{new_file}'. "
            "Requirement removal not Implemented. "
            "Ignoring."
        )

    # TODO: Check that the new file is in the watch directory
    # Handle, i.e. run/register, the new / destination file
    if fnmatch(new_file, script_pattern):
        log.debug(
            f"File '{old_file}' moved to Gears script '{new_file}'. "
            "Registering new script."
        )
        # Apply hysteresis in case additional events shortly follow,
        # before we actually (re-)run/register the script
        file_index.signal(new_file, register_script, new_file)
    elif fnmatch(new_file, requirements_pattern):
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
    patterns=[script_pattern, requirements_pattern],
    ignore_patterns=ignore_patterns,
    ignore_directories=False,
    case_sensitive=False
)

event_handler.on_deleted = on_deleted
event_handler.on_modified = on_modified
event_handler.on_moved = on_moved

observer = Observer()

for directory in watch_directories:
    log.info(f"Adding event handlers for {directory}")
    observer.schedule(event_handler, directory, recursive)

log.info("Starting watcher!")
observer.start()

# TODO: Iterate through all the files in the watch directories
# and ensure they are loaded.

try:
    while running:
        sleep(1)
except KeyboardInterrupt:
    running = False
    print("Interrupted by user. Ending!")
finally:
    redis.close()
