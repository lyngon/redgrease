# -*- coding: utf-8 -*-
"""
Module for loading Gears script files into a Redis Gears cluster, as well as
continiously monitor the files or directories for changes, and in that case reload the
modified/created scripts into the cluster.
"""
__author__ = "Anders Åström"
__contact__ = "anders@lyngon.com"
__copyright__ = "2021, Lyngon Pte. Ltd."
__licence__ = """The MIT License
Copyright © 2021 Lyngon Pte. Ltd.

Permission is hereby granted, free of charge, to any person obtaining a copy of this
 software and associated documentation files (the “Software”), to deal in the Software
 without restriction, including without limitation the rights to use, copy, modify,
 merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 permit persons to whom the Software is furnished to do so, subject to the following
 conditions:

The above copyright notice and this permission notice shall be included in all copies
 or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF
 CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE
 OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""

import logging
import re
from datetime import datetime
from fnmatch import fnmatch
from os.path import isfile
from pathlib import Path
from typing import List, Union

from redis.exceptions import RedisError, ResponseError
from watchdog.events import PatternMatchingEventHandler
from watchdog.observers import Observer

from redgrease import RedisGears, formatting, hysteresis, requirements

log = logging.getLogger(__name__)

default_index_prefix = "/redgrease/scripts"
"""The defaut prefix for the Redis keys that hold the Loader index data"""

default_script_pattern = "*.py"
"""The default pattern for scripts to monitor and load."""

default_requirements_pattern = "*requirements*.txt"
"""The default pattern for requirements files to monitor and load."""

default_unblocking_pattern = "unblock"
"""The default pattern (within script files) that indicate 'unblocking' load"""

default_ignore_patterns: List[str] = []
"""Default list of file patterns to ignore"""

# Regex type changed
# from re.SRE_Pattern in Python 3.6
# to re.Pattern in Python3 3.7
_regex_type = type(re.compile(""))


def fail(exception, *messages):
    """Convenience function for raising exceptions

    Args:
        exception ([Exception]):
            Exception to raise.

        *messages (str):
            Any number of messages.
            Will be separated by full stop/period (.).

    Raises:
        exception: Exception
    """
    error_message = ". ".join(messages)
    log.error(error_message)

    raise exception(error_message)


class GearsLoader:
    """Monitors and loads one or more script files into Redis Gear cluster.

    Note that the loader will use the Redis cluster to store some indexing data
    for its own operation.
    Most notably information about which registration id has bee assigned to any
    'registered' script files.
    """

    def __init__(
        self,
        script_pattern: str = None,
        requirements_pattern: str = None,
        unblocking_pattern=None,
        ignore_patterns: str = None,
        index_prefix: str = None,
        server: str = "localhost",
        port: int = 6379,
        observe: float = None,
        **redis_kwargs,
    ):
        """Instatiate a Gears Loader

        Args:
            script_pattern (str, optional):
                A file pattern the files to monitor. Supports globbing.
                E.g. `scripts/*.py`.
                Defaults to None.

            requirements_pattern (str, optional):
                A file pattern for any requirement files defining the requirements of
                the scripts monitored. Supports globbing.
                E.g. `scripts/*requirements*.txt`.
                Defaults to None.

            unblocking_pattern ([type], optional):
                Pattern within script files that determine if the files should be
                loaded as "unblocking".
                E.g. `unblock`.
                Defaults to None.

            ignore_patterns (str, optional):
                Patterns for files to ignore.
                Defaults to None.

            index_prefix (str, optional):
                Prefix for the keys in Redis used by the GearLoader index.
                Defaults to None.

            server (str, optional):
                Server / host name or IP.
                Defaults to "localhost".

            port (int, optional):
                Server / host port number.
                Defaults to 6379.

            observe (float, optional):
                If `True`, continously observe the files, and reload if there are any
                changes.
                If `False`, the files will only be loaded when added to the Loader.
                Defaults to None.
        """
        self.directories: List[Path] = []

        self.script_pattern = (
            script_pattern if script_pattern else default_script_pattern
        )

        self.requirements_pattern = (
            requirements_pattern
            if requirements_pattern
            else default_requirements_pattern
        )

        if unblocking_pattern is None:
            unblocking_pattern = default_unblocking_pattern
        if isinstance(unblocking_pattern, str):
            unblocking_pattern = re.compile(re.escape(unblocking_pattern))
        if not isinstance(unblocking_pattern, _regex_type):
            fail(ValueError, f"Invalid unblocking pattern: {unblocking_pattern}")
        self.unblocking_pattern = unblocking_pattern

        self.ignore_patterns = (
            ignore_patterns if ignore_patterns else default_ignore_patterns
        )

        self.index_prefix = index_prefix if index_prefix else default_index_prefix

        self.redis = RedisGears(host=server, port=port, **redis_kwargs)

        self.observer = Observer() if observe else None

        self.file_index = hysteresis.HysteresisHandlerIndex(hysteresis_duration=observe)

        # Create the file change event listener
        self.event_handler = (
            PatternMatchingEventHandler(
                patterns=[self.script_pattern, self.requirements_pattern],
                ignore_patterns=self.ignore_patterns,
                ignore_directories=False,
                case_sensitive=False,
            )
            if self.observer
            else None
        )

        if self.event_handler:
            self.event_handler.on_deleted = self.on_deleted
            self.event_handler.on_modified = self.on_modified
            self.event_handler.on_moved = self.on_moved

    def __del__(self):
        try:
            self.redis.close()
        except RedisError as ex:
            log.warn(f"Error while closing redis: {ex}")

        try:
            self.stop()
        except Exception as ex:
            log.warn(f"Error stopping observer: {ex}")
            raise

    def add_directory(
        self, directory: Union[Path, str], recursive: bool = False
    ) -> None:
        """Add a directory to the Loader

        Args:
            directory (Union[Path, str]):
                Directory of scripts to load.

            recursive (bool, optional):
                Recursively scan sub-directories.
                Defaults to False.
        """
        if not isinstance(directory, Path):
            directory = Path(str(directory))

        log.info(f"Adding event handlers for {directory}")

        # select either recursive or non-recursive glob function for the dir
        globber = directory.rglob if recursive else directory.glob

        # find and run/register all requirements files in the watch directories
        for requirements_file_path in globber(self.requirements_pattern):
            self.update_dependencies(requirements_file_path)

        # find and run/register all script files in the watch directory
        for script_file_path in globber(self.script_pattern):
            self.register_script(script_file_path)

        if self.observer is not None:
            self.observer.schedule(self.event_handler, directory, recursive)

        self.directories.append(directory)

    # TODO: Should call pyexecute with the file path directly
    # TODO: Handle multiple registrations per script.
    def register_script(self, script_path):
        """Execute / Register a gear script in redis using 'RG.PYEXECUTE'

            Note: paths that maches the 'unblocking-pattern', will be executed
            with the 'UNBLOCKING' modifier.

        Args:
            script_path (str):
                Path to the script.

        """
        log.debug(f"Registering script '{script_path}'")
        general_failure_msg = f"Unable to register script file '{script_path}'"

        if not isfile(script_path):
            fail(FileNotFoundError, general_failure_msg, "File not found")

        with open(script_path) as script_file:
            script_content = script_file.read()

        if not script_content:
            fail(IOError, general_failure_msg, "File is empty")

        # Unregister script if already present
        self.unregister_script(script_path)

        unblocking = self.unblocking_pattern.search(str(script_path)) is not None

        try:
            log.debug(
                f"Running/registering Gear script '{script_path}' " "on Redis server"
            )

            # This is a quite unsafe way of checking for registrations
            # Probabls Ok for dev situations in non-shared environments
            pre_reg = self.redis.gears.dumpregistrations()
            exec_res = self.redis.gears.pyexecute(script_content, unblocking=unblocking)
            ret = f"with return code '{exec_res}'."
            post_reg = self.redis.gears.dumpregistrations()
            pre_reg = set([reg.id for reg in pre_reg])
            log.debug(f"Pre regs: {list(pre_reg)}")
            post_reg = set([reg.id for reg in post_reg])
            log.debug(f"Post regs: {list(post_reg)}")
            diff_reg: set = post_reg - pre_reg
            if len(diff_reg) > 0:
                reg_id = diff_reg.pop()
                self.redis.hset(
                    f"{self.index_prefix}{script_path}",
                    mapping={
                        "registration_id": str(reg_id),
                        "last_updated": datetime.utcnow().strftime(
                            formatting.iso8601_datefmt
                        ),
                    },
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

        except RedisError as ex:
            log.error(f"Something went wrong: {ex}")

    # Actions
    # TODO: Should call pyexecute with the file path directly
    # TODO: Handle multiple registrations per script.
    def unregister_script(self, script_path):
        """Check if a given script is registered, and if so,
        unregister it using 'RG.UNREGISTER'

        Args:
            script_path (str):
                Script path
        """
        log.debug(f"Unregistering script: '{script_path}'")
        reg_key = f"{self.index_prefix}{script_path}"
        reg_id = self.redis.hget(reg_key, "registration_id")
        if reg_id is not None:
            log.debug(
                f"Removing registration for script '{script_path}' "
                f"with id '{reg_id}'"
            )
            try:
                self.redis.gears.unregister(reg_id)
            except ResponseError as err:
                log.warn(
                    "Unregistration failed. "
                    "Index migth be corrupt. "
                    "Is this a shared environment?"
                )
                log.error({err})
        self.redis.delete(reg_key)

    def update_dependencies(self, requirements_file_path):
        """Update (add only) package dependencies on the Redis instance

        Args:
            requirements_file_path (str):
                File path of 'requirements.txt' file.
        """
        log.debug(f"Updating dependecies as per '{requirements_file_path}'")
        try:
            requirements_set = requirements.read_requirements(requirements_file_path)
            log.debug(f"Requirements to load: {', '.join(requirements_set)}")
            self.redis.gears.pyexecute(requirements=requirements_set)
        except Exception as ex:
            log.error(f"Something went wrong: {ex}")

    # File Event Handling
    def on_deleted(self, event):
        """Watchdog event handler for events signalling that a
        script or requirement file has been deleted.
        Script files are unregistered (if present) and re-run,
        after applying some hystersis.
        Removed requirement files does not remove any installed packages.

        Args:
            event (watchdog.FileDeletedEvent):
                Event data for deleted file
        """
        file = event.src_path
        if fnmatch(file, self.script_pattern):
            log.debug(
                f"Gears script '{file}' deleted. "
                "Scheduling unregistration of script."
            )
            # Apply hysteresis in case additional events shortly follow,
            # before we actually unregister
            self.file_index.signal(file, self.unregister_script, file)
        elif fnmatch(file, self.requirements_pattern):
            log.info(
                f"Gears requirements file '{file}' deleted. "
                "Requirement removal not Implemented. "
                "Ignoring."
            )
        else:
            log.warn(f"Unknown file type '{file}' deleted. " "Ignoring.")

    def on_modified(self, event):
        """Watchdog event handler for events signalling that a
        script or requirement file has been modified.

        Args:
            event (watchdog.FileModifiedEvent):
                Event data for modified file
        """
        file = event.src_path
        if fnmatch(file, self.script_pattern):
            log.debug(f"Gears script '{file}' modified. Regestering script.")
            # Apply hysteresis in case additional events shortly follow,
            # before we actually (re-)run/register the script.
            self.file_index.signal(file, self.register_script, file)
        elif fnmatch(event.src_path, self.requirements_pattern):
            log.debug(
                f"Gears requirements file '{file}' modified. " "Updating dependencies."
            )
            # Apply hysteresis in case additional events shortly follow,
            # before we actually update requirements.
            self.file_index.signal(file, self.update_dependencies, file)
        else:
            log.warn(f"Unknown file type '{event.src_path}' modified. Ignoring.")

    def on_moved(self, event):
        """Watchdog event handler for events signalling that a
        script or requirement file has been moved.
        Script files are unregistered (if present) and re-run,
        after applying some hystersis.
        Removed requirement files does not remove any installed packages.

        Args:
            event (watchdog.FileMovedEvent):
                Event data for moved file.
        """
        old_file = event.src_path
        new_file = event.dest_path
        # Handle, i.e. unregister, the old / source file.
        if fnmatch(old_file, self.script_pattern):
            log.debug(
                f"Gears script '{old_file}' moved to '{new_file}'. "
                "Unregistering old script."
            )
            # Apply hysteresis in case additional events shortly follow,
            # before we actually unregister the script.
            self.file_index.signal(old_file, self.unregister_script, old_file)
        elif fnmatch(old_file, self.requirements_pattern):
            log.debug(
                f"Gears requirements file '{old_file}' moved to '{new_file}'. "
                "Requirement removal not Implemented. "
                "Ignoring."
            )

        # TODO: Check that the new file is in the watch directory
        # Handle, i.e. run/register, the new / destination file
        if fnmatch(new_file, self.script_pattern):
            log.debug(
                f"File '{old_file}' moved to Gears script '{new_file}'. "
                "Registering new script."
            )
            # Apply hysteresis in case additional events shortly follow,
            # before we actually (re-)run/register the script
            self.file_index.signal(new_file, self.register_script, new_file)
        elif fnmatch(new_file, self.requirements_pattern):
            log.debug(
                f"File '{old_file}' moved to requirements file '{new_file}'. "
                "Updating dependencies"
            )
            # Apply hysteresis in case additional events shortly follow,
            # before we actually update requirements.
            self.file_index.signal(new_file, self.update_dependencies, new_file)
        else:
            log.warn(
                f"File '{old_file}' moved to unknonwn type '{new_file}'. " "Ignoring."
            )

    def start(self):
        """Start Loader monitoring."""
        log.info("Starting RedGrease directory watcher ")
        if self.observer:
            self.observer.start()

    def stop(self):
        """Stop Loader monitoring."""
        if self.observer:
            self.observer.stop()
