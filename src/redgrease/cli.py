import logging
import re
import time
from pathlib import Path

import configargparse

import redgrease.formatting
import redgrease.loader

args = configargparse.ArgParser(
    description="Scans one or more directories for Redis Gears scripts, and "
    "executes them in a Redis Gears instance or cluster. "
    "Can optionally run continiously, montoring and re-loading scripts "
    "whenever changes are detected.",
    default_config_files=["./*.conf", "/etc/redgrease/conf.d/*.conf"],
)

args.add_argument(
    "directories",
    metavar="dir_path",
    nargs="+",
    type=Path,
    help="One or more directories containing Redis Gears scripts to watch",
)

args.add_argument(
    "-c",
    "--config",
    env_var="CONFIG_FILE",
    metavar="PATH",
    is_config_file=True,
    help="Config file path",
)
args.add_argument(
    "--index-prefix",
    env_var="INDEX_PREFIX",
    metavar="PREFIX",
    required=False,
    type=str,
    default=redgrease.loader.default_index_prefix,
    help="Redis key prefix added to the index of monitored/executed script " "files.",
)
args.add_argument(
    "-r",
    "--recursive",
    env_var="RECURSIVE",
    action="store_true",
    help="Recursively watch subdirectories.",
)
args.add_argument(
    "--script-pattern",
    env_var="SCRIPT_PATTERN",
    metavar="PATTERN",
    required=False,
    type=str,
    default=redgrease.loader.default_script_pattern,
    help="File name pattern (glob-style) that must be matched for scripts to "
    "be loaded.",
)
args.add_argument(
    "--requirements-pattern",
    env_var="REQUIREMENTS_PATTERN",
    metavar="PATTERN",
    required=False,
    type=str,
    default=redgrease.loader.default_requirements_pattern,
    help="File name pattern (glob-style) that must be matched for requirement "
    "files to be loaded.",
)
args.add_argument(
    "--unblocking-pattern",
    env_var="UNBLOCKING_PATTERN",
    metavar="PATTERN",
    required=False,
    type=re.compile,
    default=redgrease.loader.default_unblocking_pattern,
    help="Scripts with file paths that match this regular expression, "
    "will be executed with the 'UNBLOCKING' modifier, i.e. async execution. "
    "Note that the pattern is a 'search' pattern and not anchored to the"
    "start of the path string.",
)
args.add_argument(
    "-i",
    "--ignore",
    env_var="IGNORE",
    metavar="PATTERN",
    action="append",
    required=False,
    help="Ignore files matching this pattern.",
)
args.add_argument(
    "-w",
    "--watch",
    env_var="WATCH",
    metavar="SECONDS",
    nargs="?",
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
    "If no value is supplied, the duration is defaulting to 5 seconds.",
)
args.add_argument(
    "-s",
    "--server",
    env_var="SERVER",
    type=str,
    const="redis",
    default="localhost",
    nargs="?",
    help="Redis Gears host server IP or hostname.",
)
args.add_argument(
    "-p",
    "--port",
    env_var="PORT",
    type=int,
    default=6379,
    help="Redis Gears host port number",
)
args.add_argument(
    "-l",
    "--log-config",
    env_var="LOG_CONFIG",
    type=str,
    help="",
)


def blah(
    apanapansson: int = 2, bepanbepansdotter: str = "wut teh hellz", some_value=None
):
    ...


config = args.parse_args()

redgrease.formatting.initialize_logger(config=config.log_config)

log = logging.getLogger(__name__)


def main():
    log.debug(config)
    loader = redgrease.loader.GearsLoader(
        script_pattern=config.script_pattern,
        requirements_pattern=config.requirements_pattern,
        unblocking_pattern=config.unblocking_pattern,
        ignore_patterns=config.ignore,
        index_prefix=config.index_prefix,
        server=config.server,
        port=config.port,
        observe=config.watch,
    )
    for directory in config.directories:
        loader.add_directory(directory=directory, recursive=config.recursive)

    if loader.observer:
        log.info("Starting directory observer!")
        loader.start()
        running = True

        try:
            while running:
                time.sleep(1)
        except KeyboardInterrupt:
            log.warning("Interrupted by user. Ending!")
        finally:
            loader.stop()


if __name__ == "__main__":
    main()
