# -*- coding: utf-8 -*-
"""
Helper module for the sole purpose of handling log formatting.
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

import configparser
import json
import logging
import logging.config
import pathlib
from datetime import datetime
from typing import Any, Dict, Union

import yaml
import yaml.error

configparser.MissingSectionHeaderError

iso8601_datefmt = "%Y-%m-%dT%H:%M:%S.%f"
"""ISO 8601 date / time format string."""


# Logging matters
class UTC_ISO8601_Formatter(logging.Formatter):
    """Formatter for UTC timestamps in ISO 8601 date format."""

    converter = datetime.utcfromtimestamp  # type: ignore

    def formatTime(self, record, datefmt=None):
        ct = self.converter(record.created)
        if datefmt:
            s = ct.strftime(datefmt)
        else:
            s = ct.strftime(iso8601_datefmt)
        return s


log_message_format = (
    "%(asctime)s\t"
    "%(levelname)s\t"
    "%(name)s.%(funcName)s@%(lineno)d:\t"
    "%(message)s\t"
    "EOL"
)
"""Default log message format. E.g:

"""

log_formatter = UTC_ISO8601_Formatter(fmt=log_message_format)
"""Default log formatter"""


# TODO: Simplify
def initialize_logger(  # noqa: C901
    level=logging.DEBUG,
    fmt: Union[logging.Formatter, str] = None,
    config: Union[pathlib.Path, str, Dict[str, Any]] = None,
) -> None:
    """Initialize logger with defaults, overridden by any custom options from various
    sources, including passed parameters, json, ini or yaml config file.

    Args:
        fmt (Union[logging.Formatter, str], optional):
            Logging format string or Formatter to use.
            Defaults to the default log message format:

                ``2021-03-08T10:46:15.1329  DEBUG   package.module.func@123
                    This is a message   EOL``

        config (Union[pathlib.Path, str, Dict[str, Any]], optional):
            Optional ini, json or yaml filepath or a dict with config parameters for
            the logger.
            Defaults to None.

    Raises:
        FileNotFoundError:
            Raised if the config option is a path to a file that does not exist.

        ValueError:
            Raised if the config option is an existing file but cannot be parsed.
    """
    # Initialize to good (tm) defaults
    logging.basicConfig(level=level, format=log_message_format, datefmt=iso8601_datefmt)

    if not fmt:
        fmt = log_message_format

    if not isinstance(fmt, logging.Formatter):
        msg_formatter = UTC_ISO8601_Formatter(fmt=str(fmt))

    # Override with explicit opthions
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    root_logger.handlers[0].setFormatter(msg_formatter)

    # Override with config options, if any
    if not config:
        return

    if isinstance(config, dict):
        logging.config.dictConfig(config)
        logging.getLogger(__name__).info("Successfully loaded configuration from dict")
        return

    conf_file = pathlib.Path(str(config))
    suffix = conf_file.suffix.lower()
    config_dict = None

    if not conf_file.is_file():
        raise FileNotFoundError(conf_file)

    parse_error_string = "Unable to parse config file as {}: '{}'"

    # assume yaml, unelss explicitly otherwise
    if suffix not in [".ini", ".json"]:
        logging.getLogger(__name__).debug(
            f"Attempting to parse '{conf_file}' as yaml file."
        )
        try:
            with conf_file.open() as f:
                config_dict = yaml.safe_load(f.read())
                logging.getLogger(__name__).info(f"Successfully read '{conf_file}''")
        except yaml.error.YAMLError as e:
            msg = parse_error_string.format("YAML", conf_file)
            if suffix in [".yaml", ".yml"]:
                raise ValueError(msg) from e
            else:
                logging.getLogger(__name__).warning(msg)

    # if not yaml, assume json, unless explicitly otherwise
    if not config_dict and suffix not in [".ini", ".yaml", ".yml"]:
        logging.getLogger(__name__).debug(
            f"Attempting to parse '{conf_file}' as json file."
        )
        try:
            with conf_file.open() as f:
                config_dict = json.load(f)
                logging.getLogger(__name__).info(f"Successfully read '{conf_file}'")

        except json.JSONDecodeError as e:
            msg = parse_error_string.format("JSON", conf_file)
            if suffix == ".json":
                raise ValueError(msg) from e
            else:
                logging.getLogger(__name__).warning(msg)

    # if neither json nor yaml, attempt to parse as legacy ini
    if not config_dict and suffix not in [".json", ".yaml", ".yml"]:
        logging.getLogger(__name__).debug(
            f"Attempting to parse '{conf_file}' as legacy ini file."
        )
        try:
            logging.config.fileConfig(str(conf_file), disable_existing_loggers=False)
            logging.getLogger(__name__).info(f"Successfully loaded '{conf_file}'")
            return
        except configparser.Error as e:
            msg = parse_error_string.format("INI", conf_file)
            if suffix == ".ini":
                raise ValueError(msg) from e
            else:
                logging.getLogger(__name__).warning(msg)

    if not config_dict:
        msg = parse_error_string.format("anything", conf_file)
        raise ValueError(msg)

    logging.config.dictConfig(config_dict)
    logging.getLogger(__name__).info("Successfully loaded configuration")
    return
