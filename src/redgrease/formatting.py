import json
import logging
import logging.config
import pathlib
from datetime import datetime
from typing import Any, Dict, Union

import yaml

iso8601_datefmt = "%Y-%m-%dT%H:%M:%S.%f"


# Logging matters
class UTC_ISO8601_Formatter(logging.Formatter):
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

log_formatter = UTC_ISO8601_Formatter(fmt=log_message_format)


# TODO: Simplify
def initialize_logger(
    level=logging.DEBUG,
    fmt: Union[logging.Formatter, str] = None,
    config: Union[pathlib.Path, str, Dict[str, Any]] = None,
):
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

    # assume yaml, unelss explicitly otherwise
    if suffix not in [".ini", ".json"]:
        logging.getLogger(__name__).debug(
            f"Attempting to parse '{conf_file}' as yaml file."
        )
        try:
            with conf_file.open() as f:
                config_dict = yaml.safe_load(f.read())
                logging.getLogger(__name__).info(f"Successfully read '{conf_file}''")
        except Exception as ex:
            if suffix in [".yaml", ".yml"]:
                raise ex
            else:
                logging.getLogger(__name__).warning(
                    f"Unable to parse '{conf_file}' as yaml file."
                )

    # if not yaml, assume json, unless explicitly otherwise
    if not config_dict and suffix not in [".ini", ".yaml", ".yml"]:
        logging.getLogger(__name__).debug(
            f"Attempting to parse '{conf_file}' as json file."
        )
        try:
            with conf_file.open() as f:
                config_dict = json.load(f)
                logging.getLogger(__name__).info(f"Successfully read '{conf_file}'")

        except Exception as ex:
            if suffix == ".json":
                raise ex
            else:
                logging.getLogger(__name__).warning(
                    f"Unable to parse '{conf_file}' as json file."
                )

    # if neither json nor yaml, attempt to parse as legacy ini
    if not config_dict and suffix not in [".json", ".yaml", ".yml"]:
        logging.getLogger(__name__).debug(
            f"Attempting to parse '{conf_file}' as legacy ini file."
        )
        try:
            logging.config.fileConfig(str(conf_file), disable_existing_loggers=False)
            logging.getLogger(__name__).info(f"Successfully loaded '{conf_file}'")
            return
        except Exception as ex:
            if suffix == ".ini":
                raise ex
            else:
                logging.getLogger(__name__).warning(
                    f"Unable to parse '{conf_file}' as legacy ini"
                )

    if not config_dict:
        raise ValueError(f"Unable to parse '{conf_file}'")

    logging.config.dictConfig(config_dict)
    logging.getLogger(__name__).info("Successfully loaded configuration")
    return
