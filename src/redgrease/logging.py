import logging
from datetime import datetime


iso8601_format = "%Y-%m-%dT%H:%M:%S.%f"


# Logging matters
class UTC_ISO8601_Formatter(logging.Formatter):
    converter = datetime.utcfromtimestamp

    def formatTime(self, record, datefmt=None):
        ct = self.converter(record.created)
        if datefmt:
            s = ct.strftime(datefmt)
        else:
            s = ct.strftime(iso8601_format)
        return s


default_log_fmt = UTC_ISO8601_Formatter(
    fmt="%(asctime)s %(levelname)s %(name)s: %(message)s",
)


def getLogger(log_name=None):
    if log_name:
        return logging.getLogger(str(log_name))
    else:
        return logging.getLogger()


def setLevel(log_level="INFO", log_name=None):
    log = getLogger(log_name)
    log.setLevel(log_level)


def enable_std_out_logging(log_name=None, log_level=None, log_formatter=None):
    log = getLogger(log_name)
    log_level = log_level if log_level else log.level
    log_formatter = log_formatter if log_formatter else default_log_fmt

    stdout_log = logging.StreamHandler()
    stdout_log.setLevel(log_level)
    stdout_log.setFormatter(log_formatter)
    log.addHandler(stdout_log)


def enable_file_logging(
    log_file,
    log_name=None,
    log_level=None,
    log_formatter=None
):
    log = getLogger(log_name)
    log_level = log_level if log_level else log.level
    log_formatter = log_formatter if log_formatter else default_log_fmt

    file_log = logging.FileHandler(log_file)
    file_log.setLevel(log_level)
    file_log.setFormatter(log_formatter)
    log.addHandler(file_log)
