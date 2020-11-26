import logging

# Various Helper enums, instead of "magic" strings.


class Reader:
    """Redis Gears Reader Types"""
    KeysReader = "KeysReader"
    KeysOnlyReader = "KeysOnlyReader"
    StreamReader = "StreamReader"
    PythonReader = "PythonReader"
    ShardsIDReader = "ShardsIDReader"
    CommandReader = "CommandReader"


class TriggerMode:
    """Redis Geears Trigger modes for registered actions"""
    Async = 'async'
    AsyncLocal = 'async_local'
    Sync = 'sync'


class LogLevel:
    """Redis Gears log levels"""
    Debug = 'debug'
    Verbose = 'verbose'
    Notice = 'notice'
    Warning = 'warninig'

    @staticmethod
    def to_logging_level(rg_log_level):
        if rg_log_level == LogLevel.Debug:
            return logging.DEBUG
        elif rg_log_level == LogLevel.Verbose:
            return logging.INFO
        elif rg_log_level == LogLevel.Notice:
            return logging.WARNING
        elif rg_log_level == LogLevel.Warning:
            return logging.ERROR  # Maybe a little unintuitive ;)
        else:
            return logging.INFO
