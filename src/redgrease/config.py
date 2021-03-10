# -*- coding: utf-8 -*-
"""
Redis Gears Config
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
from typing import Any, Dict, Union

import redis

from redgrease.typing import Constructor
from redgrease.utils import safe_str, to_list

log = logging.getLogger(__name__)


class Config:
    """Redis Gears Config"""

    ValueTypes: Dict[str, Constructor] = {
        "MaxExecutions": int,
        "MaxExecutionsPerRegistration": int,
        "ProfileExecutions": bool,
        "PythonAttemptTraceback": bool,
        "DownloadDeps": bool,
        "DependenciesUrl": safe_str,
        "DependenciesSha256": safe_str,
        "PythonInstallationDir": safe_str,
        "CreateVenv": bool,
        "ExecutionThreads": int,
        "ExecutionMaxIdleTime": int,
        "PythonInstallReqMaxIdleTime": int,
        "SendMsgRetries": int,
    }
    """Mapping from config name to its corresponding value type, transformer
    or constructor.
    """

    __slots__ = "redis"

    def __init__(self, redis: redis.Redis):
        """Instantiate a Redis Gears Confg object

        Args:
            redis (redis.Redis):
                Redis client / connection for underlying communication.
        """
        self.redis = redis

    def get(self, *config_option: Union[bytes, str]) -> Dict[Union[bytes, str], Any]:
        """Get the value of one or more built-in configuration or
        a user-defined options.

        Args:
            *config_option (Union[bytes, str]):
                One or more names/key of configurations to get.

        Returns:
            Dict[Union[bytes, str], Any]:
                Dict of the requested config options mapped to their corresponding
                values.
        """
        if config_option:
            conf = [safe_str(c) for c in config_option]
        else:
            conf = list(Config.ValueTypes.keys())

        return self.redis.execute_command("RG.CONFIGGET", *conf, keys=conf)

    def set(self, config_dict=None, **config_setting) -> bool:
        """Set a value of one ore more built-in configuration or
        a user-defined options.

        This function offers two methods of providing the keys and values to set;
        either as a mapping/dict or b key-word arguments.

        Args:
            config_dict  (Mapping[str, Any]): Mapping / dict of config values to set.

            **config_setting (Any): Key-word arguments to set as config values.

        Returns:
            bool:
                `True` if all was successful, `False` oterwise
        """
        settings = {}
        if config_dict:
            settings.update(config_dict)

        if config_setting:
            settings.update(config_setting)

        return self.redis.execute_command("RG.CONFIGSET", *to_list(settings))

    def get_single(self, config_option: Union[bytes, str]):
        """Get a single config value.

        Args:
            config_option (str):
                Name of the config to get.

        Returns:
            Any:
                The value of the config option.
        """
        return self.get(config_option)[config_option]

    @property
    def MaxExecutions(self) -> int:
        """Get the current value for the `MaxExecutions` config option.

        The MaxExecutions configuration option controls the maximum number of
        executions that will be saved in the executions list.
        Once this threshold value is reached, older executions will be deleted from
        the list by order of their creation (FIFO).

        Only executions that had finished (e.g. the 'done' or 'aborted' status ) are
        deleted.
        """
        return self.get_single("MaxExecutions")

    @MaxExecutions.setter
    def MaxExecutions(self, value: int) -> None:
        self.set(MaxExecutions=value)

    @property
    def MaxExecutionsPerRegistration(self) -> int:
        """Get the current value for the `MaxExecutionsPerRegistration` config option.

        The MaxExecutionsPerRegistration configuration option controls the maximum
        number of executions that are saved in the list per registration.
        Once this threshold value is reached, older executions for that registration
        will be deleted from the list by order of their creation (FIFO).

        Only executions that had finished (e.g. the 'done' or 'aborted' status ) are
        deleted.
        """
        return self.get_single("MaxExecutionsPerRegistration")

    @MaxExecutionsPerRegistration.setter
    def MaxExecutionsPerRegistration(self, value: int) -> None:
        self.set(MaxExecutionsPerRegistration=value)

    @property
    def ProfileExecutions(self) -> bool:
        """Get the current value for the `ProfileExecutions` config option.

        The ProfileExecutions configuration option controls whether executions are
        profiled.

        **Note: Profiling impacts performance**
        Profiling requires reading the server's clock, which is a costly operation in
        terms of performance. Execution profiling is recommended only for debugging
        purposes and should be disabled in production.
        """
        return self.get_single("ProfileExecutions")

    @ProfileExecutions.setter
    def ProfileExecutions(self, value: bool) -> None:
        self.set(ProfileExecutions=value)

    @property
    def PythonAttemptTraceback(self) -> bool:
        """Get the current value for the `PythonAttemptTraceback` config option.

        The PythonAttemptTraceback configuration option controls whether the engine
        tries producing stack traces for Python runtime errors.
        """
        return self.get_single("PythonAttemptTraceback")

    @PythonAttemptTraceback.setter
    def PythonAttemptTraceback(self, value: bool) -> None:
        self.set(PythonAttemptTraceback=value)

    @property
    def DownloadDeps(self) -> bool:
        """Get the current value for the `DownloadDeps` config option.

        The DownloadDeps configuration option controls whether or not RedisGears will
        attempt to download missing Python dependencies.
        """
        return self.get_single("DownloadDeps")

    # @DownloadDeps.setter
    # def DownloadDeps(self, value):
    #     self.set(DownloadDeps=value)

    @property
    def DependenciesUrl(self) -> str:
        """Get the current value for the `DependenciesUrl` config option.

        The DependenciesUrl configuration option controls the location from which
        RedisGears tries to download its Python dependencies.
        """
        return self.get_single("DependenciesUrl")

    # @DependenciesUrl.setter
    # def DependenciesUrl(self, value):
    #     self.set(DependenciesUrl=value)

    @property
    def DependenciesSha256(self) -> str:
        """Get the current value for the `DependenciesSha256` config option.

        The DependenciesSha256 configuration option specifies the SHA265 hash value
        of the Python dependencies. This value is verified after the dependencies have
        been downloaded and will stop the server's startup in case of a mismatch.
        """
        return self.get_single("DependenciesSha256")

    # @DependenciesSha256.setter
    # def DependenciesSha256(self, value):
    #     self.set(DependenciesSha256=value)

    @property
    def PythonInstallationDir(self):
        """Get the current value for the `PythonInstallationDir` config option.

        The PythonInstallationDir configuration option specifies the path for
        RedisGears' Python dependencies.
        """
        return self.get_single("PythonInstallationDir")

    # @PythonInstallationDir.setter
    # def PythonInstallationDir(self, value):
    #     self.set(PythonInstallationDir=value)

    @property
    def CreateVenv(self):
        """Get the current value for the `CreateVenv` config option.

        The CreateVenv configuration option controls whether the engine will create a
        virtual Python environment.
        """
        return self.get_single("CreateVenv")

    # @CreateVenv.setter
    # def CreateVenv(self, value):
    #     self.set(CreateVenv=value)

    @property
    def ExecutionThreads(self):
        """Get the current value for the `ExecutionThreads` config option.

        The ExecutionThreads configuration option controls the number of threads that
        will run executions.
        """
        return self.get_single("ExecutionThreads")

    # @ExecutionThreads.setter
    # def ExecutionThreads(self, value):
    #     self.set(ExecutionThreads=value)

    @property
    def ExecutionMaxIdleTime(self):
        """Get the current value for the `ExecutionMaxIdleTime` config option.

        The ExecutionMaxIdleTime configuration option controls the maximal amount of
        idle time (in milliseconds) before execution is aborted. Idle time means no
        progress is made by the execution.

        The main reason for idle time is an execution that's blocked on waiting for
        records from another shard that had failed (i.e. crashed).
        In that case, the execution will be aborted after the specified time limit.
        The idle timer is reset once the execution starts progressing again.
        """
        return self.get_single("ExecutionMaxIdleTime")

    @ExecutionMaxIdleTime.setter
    def ExecutionMaxIdleTime(self, value):
        self.set(ExecutionMaxIdleTime=value)

    @property
    def PythonInstallReqMaxIdleTime(self):
        """Get the current value for the `PythonInstallReqMaxIdleTime` config option.

        The PythonInstallReqMaxIdleTime configuration option controls the maximal
        amount of idle time (in milliseconds) before Python's requirements installation
        is aborted. Idle time means that the installation makes no progress.

        The main reason for idle time is the same as for ExecutionMaxIdleTime .
        """
        return self.get_single("PythonInstallReqMaxIdleTime")

    @PythonInstallReqMaxIdleTime.setter
    def PythonInstallReqMaxIdleTime(self, value):
        self.set(PythonInstallReqMaxIdleTime=value)

    @property
    def SendMsgRetries(self):
        """Get the current value for the `SendMsgRetries` config option.
        The SendMsgRetries configuration option controls the maximum number of retries
        for sending a message between RedisGears' shards. When a message is sent and
        the shard disconnects before acknowledging it, or when it returns an error,
        the message will be resent until this threshold is met.

        Setting the value to 0 means unlimited retries.
        """
        return self.get_single("SendMsgRetries")

    @SendMsgRetries.setter
    def SendMsgRetries(self, value):
        self.set(SendMsgRetries=value)
