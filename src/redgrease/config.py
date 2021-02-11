import logging
from typing import Any, Dict, List

import redis

from redgrease.typing import Constructor
from redgrease.utils import safe_str, to_list

log = logging.getLogger(__name__)


class Config:
    ValueTypes: Dict[str, Constructor[Any]] = {
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

    __slots__ = "redis"

    def __init__(self, redis: redis.Redis):
        self.redis = redis

    def get(self, *config_option) -> List[str]:
        """Get the value of one or more built-in configuration or
        a user-defined options.

        Args:
            config_name (Union[ID, bytes, str]): One or more names/key
            of configurations to get

        Returns:
            List of config values
        """
        if config_option:
            conf = [safe_str(c) for c in config_option]
        else:
            conf = list(Config.ValueTypes.keys())

        return self.redis.execute_command("RG.CONFIGGET", *conf, keys=conf)

    def set(self, config_dict=None, **config_setting) -> bool:
        """Set a value of one ore more built-in configuration or
        a user-defined options.

        Args:
            config_setting: Key-value-pairs of config settings

        Returns:
            bool: True if all was successful, false oterwise
        """
        settings = {}
        if config_dict:
            settings.update(config_dict)

        if config_setting:
            settings.update(config_setting)

        return self.redis.execute_command("RG.CONFIGSET", *to_list(settings))

    def get_single(self, key):
        return self.get(key)[key]

    @property
    def MaxExecutions(self):
        return self.get_single("MaxExecutions")

    @MaxExecutions.setter
    def MaxExecutions(self, value):
        return self.set(MaxExecutions=value)

    @property
    def MaxExecutionsPerRegistration(self):
        return self.get_single("MaxExecutionsPerRegistration")

    @MaxExecutionsPerRegistration.setter
    def MaxExecutionsPerRegistration(self, value):
        self.set(MaxExecutionsPerRegistration=value)

    @property
    def ProfileExecutions(self):
        return self.get_single("ProfileExecutions")

    @ProfileExecutions.setter
    def ProfileExecutions(self, value):
        self.set(ProfileExecutions=value)

    @property
    def PythonAttemptTraceback(self):
        return self.get_single("PythonAttemptTraceback")

    @PythonAttemptTraceback.setter
    def PythonAttemptTraceback(self, value):
        self.set(PythonAttemptTraceback=value)

    @property
    def DownloadDeps(self):
        return self.get_single("DownloadDeps")

    # @DownloadDeps.setter
    # def DownloadDeps(self, value):
    #     self.set(DownloadDeps=value)

    @property
    def DependenciesUrl(self):
        return self.get_single("DependenciesUrl")

    # @DependenciesUrl.setter
    # def DependenciesUrl(self, value):
    #     self.set(DependenciesUrl=value)

    @property
    def DependenciesSha256(self):
        return self.get_single("DependenciesSha256")

    # @DependenciesSha256.setter
    # def DependenciesSha256(self, value):
    #     self.set(DependenciesSha256=value)

    @property
    def PythonInstallationDir(self):
        return self.get_single("PythonInstallationDir")

    # @PythonInstallationDir.setter
    # def PythonInstallationDir(self, value):
    #     self.set(PythonInstallationDir=value)

    @property
    def CreateVenv(self):
        return self.get_single("CreateVenv")

    # @CreateVenv.setter
    # def CreateVenv(self, value):
    #     self.set(CreateVenv=value)

    @property
    def ExecutionThreads(self):
        return self.get_single("ExecutionThreads")

    # @ExecutionThreads.setter
    # def ExecutionThreads(self, value):
    #     self.set(ExecutionThreads=value)

    @property
    def ExecutionMaxIdleTime(self):
        return self.get_single("ExecutionMaxIdleTime")

    @ExecutionMaxIdleTime.setter
    def ExecutionMaxIdleTime(self, value):
        self.set(ExecutionMaxIdleTime=value)

    @property
    def PythonInstallReqMaxIdleTime(self):
        return self.get_single("PythonInstallReqMaxIdleTime")

    @PythonInstallReqMaxIdleTime.setter
    def PythonInstallReqMaxIdleTime(self, value):
        self.set(PythonInstallReqMaxIdleTime=value)

    @property
    def SendMsgRetries(self):
        return self.get_single("SendMsgRetries")

    @SendMsgRetries.setter
    def SendMsgRetries(self, value):
        self.set(SendMsgRetries=value)
