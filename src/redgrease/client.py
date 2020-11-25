from redis import Redis


class RedisGears(Redis):
    def abortexecution(self):
        """
        docstring
        """
        raise NotImplementedError

    def configget(self):
        """
        docstring
        """
        raise NotImplementedError

    def configset(self, parameter_list):
        """
        docstring
        """
        raise NotImplementedError

    def dropexecution(self, parameter_list):
        """
        docstring
        """
        raise NotImplementedError

    def dumpexecutions(self, parameter_list):
        """
        docstring
        """
        raise NotImplementedError

    def dumpregistrations(self, parameter_list):
        """
        docstring
        """
        raise NotImplementedError

    def getexecution(self, parameter_list):
        """
        docstring
        """
        raise NotImplementedError

    def getresults(self, parameter_list):
        """
        docstring
        """
        raise NotImplementedError

    def getresultsblocking(self, parameter_list):
        """
        docstring
        """
        raise NotImplementedError

    def infocluster(self, parameter_list):
        """
        docstring
        """
        raise NotImplementedError

    def pyexecute(self, parameter_list):
        """
        docstring
        """
        raise NotImplementedError

    def pstats(self, parameter_list):
        """
        docstring
        """
        raise NotImplementedError

    def pydumpreqs(self, parameter_list):
        """
        docstring
        """
        raise NotImplementedError

    def refreshcluster(self, parameter_list):
        """
        docstring
        """
        raise NotImplementedError

    def trigger(self, parameter_list):
        """
        docstring
        """
        raise NotImplementedError

    def unregister(self, parameter_list):
        """
        docstring
        """
        raise NotImplementedError
