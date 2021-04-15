from typing import Callable, Dict

import rediscluster
import rediscluster.exceptions

import redgrease.client
import redgrease.data
import redgrease.utils


class RedisCluster(rediscluster.RedisCluster):
    """RedisCluster client class, with support for gears features

    Behaves exactly like the rediscluster.RedisCluster client, but is extended with
    a 'gears' property fo executiong Gears commands.

    Attributes:
        gears (redgrease.client.Gears):
            Gears command client.
    """

    # States how target node is selected for cluster commands:
    # - blocked : command is not allowed - Raises a RedisClusterException
    # - random : excuted on one randomly selected node
    # - all-masters : executed on all master node
    # - all-nodes : executed on all nodes
    # - slot-id : executed on the node defined by the second argument
    NODES_FLAGS = {
        **rediscluster.RedisCluster.NODES_FLAGS,
        **{
            "RG.INFOCLUSTER": "random",
            "RG.PYSTATS": "all-nodes",
            "RG.PYDUMPREQS": "random",
            "RG.REFRESHCLUSTER": "all-nodes",
        },
    }

    # Not to be confused with redis.Redis.RESPONSE_CALLBACKS
    # RESULT_CALLBACKS is special to rediscluster.RedisCluster.
    # It decides how results of commands defined in `NODES_FLAGS` are aggregated into
    # a final response, **after** redis.Redis.RESPONSE_CALLBACKS as been applied to
    # each response individually.
    RESULT_CALLBACKS = {
        **rediscluster.RedisCluster.RESULT_CALLBACKS,
        **{
            "RG.INFOCLUSTER": lambda _, res: next(iter(res.values())),
            "RG.PYSTATS": lambda _, res: res,
            "RG.PYDUMPREQS": lambda _, res: next(iter(res.values())),
            "RG.REFRESHCLUSTER": lambda _, res: all(res.values()),
        },
    }

    RESPONSE_CALLBACKS: Dict[str, Callable] = {
        **rediscluster.RedisCluster.RESPONSE_CALLBACKS,
        **redgrease.client.Gears.RESPONSE_CALLBACKS,
    }

    def __init__(self, *args, **kwargs):
        """Instantiate a redis cluster client, with gears features"""
        self._gears = None
        self.connection = None
        super().__init__(*args, **kwargs)

    @property
    def gears(self) -> redgrease.client.Gears:
        """Gears client, exposing gears commands

        Returns:
            Gears:
                Gears client
        """
        if not self._gears:
            self._gears = redgrease.client.Gears(self)

        return self._gears


def RedisGears(*args, **kwargs):
    try:
        return RedisCluster(*args, **kwargs)

    except (AttributeError, rediscluster.exceptions.RedisClusterException):
        return redgrease.client.Redis(*args, **kwargs)
