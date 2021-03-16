import rediscluster
import rediscluster.exceptions

import redgrease.client


@redgrease.client.geared
class RedisCluster(rediscluster.RedisCluster):
    """RedisCluster client class, with support for gears features

    Behaves exactly like the rediscluster.RedisCluster client, but is extended with
    a 'gears' property fo executiong Gears commands.

    Attributes:
        gears (redgrease.client.Gears):
            Gears command client.
    """

    def __init__(self, *args, **kwargs):
        """Instantiate a redis cluster client, with gears features"""

        super().__init__(*args, **kwargs)


def RedisGears(*args, **kwargs):
    try:
        return RedisCluster(*args, **kwargs)

    except (AttributeError, rediscluster.exceptions.RedisClusterException):
        return redgrease.client.Redis(*args, **kwargs)
