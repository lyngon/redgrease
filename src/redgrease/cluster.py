# import rediscluster
# import rediscluster.exceptions

# import redgrease.client
# import redgrease.data
# import redgrease.utils

# class RedisCluster(rediscluster.RedisCluster):
#     """RedisCluster client class, with support for gears features

#     Behaves exactly like the rediscluster.RedisCluster client, but is extended with
#     a 'gears' property fo executiong Gears commands.

#     Attributes:
#         gears (redgrease.client.Gears):
#             Gears command client.
#     """

#     def __init__(self, *args, **kwargs):
#         """Instantiate a redis cluster client, with gears features"""
#         self._gears = None
#         self.connection = None
#         super().__init__(*args, **kwargs)

#     @property
#     def gears(self) -> redgrease.client.Gears:
#         """Gears client, exposing gears commands

#         Returns:
#             Gears:
#                 Gears client
#         """
#         if not self._gears:
#             self._gears = redgrease.client.Gears(self)

#         return self._gears


# def RedisGears(*args, **kwargs):
#     try:
#         return RedisCluster(*args, **kwargs)

#     except (AttributeError, rediscluster.exceptions.RedisClusterException):
#         return redgrease.client.Redis(*args, **kwargs)
