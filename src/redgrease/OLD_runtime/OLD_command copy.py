from redgrease.runtime import execute, log

# # Remove
import redis

r = redis.Redis()
##


# Decorator for not yet implemented functions
def not_implemented(func):
    def raiser(*args, **kwargs):
        msg = f"redgrease.runtime.command.{fun_name} is not yet implemented. Contact support@lyngon.com for more info or for feature request."
        log(f"Error: {msg}")
        raise NotImplementedError(msg)

    return raiser


r.close()
r.execute_command(args=[], options={})
r.lock(
    name="",
    timeout=...,
    sleep=...,
    blocking_timeout=...,
    lock_class=...,
    thread_local=...,
)
r.ping()
r.pipeline(transaction=..., shard_hint=...)
r.response_callbacks()  # ???
r.sentinel(args=[])
r.sentinel_get_master_addr_by_name(service_name="")
r.sentinel_master(service_name="")
r.sentinel_masters()
r.sentinel_monitor(name="", ip="", port=6379)
r.sentinel_remove(name="")
r.sentinel_sentinels(service_name="")
r.sentinel_set(name="", option=None, value=None)
r.sentinel_slaves(service_name="")
r.set_response_callback(command=None, callback=None)
r.transaction(func=None, watches=[], kwargs={})


## Cluster
r.cluster(cluster_arg=None)
# Cluster AddSlots
# Cluster BumpEpoch
# Cluster Count-Failure-Reports
# Cluster CountKeysInSlot
# Cluster DelSlots
# Cluster Failover
# Cluster FlushSlots
# Cluster Forget
# Cluster GetKeysInSlot
# Cluster Info
# Cluster KeySlot
# Cluster Meet
# Cluster MyID
# Cluster Nodes
# Cluster Replicate
# Cluster Reset
# Cluster SaveConfig
# Cluster Set-Config-Epoch
# Cluster SetSlot
# Cluster Slaves
# Cluster Replicas
# Cluster Slots
# ReadOnly
r.readonly()
# ReadWrite
r.readwrite()


## Connection
# Auth
# Client Caching
# Client Id
r.client_id()
# Client Info
# Client Kill
r.client_kill(address="")
r.client_kill_filter(_id=None, _type=None, addr=None, skipme=None)
# Client List
r.client_list()
# Client GetName
r.client_getname()
# Client GetRedir
# Client Unpause
# Client Pause
r.client_pause(timeout=0)
# Client Reply
# Client SetName
r.client_setname(name="")
# Client Tracking
# Client TrackingInfo
# Client Unblock
r.client_unblock(client_id=None, error=False)
# Echo
r.echo(value=None)
# Reset
# Select

## Geo
# GeoAdd
r.geoadd(name="")
# GeoHash
r.geohash(name="")
# GeoPos
r.geopos(name="")
# GeoDist
r.geodist(name="", place1=None, place2=None, unit="m")
# GeoRadius
r.georadius(
    name="",
    longitude=0.0,
    latitude=0.0,
    radius=0.0,
    unit="m",
    withdist=False,
    withcoord=False,
    withhash=False,
    count=None,
    sort=None,
    store=None,
    store_dist=None,
)
# GeoRadiusByMember
r.georadiusbymember(
    name="",
    member=...,
    radius=0.0,
    unit="m",
    withdist=False,
    withcoord=False,
    withhash=False,
    count=None,
    sort=None,
    store=None,
    store_dist=None,
)
# GeoSearch
# GeoSearchStore

## Hashes
# HDel
r.hdel(name="", keys=[])
# HExists
r.hexists(name="", key="")
# HGet
r.hget(name="", key="")
# HGetAll
r.hgetall(name="")
# HIncrBy
r.hincrby(name="", key="", amount=...)
# HIncrByFloat
r.hincrbyfloat(name="", key="", amount=...)
# HKeys
r.hkeys(name="")
# HLen
r.hlen(name="")
# HMGet
r.hmget(name="", keys=..., args=[])
# HMSet
r.hmset(name="", mapping={})
# HSet
r.hset(name="", key="", value=None)
# HSetNX
r.hsetnx(name="", key="", value=None)
# HStrLen
r.hstrlen(name="", key="")
# HVals
r.hvals(name="")
# HScan
r.hscan(name="", cursor=..., match=..., count=...)
r.hscan_iter(name="", match=..., count=...)

## HyperLogLog
# PFAdd
r.pfadd(name="", values=[])
# PFCount
r.pfcount(name="")
# PFMerge
r.pfmerge(dest="", sources=[])

## Keys
# Copy
# Del
r.delete(names="")
# Dump
r.dump(name="")
# Exists
r.exists(names="")
# Expire
r.expire(name="", time=0)
# ExpireAt
r.expireat(name="", when="")
# Keys
r.keys(pattern="*")
# Migrate
r.migrate(
    host="",
    port=6379,
    keys=[],
    destination_db="",
    timeout=0,
    copy=False,
    replace=False,
    auth=None,
)
# Move
r.move(name="", db="")
# Object
# Persist
r.persist(name="")
# PExpire
r.pexpire(name="", time=...)
# PExpireAt
r.pexpireat(name="", when=...)
# PTTL
r.pttl(name="")
# RandomKey
r.randomkey()
# Rename
r.rename(src="", dst="")
# RenameNX
r.renamenx(src="", dst="")
# Restore
r.restore(name="", ttl=..., value=None)
# Sort
r.sort(
    name="",
    start=...,
    num=...,
    by=...,
    get=...,
    desc=...,
    alpha=...,
    store=...,
    groups=...,
)
# Touch
r.touch()
# TTL
# Type
# Unlink
r.unlink()
# Wait
r.wait(num_replicas=0, timeout=0)
# Scan
r.scan(cursor=..., match=..., count=...)
r.scan_iter(match=..., count=...)

## List
# BLPop
r.blpop(keys="", timeout=0)
# BRPop
r.brpop(keys="", timeout=0)
# BRPopLPush
r.brpoplpush(src="", dst="", timeout=0)
# BLMove
# LIndex
r.lindex(name="", index=0)
# LInsert
r.linsert(name="", where=0, refvalue=None, value=None)
# LLen
r.llen(name="")
# LPop
r.lpop(name="")
# LPos
# LPush
r.lpush(name="", values=[])
# LPushX
r.lpushx(name="", value=None)
# LRange
r.lrange(name="", start=..., end=...)
# LRem
r.lrem(name="", count=..., value=None)
# LSet
r.lset(name="", index=..., value=None)
# LTrim
r.ltrim(name="", start=..., end=...)
# RPop
r.rpop(name="")
# RPopLPush
r.rpoplpush(src="", dst="")
# LMove
# RPush
r.rpush(name="", values=[])
# RPushX
r.rpushx(name="", value=None)

## Pub/Sub
# PSubscribe
# PubSub
r.pubsub(shard_hint=..., ignore_subscribe_messages=...)
r.pubsub_channels(pattern="*")
r.pubsub_numpat()
r.pubsub_numsub(args=[])
# Publish
r.publish(channel="", message="")
# PUnsubscribe
# Subcribe
# Unsubscribe

## Scripting
r.register_script(script="")  # ???? used wist evalsha
# Eval
r.eval(script=None, numkeys=None, keys_and_args=[])
# EvalSha
r.evalsha(sha="", numkeys=0, keys_and_args=[])
# Script Debug
# Script Exists
r.script_exists(args=[])
# Script Flush
r.script_flush()
# Script Kill
r.script_kill()
# Script Load
r.script_load(script="")


## Server
# ACL Load
r.acl_load()
# ACL Save
r.acl_save()
# ACL List
r.acl_list()
# ACL Users
r.acl_users()
# ACL GetUser
r.acl_getuser(username="")
# ACL SetUser
r.acl_setuser(
    username="",
    enabled=False,
    passwords=None,
    nopass=False,
    hashed_passwords=None,
    categories=None,
    commands=None,
    keys=None,
    reset=False,
    reset_keys=False,
    reset_passwords=False,
)
# ACL DelUser
r.acl_deluser(username="")
# ACL Cat
@not_implemented
def acl_cat(category=...):
    ...


# ACL GenPass
r.acl_genpass()
# ACL WhoAmI
r.acl_whoami()
# ACL Log
# ACL Help
# BGRewriteAOF
r.bgrewriteaof()
# BGSave
r.bgsave()
# Command
# Command Count
# Command GetKeys
# Command Info
# Config Get
r.config_get(pattern=...)
# Config Rewrite
r.config_rewrite()
# Config Set
r.config_set(name="", value=None)
# Config ResetStat
r.config_resetstat()
# DBSize
r.dbsize()
# Debug Object
r.debug_object(key="")
# Debug Segfault
# FlushAll
r.flushall()
# FlushDB
r.flushdb()
# Info
r.info(section="")
# LolWut
# LastSave
r.lastsave()
# Memory Doctor
# Memory Help
# Memory Malloc-Stats
# Memory Purge
r.memory_purge()
# Memors Stats
r.memory_stats()
# Memory Usage
r.memory_usage(key="", samples=None)
# Module List
# Module Load
# Module Unload
# Monitor
r.monitor()
# Role
# Save
r.save()
# Shutdown
r.shutdown()
# SlaveOf
r.slaveof(host=..., port=...)
# ReplicaOf
# SlowLog
r.slowlog_get(num=0)
r.slowlog_len()
r.slowlog_reset()
# SwapDB
r.swapdb(first="", second="")
# Sync
# PSync
# Time
r.time()
# Latency Docto4
# Latency Graph
# Latency History
# Latency Latest
# Latency Reset
# Latency Help

## Sets
# SAdd
r.sadd(name="", values=[])
# SCard
r.scard(name="")
# SDiff
r.sdiff(keys=..., args=[])
# SDiffScore
r.sdiffstore(dest="", keys=..., args=[])
# SInter
r.sinter(keys="", args=[])
# SInterStore
r.sinterstore(dest=..., keys=..., args=[])
# SIsMember
r.sismember(name="", value=None)
# SMIsMember
# SMembers
r.smembers(name="")
# SMove
r.smove(src="", dst="", value=None)
# SPop
r.spop(name="")
# SRandMember
r.srandmember(name="", number=...)
# SRem
r.srem(name="", values=[])
# SUnion
r.sunion(keys=[], args=[])
# SUnionStore
r.sunionstore(dest, keys=[], args=[])
# SScan
r.sscan(name="", cursor=..., match=..., cmunt=...)
r.sscan_iter(name="", match=..., count=...)

## Sorted Sets
# BZPopMin
r.bzpopmax(keys="", timeout=0)
# BZPopMax
r.bzpopmin(keys="", timeout=0)
# ZAdd
r.zadd(name="", mapping={}, nx=..., xx=..., ch=..., incr=...)
# ZCard
r.zcard(name="")
# ZCount
r.zcount(name="", min=..., max=...)
# ZDiff
# ZDiffStore
# ZIncrBy
r.zincrby(name="", value=Non, amount=...)
# ZInter
r.zscan_iter(name="", match=..., count=..., score_cast_func=...)  # ???
# ZInterStore
r.zinterstore(dest="", keys=[], aggregate=...)
# ZLexCount
r.zlexcount(name="", min=..., max=...)
# ZPopMax
# ZPopMin
# ZRangeStore
# ZRange
# ZRangeByLex
# ZRevRangeByLex
r.zrevrangebylex(name="", max=..., min=..., start=None, num=None)
# ZRangeByScore
r.zrevrangebyscore(
    name="", max=..., min=..., start=..., num=..., withscores=..., score_cast_func=...
)
# ZRank
r.zrank(name="", value=None)
# ZRem
r.zrem(name="", values=[])
# ZRemRangeByLex
r.zremrangebylex(name="", min=..., max=...)
# ZRemRangeByRank
r.zremrangebyrank(name="", min=..., max=...)
# ZRemRangeByScore
r.zremrangebyscore(name="", min=..., max=...)
# ZRevRange
r.zrevrange(name="", start=..., end=..., withscores=..., score_cast_func=...)
# ZRevRAngeByScore
# ZRevRank
r.zrevrank(name="", value=None)
# ZScore
r.zscore(name="", value=None)
# ZUnion
# ZMScore
# ZUnionStore
r.zunionstore(dest="", keys=[], aggregate=...)
# ZScan
r.zscan(name="", cursor=..., match=..., count=..., score_cast_func=...)

## Streams
# XInfo
r.xinfo_consumers(name="", groupname="")
r.xinfo_groups(name="")
r.xinfo_stream(name="")
# XAdd
r.xadd(name="", fields=..., id=..., maxlen=..., approximate=...)
# XTrim
r.xtrim(name="", maxlen=0, approximate=...)
# XDel
r.xdel(name="", ids=[])
# XRange
r.xrange(name="", min=..., max=..., count=...)
# XRevRange
r.xrevrange(name="", max=..., min=..., count=...)
# XLen
r.xlen(name="")
# XRead
r.xread(streams=[], count=..., block=...)
# XGroup
r.xgroup_create(name="", groupname="", id=..., mkstream=...)
r.xgroup_delconsumer(name="", groupname="", consumername="")
r.xgroup_destroy(name="", groupname="")
r.xgroup_setid(name="", groupname="", id=...)
# XReadGroup
r.xreadgroup(groupname="", consumername="", streams=[], count=..., block=..., noack=...)
# XAck
r.xack(name="", groupname="", ids=[])
# XClaim
r.xclaim(
    name="",
    groupname="",
    consumername="",
    min_idle_time=0,
    message_ids=[],
    idle=...,
    time=...,
    retrycount=...,
    force=...,
    justid=...,
)
# XAutoClaim
# XPending
r.xpending(name="", groupname="")
r.xpending_range(name="", groupname="", min=..., max=..., count=..., consumername=...)


## Strings
# Append
r.append(key="", value=None)
# Bitcount
r.bitcount(key="", start=..., end=...)
# Bitfield
r.bitfield(keyy="", default_overflow=None)
# Bitop
r.bitop(operation=None, dest=None, keys=[])
# Bitpos
r.bitpos(key="", bit=None, start=..., end=...)
# Decr
r.decr(name, amount=...)
# DecrBy
r.decrby(name, amount=1)
# Get
r.get(name="")
# GetBit
r.getbit(name="", offset=None)
# GetRange
r.getrange(key="", start=..., end=...)
r.substr(name="", start=..., end=...)  # ???
# GetSet
r.getset(name="", value=None)
# Incr
r.incr(name="", amount=...)
# IncrBy
r.incrby(name="", amount=...)
# IncrByFloat
r.incrbyfloat(name="", amount=...)
# MGet
r.mget(keys="", args=[])
# MSet
r.mset(args=[], kwargs={})
# MSetNx
r.msetnx(args=[], kwargs={})
# PSetEx
r.psetex(name="", time_ms=..., value=None)
# Set
r.set(name="", value=None, ex=None, px=None, nx=None, xx=None)
# SetBit
r.setbit(name="", offset=0, value=None)
# SetEx
r.setex(name="", time=..., value=None)
# SetNx
r.setnx(name="", value=None)
# SetRange
r.setrange(name="", offset=0, value=None)
# StrAlgo
# StrLen
r.strlen(name="")


## Transactions
# Discard
# Exec
# Muli
# Unwatch
r.unwatch()
# Watch
r.watch(names=[])
