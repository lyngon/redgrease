from redgrease import execute
from redgrease.client import RedisGears
from functools import lru_cache


def runtime_execute_command(self, *args, **options):
    "Execute a command in local Gear runtime and return a parsed response"
    command_name = args[0]
    response = execute(*args)
    if command_name in self.response_callbacks:
        return self.response_callbacks[command_name](response, **options)
    return response


@lru_cache
def get_runtime_client():
    runtime_client = RedisGears(connection_pool=...)
    runtime_client.execute_command = runtime_execute_command
    return runtime_client


execute = get_runtime_client()  # noqa: F811 - Used as exported var


# ## Redis v6 commands
# # Cluster
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
# ReadWrite


# # Connection
# Auth
# Client Caching
# Client Id
# Client Info
# Client Kill
# Client List
# Client GetName
# Client GetRedir
# Client Unpause
# Client Pause
# Client Reply
# Client SetName
# Client Tracking
# Client TrackingInfo
# Client Unblock
# Echo
# Reset
# Select

# # Geo
# GeoAdd
# GeoHash
# GeoPos
# GeoDist
# GeoRadius
# GeoRadiusByMember
# GeoSearch
# GeoSearchStore

# # Hashes
# HDel
# HExists
# HGet
# HGetAll
# HIncrBy
# HIncrByFloat
# HKeys
# HLen
# HMGet
# HMSet
# HSet
# HSetNX
# HStrLen
# HVals
# HScan

# # HyperLogLog
# PFAdd
# PFCount
# PFMerge

# # Keys
# Copy
# Del
# Dump
# Exists
# Expire
# ExpireAt
# Keys
# Migrate
# Move
# Object
# Persist
# PExpire
# PExpireAt
# PTTL
# RandomKey
# Rename
# RenameNX
# Restore
# Sort
# Touch
# TTL
# Type
# Unlink
# Wait
# Scan

# # List
# BLPop
# BRPop
# BRPopLPush
# BLMove
# LIndex
# LInsert
# LLen
# LPop
# LPos
# LPush
# LPushX
# LRange
# LRem
# LSet
# LTrim
# RPop
# RPopLPush
# LMove
# RPush
# RPushX

# # Pub/Sub
# PSubscribe
# PubSub
# Publish
# PUnsubscribe
# Subcribe
# Unsubscribe

# # Scripting
# Eval
# EvalSha
# Script Debug
# Script Exists
# Script Flush
# Script Kill
# Script Load

# # Server
# ACL Load
# ACL Save
# ACL List
# ACL Users
# ACL GetUser
# ACL SetUser
# ACL DelUser
# ACL Cat
# ACL GenPass
# ACL WhoAmI
# ACL Log
# ACL Help
# BGRewriteAOF
# BGSave
# Command
# Command Count
# Command GetKeys
# Command Info
# Config Get
# Config Rewrite
# Config Set
# Config ResetStat
# DBSize
# Debug Object
# Debug Segfault
# FlushAll
# FlushDB
# Info
# LolWut
# LastSave
# Memory Doctor
# Memory Help
# Memory Malloc-Stats
# Memory Purge
# Memors Stats
# Memory Usage
# Module List
# Module Load
# Module Unload
# Monitor
# Role
# Save
# Shutdown
# SlaveOf
# ReplicaOf
# SlowLog
# SwapDB
# Sync
# PSync
# Time
# Latency Docto4
# Latency Graph
# Latency History
# Latency Latest
# Latency Reset
# Latency Help

# # Sets
# SAdd
# SCard
# SDiff
# SDiffScore
# SInter
# SInterStore
# SIsMember
# SMIsMember
# SMembers
# SMove
# SPop
# SRandMember
# SRem
# SUnion
# SUnionStore
# SScan

# # Sorted Sets
# BZPopMin
# BZPopMax
# ZAdd
# ZCard
# ZCount
# ZDiff
# ZDiffStore
# ZIncrBy
# ZInter
# ZInterStore
# ZLexCount
# ZPopMax
# ZPopMin
# ZRangeStore
# ZRange
# ZRangeByLex
# ZRevRangeByLex
# ZRangeByScore
# ZRank
# ZRem
# ZRemRangeByLex
# ZRemRangeByRank
# ZRemRangeByScore
# ZRevRange
# ZRevRAngeByScore
# ZRevRank
# ZScore
# ZUnion
# ZMScore
# ZUnionStore
# ZScan

# # Streams
# XInfo
# XAdd
# XTrim
# XDel
# XRange
# XRevRange
# XLen
# XRead
# XGroup
# XReadGroup
# XAck
# XClaim
# XAutoClaim
# XPending


# # Strings
# Append
# Bitcount
# Bitfield
# Bitop
# Bitpos
# Decr
# DecrBy
# Get
# GetBit
# GetRange
# GetSet
# Incr
# IncrBy
# IncrByFloat
# MGet
# MSet
# MSetNx
# PSetEx
# Set
# SetBit
# SetEx
# SetNx
# SetRange
# StrAlgo
# StrLen


# # Transactions
# Discard
# Exec
# Muli
# Unwatch
# Watch
