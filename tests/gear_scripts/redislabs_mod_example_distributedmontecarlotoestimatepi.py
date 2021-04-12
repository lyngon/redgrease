from redgrease import PythonReader, cmd

TOTAL_DARTS = 1000000  # total number of darts


def inside(p):
    """ Generates a random point that is or isn't inside the circle """
    from random import random

    x, y = random(), random()
    return x * x + y * y < 1


def throws():
    """ Calculates each shard's number of throws """
    global TOTAL_DARTS
    throws = TOTAL_DARTS
    cluster = cmd.gears.infocluster()
    if cluster:  # assume a cluster
        n = len(cluster.shards)  # number of shards
        ids = sorted([shard.id for shard in cluster.shards])  # shards' IDs list
        i = ids.index(cluster.my_id)  # my index
        throws = TOTAL_DARTS // n  # minimum throws per shard
        if i == 0 and TOTAL_DARTS % n > 0:  # first shard gets remainder
            throws += 1
    yield throws


def estimate(hits):
    """ Estimates Pi's value from hits """
    from math import log10

    hits = hits * 4  # one quadrant is used
    r = hits / 10 ** int(log10(hits))  # make it irrational
    return f"Pi's estimated value is {r}"


fun = (
    PythonReader()
    .flatmap(lambda x: [i for i in range(int(x))])  # throw the local darts
    .filter(inside)  # throw out missed darts
    .accumulate(lambda a, x: 1 + (a if a else 0))  # count the remaining darts
    .collect()  # collect the results
    .accumulate(lambda a, x: x + (a if a else 0))  # merge darts' counts
    .map(estimate)  # four pieces of pie
)
fun.run(throws)
