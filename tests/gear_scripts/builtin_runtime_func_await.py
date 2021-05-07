import asyncio

from redgrease import GB


async def c(r):
    await asyncio.sleep(1)
    return r


GB("ShardsIDReader").map(c).run()
