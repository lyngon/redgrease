from redgrease import KeysReader, StreamReader, cmd, hashtag, log


def process(x):
    """
    Processes a message from the local expiration stream
    Note: in this example we simply print to the log, but feel free to replace
    this logic with your own, e.g. an HTTP request to a REST API or a call to an
    external data store.
    """
    log(f"Key '{x['value']['key']}' expired at {x['id'].split('-')[0]}")


# Capture an expiration event and adds it to the shard's local 'expired' stream
KeysReader().keys().foreach(
    lambda key: cmd.xadd(f"expired:{hashtag()}", {"key": key})
).register(prefix="*", mode="sync", eventTypes=["expired"], readValue=False)

# Consume new messages from expiration streams and process them somehow
StreamReader().foreach(process).register(prefix="expired:*", batch=100, duration=1)
