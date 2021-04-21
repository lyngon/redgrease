import requests

# # Begin Example
import redgrease


# This function runs **on** the Redis server.
def download_image(annotation):
    img_id = annotation["image_id"]
    img_key = f"image:{img_id}"
    if redgrease.cmd.hexists(img_key, "image_data"):  # <- hexists
        # image already downloaded
        return img_key
    redgrease.log(f"Downloadign image for annotation: {annotation}")
    image_url = redgrease.cmd.hget(img_key, "url")  # <- hget
    response = requests.get(image_url)
    redgrease.cmd.hset(img_key, "image_data", bytes(response.content))  # <- hset
    return img_key


# Redis connection (with Gears)
connection = redgrease.RedisGears()

# Automatically download corresponding image, whenever an annotation is created.
image_keys = (
    redgrease.KeysReader()
    .values(type="hash", event="hset")
    .foreach(download_image, requirements=["requests"])
    .register("annotation:*", on=connection)
)
# # End Example

some_image_urls = [
    "http://images.cocodataset.org/train2017/000000483381.jpg",
    "http://images.cocodataset.org/train2017/000000237137.jpg",
    "http://images.cocodataset.org/train2017/000000017267.jpg",
    "http://images.cocodataset.org/train2017/000000197756.jpg",
    "http://images.cocodataset.org/train2017/000000193332.jpg",
    "http://images.cocodataset.org/train2017/000000475564.jpg",
]

for i, url in enumerate(some_image_urls):
    connection.hset(
        f"image:{i}", mapping={"id": i, "url": url, "dataset": "dataset:coco"}
    )

# lets add some annotations
connection.hset(
    "annotation:0",
    mapping={
        "id": 0,
        "image_id": 2,
        "category": 1,
    },
)
connection.hset(
    "annotation:1",
    mapping={
        "id": 1,
        "image_id": 4,
        "category": 6,
    },
)
connection.hset(
    "annotation:2",
    mapping={
        "id": 2,
        "image_id": 2,
        "category": 27,
    },
)

assert not connection.hexists("image:0", "image_data")
assert not connection.hexists("image:1", "image_data")
assert connection.hexists("image:2", "image_data")
assert not connection.hexists("image:3", "image_data")
assert connection.hexists("image:4", "image_data")
assert not connection.hexists("image:5", "image_data")
