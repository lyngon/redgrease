from redgrease import GB, execute, hashtag


def count_hashtags(_):
    ht = hashtag()
    execute("HINCRBY", "hashtag", ht, 1)


gb = GB()
gb.foreach(count_hashtags)
gb.run()
