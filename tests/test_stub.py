from redgrease import runtime


def test_default_builder():
    assert runtime.GearsBuilder() is not None


def test_shorthand_builder():
    assert runtime.GB() is not None
