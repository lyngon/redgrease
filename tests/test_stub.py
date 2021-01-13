import redgrease


def test_default_builder():
    assert redgrease.GearsBuilder() is not None


def test_shorthand_builder():
    assert redgrease.GB() is not None
