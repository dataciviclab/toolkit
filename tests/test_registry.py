import pytest

from toolkit.core.registry import Registry


def test_registry_register_and_create():
    r = Registry()
    r.register("x", lambda a=0: {"a": a})
    assert r.create("x", a=2) == {"a": 2}


def test_registry_unknown_plugin_raises_keyerror():
    r = Registry()
    with pytest.raises(KeyError):
        r.create("missing")