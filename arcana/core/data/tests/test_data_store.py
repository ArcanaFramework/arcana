from arcana.core.data.store import DataStore


def test_singletons():
    assert sorted(DataStore.singletons()) == ["file"]
