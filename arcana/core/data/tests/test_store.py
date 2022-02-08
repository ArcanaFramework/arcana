from arcana.core.data.store import DataStore


def test_singletons():
    assert sorted(DataStore.singleons()) == ['bids', 'file_system']    