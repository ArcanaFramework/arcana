from arcana.core.utils import serialise, unserialise


def test_dataset_serialise_roundtrip(dataset):

    dct = serialise(dataset, skip=['store'])
    unserialised = unserialise(dct, store=dataset.store)
    assert isinstance(dct, dict)
    assert 'store' not in dct
    del dataset.blueprint
    assert dataset == unserialised
