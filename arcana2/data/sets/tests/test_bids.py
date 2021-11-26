import tempfile
from bids_validator import BIDSValidator
from arcana2.data.sets.bids import BidsDataset


def test_bids_roundtrip(work_dir):

    path = work_dir / 'bids-dataset'
    name = 'bids-dataset'

    dataset = BidsDataset.create(path, name,
                                 subject_ids=[str(i) for i in range(1, 4)],
                                 session_ids=[str(i) for i in range(1, 3)])
    assert BIDSValidator().is_bids(str(dataset.id))
    reloaded = BidsDataset.load(path)
    assert dataset == reloaded
    