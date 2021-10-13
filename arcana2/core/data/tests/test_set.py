from pathlib import Path
import cloudpickle as cp
from arcana2.core.data.set import Dataset
from arcana2.core.data.spec import DataSource, DataSink
from arcana2.repositories.file_system import FileSystem
from arcana2.dataspaces.clinical import Clinical as cl
from arcana2.datatypes.neuroimaging import dicom, niftix_gz


def test_dataset_pickle(dataset: Dataset, tmp_dir: Path):
    fpath = tmp_dir / 'dataset.pkl'
    with fpath.open("wb") as fp:
        cp.dump(dataset, fp)
    with fpath.open("rb") as fp:
        reloaded = cp.load(fp)
    assert dataset == reloaded
