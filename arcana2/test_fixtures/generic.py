import os
import pytest
from pathlib import Path
from tempfile import mkdtemp
import shutil
from arcana2.core.data.spec import DataSource, DataSink
from arcana2.repositories import FileSystem, Xnat
from arcana2.dataspaces.clinical import Clinical
from arcana2.datatypes import dicom, niftix_gz


def create_test_file(fname, dpath):
    fpath = Path(fname)
    os.makedirs(dpath, exist_ok=True)
    # Make double dir
    if fname.startswith('doubledir'):
        os.makedirs(dpath / fpath, exist_ok=True)
        fname = 'dir'
        fpath /= fname
    if fname.startswith('dir'):
        os.makedirs(dpath / fpath, exist_ok=True)
        fname = 'test.txt'
        fpath /= fname
    with open(dpath / fpath, 'w') as f:
        f.write(f'test {fname}')
    return fpath


@pytest.fixture(scope='session')
def test_ref_data_dir():
    return Path(__file__).parent.parent.joinpath("tests", "data").absolute()


@pytest.fixture
def test_dicom_dataset_dir(test_ref_data_dir):
    return test_ref_data_dir / 'test-dataset'


@pytest.fixture
def dicom_dataset(test_dicom_dataset_dir):
    return FileSystem().dataset(
        test_dicom_dataset_dir,
        hierarchy=[Clinical.session])


@pytest.fixture
def dicom_inputs():
    return {
        'in_dir': DataSource(
            path='sample-dicom',
            datatype=dicom,
            frequency=Clinical.session)}


@pytest.fixture
def nifti_outputs():
    return {
        'out_file': DataSink(
            path='output-nifti',
            datatype=niftix_gz,
            frequency=Clinical.session)}


@pytest.fixture
def work_dir():
    work_dir = mkdtemp()
    yield Path(work_dir)
    shutil.rmtree(work_dir)