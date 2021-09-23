import os
from tempfile import mkdtemp
import shutil
import logging
import pytest
from pathlib import Path
from arcana2.core.data.spec import DataSource, DataSink
from arcana2.repositories import FileSystem, Xnat
from arcana2.dimensions.clinical import Clinical
from arcana2.data_formats import dicom, niftix_gz

# Set DEBUG logging for unittests
logger = logging.getLogger('arcana')
logger.setLevel(logging.DEBUG)
fch = logging.FileHandler('./test.log')
fch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fch.setFormatter(formatter)
logger.addHandler(fch)

sch = logging.StreamHandler()
sch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
sch.setFormatter(formatter)
logger.addHandler(sch)


TEST_DATA_DIR = os.path.realpath(
    os.path.join(os.path.dirname(__file__), "tests", "data"))


@pytest.fixture(scope='session')
def test_ref_data_dir():
    return TEST_DATA_DIR


@pytest.fixture
def test_dicom_dataset_dir(test_ref_data_dir):
    return os.path.join(test_ref_data_dir, 'test-dataset')


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
            data_format=dicom,
            frequency=Clinical.session)}


@pytest.fixture
def nifti_outputs():
    return {
        'out_file': DataSink(
            path='output-nifti',
            data_format=niftix_gz,
            frequency=Clinical.session)}


@pytest.fixture
def work_dir():
    work_dir = mkdtemp()
    yield Path(work_dir)
    shutil.rmtree(work_dir)


# For debugging in IDE's don't catch raised exceptions and let the IDE
# break at it
if os.getenv('_PYTEST_RAISE', "0") != "0":

    @pytest.hookimpl(tryfirst=True)
    def pytest_exception_interact(call):
        raise call.excinfo.value

    @pytest.hookimpl(tryfirst=True)
    def pytest_internalerror(excinfo):
        raise excinfo.value
