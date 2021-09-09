import os.path
import tempfile
import pytest
import cloudpickle as cp
from arcana2.core.data.spec import DataSource, DataSink
from arcana2.repositories.file_system import FileSystem
from arcana2.dimensions.clinical import Clinical
from arcana2.file_formats.neuroimaging import dicom, niftix_gz


@pytest.fixture
def dataset(test_data):
    return FileSystem().dataset(
        os.path.join(test_data, 'test-repo'),
        hierarchy=[Clinical.session])

@pytest.fixture
def inputs():
    return {
        'in_dir': DataSource(
            path='sample-dicom',
            data_format=dicom,
            frequency=Clinical.session)}

@pytest.fixture
def outputs():
    return {
        'out_file': DataSink(
            path='output-nifti',
            data_format=niftix_gz,
            frequency=Clinical.session)}

def test_dataset_pickle(dataset):
    with tempfile.TemporaryFile(mode='w+b') as f:
        cp.dump(dataset, f)
    assert True


def test_workflow_pickle(dataset, inputs, outputs):
    workflow = dataset.workflow('test', inputs, outputs)
    workflow.pickle_task()
    assert True