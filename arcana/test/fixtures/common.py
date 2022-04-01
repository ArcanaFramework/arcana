import os
from tempfile import mkdtemp
from pathlib import Path
import shutil
import pytest
from arcana.data.stores.common import FileSystem
from arcana.data.formats.common import Text, Directory, Json
from arcana.data.formats.medimage import (
    NiftiGz, NiftiXGz, NiftiX, Nifti, Analyze, MrtrixImage)
from pathlib import Path
import pytest
from click.testing import CliRunner
from arcana.test.tasks import add, path_manip, attrs_func, A, B, C
from arcana.test.datasets import (
    TestDatasetBlueprint, TestDataSpace as TDS, Xyz, make_dataset)


@pytest.fixture
def work_dir():
    # work_dir = Path.home() / '.arcana-tests'
    # work_dir.mkdir(exist_ok=True)
    # return work_dir
    work_dir = mkdtemp()
    yield Path(work_dir)
    shutil.rmtree(work_dir)


TEST_TASKS = {
    'add': (add,
            {'a': 4,
             'b': 5},
            {'out': 9}),
    'path_manip': (path_manip,
                   {'dpath': Path('/home/foo/Desktop'),
                    'fname': 'bar.txt'},
                   {'path': '/home/foo/Desktop/bar.txt',
                    'suffix': '.txt'}),
    'attrs_func': (attrs_func, 
                   {'a': A(x=2, y=4),
                    'b': B(u=2.5, v=1.25)},
                   {'c': C(z=10)})}

BASIC_TASKS = ['add', 'path_manip', 'attrs_func']

FILE_TASKS = ['concatenate']


@pytest.fixture(params=BASIC_TASKS)
def pydra_task_details(request):
    func_name = request.param
    return ('arcana.tasks.tests.fixtures' + func_name,) + tuple(
        TEST_TASKS[func_name][1:])
            

@pytest.fixture(params=BASIC_TASKS)
def pydra_task(request):
    task, args, expected_out = TEST_TASKS[request.param]
    task.test_args = args  # stash args away in task object for future access
    return task


TEST_DATASET_BLUEPRINTS = {
    'full' : TestDatasetBlueprint(  # dataset name
        [TDS.a, TDS.b, TDS.c, TDS.d],
        [2, 3, 4, 5],
        ['file1.txt', 'file2.nii.gz', 'dir1'],
        [],
        {'file1': [
            (Text, ['file1.txt'])],
         'file2': [
            (NiftiGz, ['file2.nii.gz'])],
         'dir1': [
            (Directory, ['dir1'])]},
        [('deriv1', TDS.abcd, Text, ['file1.txt']),  # Derivatives to insert
         ('deriv2', TDS.c, Directory, ['dir']),
         ('deriv3', TDS.bd, Text, ['file1.txt'])]
    ),
    'one_layer': TestDatasetBlueprint(
        [TDS.abcd],
        [1, 1, 1, 5],
        ['file1.nii.gz', 'file1.json', 'file2.nii', 'file2.json'],
        [],
        {'file1': [
            (NiftiXGz, ['file1.nii.gz', 'file1.json']),
            (NiftiGz, ['file1.nii.gz']),
            (Json, ['file1.json'])],
         'file2': [
            (NiftiX, ['file2.nii', 'file2.json']),
            (Nifti, ['file2.nii']),
            (Json, ['file2.json'])]},
        [('deriv1', TDS.abcd, Json, ['file1.json']),
         ('deriv2', TDS.bc, Xyz, ['file1.x', 'file1.y', 'file1.z']),
         ('deriv3', TDS._, MrtrixImage, ['file1.mif'])]
    ),
    'skip_single': TestDatasetBlueprint(
        [TDS.a, TDS.bc, TDS.d],
        [2, 1, 2, 3],
        ['doubledir1', 'doubledir2'],
        [],
        {'doubledir1': [
            (Directory, ['doubledir1'])],
         'doubledir2': [
            (Directory, ['doubledir2'])]},
        [('deriv1', TDS.ad, Json, ['file1.json'])]
    ),
    'skip_with_inference': TestDatasetBlueprint(
        [TDS.bc, TDS.ad],
        [2, 3, 2, 4],
        ['file1.img', 'file1.hdr', 'file2.mif'],
        [(TDS.bc, r'b(?P<b>\d+)c(?P<c>\d+)'),
         (TDS.ad, r'a(?P<a>\d+)d(?P<d>\d+)')],
        {'file1': [
            (Analyze, ['file1.hdr', 'file1.img'])],
         'file2': [
            (MrtrixImage, ['file2.mif'])]},
        []
    ),
    'redundant': TestDatasetBlueprint(
        [TDS.abc, TDS.abcd],  # e.g. XNAT where session ID is unique in project but final layer is organised by timepoint
        [3, 4, 5, 6],
        ['doubledir', 'file1.x', 'file1.y', 'file1.z'],
        [(TDS.abc, r'a(?P<a>\d+)b(?P<b>\d+)c(?P<c>\d+)'),
         (TDS.abcd, r'a\d+b\d+c\d+d(?P<d>\d+)')],
        {'doubledir': [
            (Directory, ['doubledir'])],
         'file1': [
            (Xyz, ['file1.x', 'file1.y', 'file1.z'])]},
        [('deriv1', TDS.d, Json, ['file1.json'])]
    ),
    'concatenate_test': TestDatasetBlueprint(
        [TDS.abcd],  # e.g. XNAT where session ID is unique in project but final layer is organised by timepoint
        [1, 1, 1, 2],
        ['file1.txt', 'file2.txt'],
        {}, {}, []),
    'concatenate_zip_test': TestDatasetBlueprint(
        [TDS.abcd],  # e.g. XNAT where session ID is unique in project but final layer is organised by timepoint
        [1, 1, 1, 1],
        ['file1.zip', 'file2.zip'],
        {}, {}, [])}


GOOD_DATASETS = ['full', 'one_layer', 'skip_single', 'skip_with_inference',
                 'redundant']

# ------------------------------------
# Pytest fixtures and helper functions
# ------------------------------------

@pytest.fixture
def test_dataspace():
    return TDS


@pytest.fixture
def test_dataspace_location():
    return __name__ + '.TestDataSpace'


@pytest.fixture
def test_dicom_dataset_dir(test_ref_data_dir):
    return Path(__file__).parent / 'test-dataset'


@pytest.fixture
def dicom_dataset(test_dicom_dataset_dir):
    return FileSystem().dataset(
        test_dicom_dataset_dir,
        hierarchy=['session'])


@pytest.fixture(params=GOOD_DATASETS)
def dataset(work_dir, request):
    dataset_name = request.param
    blueprint = TEST_DATASET_BLUEPRINTS[dataset_name]
    dataset_path = work_dir / dataset_name
    dataset = make_dataset(blueprint, dataset_path)
    yield dataset
    #shutil.rmtree(dataset.id)


@pytest.fixture
def saved_dataset(work_dir):

    blueprint = TestDatasetBlueprint(
        [TDS.abcd],  # e.g. XNAT where session ID is unique in project but final layer is organised by timepoint
        [1, 1, 1, 1],
        ['file1.txt', 'file2.txt'],
        {}, {}, [])

    dataset_path = work_dir / 'saved_dataset'
    dataset = make_dataset(blueprint, dataset_path)
    dataset.save()
    return dataset


@pytest.fixture
def tmp_dir():
    tmp_dir = Path(mkdtemp())
    yield tmp_dir
    shutil.rmtree(tmp_dir)



# For debugging in IDE's don't catch raised exceptions and let the IDE
# break at it
if os.getenv('_PYTEST_RAISE', "0") != "0":

    @pytest.hookimpl(tryfirst=True)
    def pytest_exception_interact(call):
        raise call.excinfo.value

    @pytest.hookimpl(tryfirst=True)
    def pytest_internalerror(excinfo):
        raise excinfo.value

    catch_cli_exceptions = False
else:
    catch_cli_exceptions = True

@pytest.fixture
def cli_runner():
    def invoke(*args, **kwargs):
        runner = CliRunner()
        result = runner.invoke(*args, catch_exceptions=catch_cli_exceptions,
                               **kwargs)
        return result
    return invoke