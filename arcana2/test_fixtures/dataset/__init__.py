import os
from tempfile import mkdtemp
from pathlib import Path
from dataclasses import dataclass
import shutil
from itertools import product
import pytest
from arcana2.repositories.file_system import FileSystem
from arcana2.core.data.enum import DataSpace
from arcana2.core.data.datatype import FileFormat
from arcana2.datatypes.general import text, directory, json
from arcana2.dataspaces.clinical import Clinical
from arcana2.datatypes.neuroimaging import (
    nifti_gz, niftix_gz, niftix, nifti, analyze, mrtrix_image)


class TestDataSpace(DataSpace):
    """Dummy data dimensions for ease of testing"""

    # Per dataset
    _ = 0b0000

    # Basis
    a = 0b1000
    b = 0b0100
    c = 0b0010
    d = 0b0001

    # Secondary combinations
    ab = 0b1100
    ac = 0b1010
    ad = 0b1001
    bc = 0b0110
    bd = 0b0101
    cd = 0b0011

    # Tertiary combinations
    abc = 0b1110
    abd = 0b1101
    acd = 0b1011
    bcd = 0b0111

    # Leaf nodes
    abcd = 0b1111


td = TestDataSpace

dummy_format = FileFormat(name='xyz', extension='.x',
                          side_cars={'y': '.y', 'z': '.z'})


# -----------------------
# Test dataset structures
# -----------------------

@dataclass
class TestDatasetBlueprint():

    hierarchy: list[DataSpace]
    dim_lengths: list[int]  # size of layers a-d respectively
    files: list[str]  # files present at bottom layer
    id_inference: dict[DataSpace, str]  # id_inference dict
    expected_formats: dict[str, tuple[FileFormat, list[str]]]  # expected formats
    to_insert: list[str, tuple[DataSpace, FileFormat, list[str]]]  # files to insert as derivatives


TEST_DATASET_BLUEPRINTS = {
    'full' : TestDatasetBlueprint(  # dataset name
        [td.a, td.b, td.c, td.d],
        [2, 3, 4, 5],
        ['file1.txt', 'file2.nii.gz', 'dir1'],
        {},
        {'file1': [
            (text, ['file1.txt'])],
         'file2': [
            (nifti_gz, ['file2.nii.gz'])],
         'dir1': [
            (directory, ['dir1'])]},
        [('deriv1', td.abcd, text, ['file1.txt']),  # Derivatives to insert
         ('deriv2', td.c, directory, ['dir']),
         ('deriv3', td.bd, text, ['file1.txt'])]
    ),
    'one_layer': TestDatasetBlueprint(
        [td.abcd],
        [1, 1, 1, 5],
        ['file1.nii.gz', 'file1.json', 'file2.nii', 'file2.json'],
        {},
        {'file1': [
            (niftix_gz, ['file1.nii.gz', 'file1.json']),
            (nifti_gz, ['file1.nii.gz']),
            (json, ['file1.json'])],
         'file2': [
            (niftix, ['file2.nii', 'file2.json']),
            (nifti, ['file2.nii']),
            (json, ['file2.json'])]},
        [('deriv1', td.abcd, json, ['file1.json']),
         ('deriv2', td.bc, dummy_format, ['file1.x', 'file1.y', 'file1.z']),
         ('deriv3', td._, mrtrix_image, ['file1.mif'])]
    ),
    'skip_single': TestDatasetBlueprint(
        [td.a, td.bc, td.d],
        [2, 1, 2, 3],
        ['doubledir1', 'doubledir2'],
        {},
        {'doubledir1': [
            (directory, ['doubledir1'])],
         'doubledir2': [
            (directory, ['doubledir2'])]},
        [('deriv1', td.ad, json, ['file1.json'])]
    ),
    'skip_with_inference': TestDatasetBlueprint(
        [td.bc, td.ad],
        [2, 3, 2, 4],
        ['file1.img', 'file1.hdr', 'file2.mif'],
        {td.bc: r'b(?P<b>\d+)c(?P<c>\d+)',
         td.ad: r'a(?P<a>\d+)d(?P<d>\d+)'},
        {'file1': [
            (analyze, ['file1.hdr', 'file1.img'])],
         'file2': [
            (mrtrix_image, ['file2.mif'])]},
        []
    ),
    'redundant': TestDatasetBlueprint(
        [td.abc, td.abcd],  # e.g. XNAT where session ID is unique in project but final layer is organised by timepoint
        [3, 4, 5, 6],
        ['doubledir', 'file1.x', 'file1.y', 'file1.z'],
        {td.abc: r'a(?P<a>\d+)b(?P<b>\d+)c(?P<c>\d+)',
         td.abcd: r'a\d+b\d+c\d+d(?P<d>\d+)'},
        {'doubledir': [
            (directory, ['doubledir'])],
         'file1': [
            (dummy_format, ['file1.x', 'file1.y', 'file1.z'])]},
        [('deriv1', td.d, json, ['file1.json'])]
    )}


GOOD_DATASETS = ['full', 'one_layer', 'skip_single', 'skip_with_inference',
                 'redundant']

# ------------------------------------
# Pytest fixtures and helper functions
# ------------------------------------

@pytest.fixture
def test_dataspace():
    return TestDataSpace


@pytest.fixture
def test_dataspace_location():
    return 'arcana2.test_fixtures.dataset.TestDataSpace'


@pytest.fixture
def test_dicom_dataset_dir(test_ref_data_dir):
    return Path(__file__).parent / 'test-dataset'


@pytest.fixture
def dicom_dataset(test_dicom_dataset_dir):
    return FileSystem().dataset(
        test_dicom_dataset_dir,
        hierarchy=[Clinical.session])


@pytest.fixture(params=GOOD_DATASETS)
def dataset(work_dir, request):
    dataset_name = request.param
    blueprint = TEST_DATASET_BLUEPRINTS[dataset_name]
    dataset_path = get_dataset_path(dataset_name, work_dir)
    make_dataset(blueprint, dataset_path)
    yield dataset
    #shutil.rmtree(dataset.name)


@pytest.fixture
def tmp_dir():
    tmp_dir = Path(mkdtemp())
    yield tmp_dir
    shutil.rmtree(tmp_dir)


def make_dataset(blueprint, dataset_path):
    create_dataset_in_repo(blueprint, dataset_path)
    return access_dataset(blueprint, dataset_path)


def create_dataset_in_repo(blueprint, dataset_path):
    "Creates a dataset from parameters in TEST_DATASETS"
    dataset_path.mkdir(exist_ok=True, parents=True)
    for id_tple in product(*(list(range(d)) for d in blueprint.dim_lengths)):
        ids = dict(zip(TestDataSpace.basis(), id_tple))
        dpath = dataset_path
        for layer in blueprint.hierarchy:
            dpath /= ''.join(f'{b}{ids[b]}' for b in layer.nonzero_basis())
        os.makedirs(dpath)
        for fname in blueprint.files:
            create_test_file(fname, dpath)


def access_dataset(blueprint, dataset_path):
    dataset = FileSystem().dataset(
        dataset_path,
        hierarchy=blueprint.hierarchy,
        id_inference=blueprint.id_inference)
    dataset.blueprint = blueprint
    return dataset


def get_dataset_path(name, base_dir):
    return base_dir / name


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
        f.write(f'{fname}')
    return fpath

