import os
from tempfile import mkdtemp
from pathlib import Path
import typing as ty
import zipfile
from dataclasses import dataclass
import shutil
from itertools import product
import pytest
from arcana.data.stores.file_system import FileSystem
from arcana.core.data.dimensions import DataDimensions
from arcana.core.utils import set_cwd
from arcana.core.data.type import FileFormat
from arcana.data.types.general import text, directory, json
from arcana.data.dimensions.medicalimaging import ClinicalTrial
from arcana.data.types.medicalimaging import (
    nifti_gz, niftix_gz, niftix, nifti, analyze, mrtrix_image)


class TestDataDimensions(DataDimensions):
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


td = TestDataDimensions

dummy_format = FileFormat(name='xyz', extension='.x',
                          side_cars={'y': '.y', 'z': '.z'})


# -----------------------
# Test dataset structures
# -----------------------

@dataclass
class TestDatasetBlueprint():

    hierarchy: ty.List[DataDimensions]
    dim_lengths: ty.List[int]  # size of layers a-d respectively
    files: ty.List[str]  # files present at bottom layer
    id_inference: ty.Dict[DataDimensions, str]  # id_inference dict
    expected_formats: ty.Dict[str, ty.Tuple[FileFormat, ty.List[str]]]  # expected formats
    to_insert: ty.List[ty.Tuple[str, ty.Tuple[DataDimensions, FileFormat, ty.List[str]]]]  # files to insert as derivatives


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
    ),
    'concatenate_test': TestDatasetBlueprint(
        [TestDataDimensions.abcd],  # e.g. XNAT where session ID is unique in project but final layer is organised by timepoint
        [1, 1, 1, 2],
        ['file1.txt', 'file2.txt'],
        {}, {}, []),
    'concatenate_zip_test': TestDatasetBlueprint(
        [TestDataDimensions.abcd],  # e.g. XNAT where session ID is unique in project but final layer is organised by timepoint
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
    return TestDataDimensions


@pytest.fixture
def test_dataspace_location():
    return __name__ + '.TestDataDimensions'


@pytest.fixture
def test_dicom_dataset_dir(test_ref_data_dir):
    return Path(__file__).parent / 'test-dataset'


@pytest.fixture
def dicom_dataset(test_dicom_dataset_dir):
    return FileSystem().dataset(
        test_dicom_dataset_dir,
        hierarchy=[ClinicalTrial.session])


@pytest.fixture(params=GOOD_DATASETS)
def dataset(work_dir, request):
    dataset_name = request.param
    blueprint = TEST_DATASET_BLUEPRINTS[dataset_name]
    dataset_path = get_dataset_path(dataset_name, work_dir)
    dataset = make_dataset(blueprint, dataset_path)
    yield dataset
    #shutil.rmtree(dataset.id)


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
        ids = dict(zip(TestDataDimensions.basis(), id_tple))
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
    dpath = Path(dpath)
    os.makedirs(dpath, exist_ok=True)
    next_part = fname
    if next_part.endswith('.zip'):
        next_part = next_part.strip('.zip')
    fpath = Path(next_part)
    # Make double dir
    if next_part.startswith('doubledir'):
        os.makedirs(dpath / fpath, exist_ok=True)
        next_part = 'dir'
        fpath /= next_part
    if next_part.startswith('dir'):
        os.makedirs(dpath / fpath, exist_ok=True)
        next_part = 'test.txt'
        fpath /= next_part
    if not fpath.suffix:
        fpath = fpath.with_suffix('.txt')
    with open(dpath / fpath, 'w') as f:
        f.write(f'{fname}')
    if fname.endswith('.zip'):
        with zipfile.ZipFile(dpath / fname, mode='w') as zfile, set_cwd(dpath):
            zfile.write(fpath)
        (dpath / fpath).unlink()
        fpath = Path(fname)
    return fpath
