import os
import os.path
from tempfile import mkdtemp
from pathlib import Path
from dataclasses import dataclass
import shutil
import operator as op
from itertools import product
from functools import reduce
from copy import copy
import pytest
from arcana2.repositories.file_system import FileSystem
from arcana2.core.data.enum import DataDimension
from arcana2.core.data.set import Dataset
from arcana2.core.data.format import FileFormat
from arcana2.data_formats.general import text, directory, json
from arcana2.data_formats.neuroimaging import (
    nifti_gz, niftix_gz, niftix, nifti, analyze, mrtrix_image)


class TestDimension(DataDimension):
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


td = TestDimension

dummy_format = FileFormat(name='xyz', extension='.x',
                          side_cars={'y': '.y', 'z': '.z'})


def test_find_nodes(dataset: Dataset):
    for freq in TestDimension:
        # For all non-zero bases in the frequency, multiply the dim lengths
        # together to get the combined number of nodes expected for that
        # frequency
        num_nodes = reduce(
            op.mul, (l for l, b in zip(dataset.dim_lengths, freq) if b), 1)
        assert len(dataset.nodes(freq)) == num_nodes, (
            f"{freq} doesn't match {len(dataset.nodes(freq))} vs {num_nodes}")


def test_get_items(dataset: Dataset):
    source_files = {}
    for fg_name, formats in dataset.blueprint.expected_formats.items():
        for format, files in formats:
            source_name = fg_name + format.name
            dataset.add_source(source_name, fg_name, format)
            source_files[source_name] = set(files)
    for node in dataset.nodes(td.abcd):
        for source_name, files in source_files.items():
            item = node[source_name]
            item.get()
            assert set(os.path.basename(p) for p in item.cache_paths) == files


def test_put_item(dataset: Dataset, tmp_dir: str):
    for freq, name, data_format, files in dataset.blueprint.to_insert:
        dataset.add_sink(name=name, format=data_format, frequency=freq)
        node = next(dataset.nodes(freq))
        item = node[name]
        for fname in files:
            test_file = create_test_file(fname, tmp_dir / name)
        item.put()

# -----------------------
# Test dataset structures
# -----------------------

@dataclass
class TestDatasetBlueprint():

    hiearchy: list[DataDimension]
    layer_sizes: list[int]  # size of layers a-d respectively
    files: list[str]  # files present at bottom layer
    id_inference: dict[DataDimension, str]  # id_inference dict
    expected_formats: dict[str, tuple[FileFormat, list[str]]]  # expected formats
    to_insert: list[tuple(DataDimension, str, FileFormat, list[str])]  # files to insert as derivatives


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
        [(td.d, 'deriv1', text, ['file1.txt']),  # Derivatives to insert
         (td.c, 'deriv2', directory, ['dir1', 'dir2', 'file1.png']),
         (td.bd, 'deriv3', text, ['file1.txt'])]
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
        [(td.abcd, 'deriv1', json, ['file1.json']),
         (td.bc, 'deriv2', dummy_format, ['file1.x', 'file1.y', 'file1.z']),
         (td._, 'deriv3', mrtrix_image, ['file1.mif'])]
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
        [(td.ad, 'deriv1', json, ['file1.json'])]
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
        [(td.d, 'deriv1', json, ['file1.json'])]
    )}


GOOD_DATASETS = ['full', 'one_layer', 'skip_single', 'skip_with_inference',
                 'redundant']

# ------------------------------------
# Pytest fixtures and helper functions
# ------------------------------------


@pytest.fixture(params=GOOD_DATASETS)
def dataset(work_dir, request):
    dataset_name = request.param
    create_dataset_in_repo(dataset_name, work_dir)
    dataset = access_dataset(dataset_name, work_dir)
    yield dataset
    shutil.rmtree(dataset.name)


@pytest.fixture
def tmp_dir():
    tmp_dir = Path(mkdtemp())
    yield tmp_dir
    shutil.rmtree(tmp_dir)


def create_dataset_in_repo(name, base_dir):
    "Creates a dataset from parameters in TEST_DATASETS"

    blueprint = TEST_DATASET_BLUEPRINTS[name]

    dataset_path = get_dataset_path(name, base_dir)
    os.mkdir(dataset_path)

    for id_tple in product(*(list(range(d)) for d in blueprint.dim_lengths)):
        ids = dict(zip(TestDimension.basis(), id_tple))
        dpath = dataset_path
        for layer in blueprint.hierarchy:
            dname = ''.join(f'{b}{ids[b]}' for b in layer.nonzero_basis())
            dpath = os.path.join(dpath, dname)
        os.makedirs(dpath)
        for fname in blueprint.files:
            create_test_file(fname, dpath)


def access_dataset(name, base_dir):
    blueprint = TEST_DATASET_BLUEPRINTS[name]
    dataset = FileSystem().dataset(
        get_dataset_path(name, base_dir),
        hierarchy=blueprint.hierarchy,
        id_inference=blueprint.id_inference)
    dataset.blueprint = blueprint
    return dataset


def get_dataset_path(name, base_dir):
    return base_dir / name


def create_test_file(fname, dpath):
    os.makedirs(dpath, exist_ok=True)
    top_path = fpath = Path(dpath) / fname
    # Make double dir
    if fname.startswith('doubledir'):
        os.mkdir(fpath)
        fname = 'dir'
        fpath = fpath / fname
    if fname.startswith('dir'):
        os.mkdir(fpath)
        fname = 'test.txt'
        fpath = fpath / fname
    with open(fpath, 'w') as f:
        f.write(f'test {fname}')
    return top_path
