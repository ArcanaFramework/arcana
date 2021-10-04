import os
import os.path
from tempfile import mkdtemp
from pathlib import Path
import shutil
import operator as op
from itertools import product
from functools import reduce
from copy import copy
import pytest
from arcana2.repositories.file_system import FileSystem
from arcana2.core.data.enum import DataDimension
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


def test_construct_tree(dataset):
    for freq in TestDimension:
        # For all non-zero bases in the frequency, multiply the dim lengths
        # together to get the combined number of nodes expected for that
        # frequency
        num_nodes = reduce(
            op.mul, (l for l, b in zip(dataset.dim_lengths, freq) if b), 1)
        assert len(dataset.nodes(freq)) == num_nodes, (
            f"{freq} doesn't match {len(dataset.nodes(freq))} vs {num_nodes}")


def test_populate_items(dataset):
    source_files = {}
    for fg_name, formats in dataset.expected_formats.items():
        for format, files in formats:
            source_name = fg_name + format.name
            dataset.add_source(source_name, fg_name, format)
            source_files[source_name] = set(files)
    for node in dataset.nodes(td.abcd):
        for source_name, files in source_files.items():
            item = node[source_name]
            item.get()
            assert set(os.path.basename(f) for f in item.file_paths) == files


# -----------------------
# Test dataset structures
# -----------------------


TEST_DATASETS = {
    'full' : (  # dataset name
        [td.a, td.b, td.c, td.d],  # layers
        [2, 3, 4, 5],  # size of layers a-d respectively
        ['file1.txt', 'file2.nii.gz', 'dir1'],  # files present at bottom layer
        {},  # id_inference dict
        {'file1': [
            (text, ['file1.txt'])],
         'file2': [
            (nifti_gz, ['file2.nii.gz'])],
         'dir1': [
            (directory, ['dir1'])]},
    ),
    'one_layer': (
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
    ),
    'skip_single': (
        [td.a, td.bc, td.d],
        [2, 1, 2, 3],
        ['doubledir1', 'doubledir2'],
        {},
        {'doubledir1': [
            (directory, ['doubledir1'])],
         'doubledir2': [
            (directory, ['doubledir2'])]}
    ),
    'skip_with_inference': (
        [td.bc, td.ad],
        [2, 3, 2, 4],
        ['file1.img', 'file1.hdr', 'file2.mif'],
        {td.bc: r'b(?P<b>\d+)c(?P<c>\d+)',
         td.ad: r'a(?P<a>\d+)d(?P<d>\d+)'},
        {'file1': [
            (analyze, ['file1.hdr', 'file1.img'])],
         'file2': [
            (mrtrix_image, ['file2.mif'])]},
    ),
    'redundant': (
        [td.abc, td.abcd],  # e.g. XNAT where session ID is unique in project but final layer is organised by timepoint
        [3, 4, 5, 6],
        ['doubledir', 'file1.x', 'file1.y', 'file1.z'],
        {td.abc: r'a(?P<a>\d+)b(?P<b>\d+)c(?P<c>\d+)',
         td.abcd: r'a\d+b\d+c\d+d(?P<d>\d+)'},
        {'doubledir': [
            (directory, ['doubledir'])],
         'file1': [
            (dummy_format, ['file1.x', 'file1.y', 'file1.z'])]})}


GOOD_DATASETS = ['full', 'one_layer', 'skip_single', 'skip_with_inference',
                 'redundant']

# ------------------------------------
# Pytest fixtures and helper functions
# ------------------------------------


@pytest.fixture(params=GOOD_DATASETS)
def dataset(work_dir, request):
    dataset_name = request.param
    create_dataset_in_repo(dataset_name, work_dir)
    return access_dataset(dataset_name, work_dir)


def create_dataset_in_repo(name, base_dir):
    "Creates a dataset from parameters in TEST_DATASETS"

    hierarchy, dim_lengths, files, _, _ = TEST_DATASETS[name]

    dataset_path = get_dataset_path(name, base_dir)
    os.mkdir(dataset_path)

    for id_tple in product(*(list(range(d)) for d in dim_lengths)):
        ids = dict(zip(TestDimension.basis(), id_tple))
        dpath = dataset_path
        for layer in hierarchy:
            dname = ''.join(f'{b}{ids[b]}' for b in layer.nonzero_basis())
            dpath = os.path.join(dpath, dname)
        os.makedirs(dpath)
        for fname in files:
            fpath = os.path.join(dpath, fname)
            # Make double
            if fname.startswith('doubledir'):
                os.mkdir(fpath)
                fname = 'dir'
                fpath = os.path.join(fpath, fname)
            if fname.startswith('dir'):
                os.mkdir(fpath)
                fname = 'test.txt'
                fpath = os.path.join(fpath, fname)
            with open(fpath, 'w') as f:
                f.write(f'test {fname}')


def access_dataset(name, base_dir):
    (hierarchy, dim_lengths, files,
     id_inference, expected_formats) = TEST_DATASETS[name]
    dataset = FileSystem().dataset(
        get_dataset_path(name, base_dir),
        hierarchy=hierarchy,
        id_inference=id_inference)
    dataset.dim_lengths = dim_lengths
    dataset.files = files
    dataset.expected_formats = expected_formats
    return dataset


def get_dataset_path(name, base_dir):
    return os.path.join(base_dir, name)
