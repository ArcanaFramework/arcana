import os
import os.path
import operator as op
from itertools import product
from functools import reduce
from copy import copy
import pytest
from arcana2.repositories.file_system import FileSystem
from arcana2.core.data.enum import DataDimension


class TestDimension(DataDimension):

    _ = 0b0000

    a = 0b1000
    b = 0b0100
    c = 0b0010
    d = 0b0001

    ab = 0b1100
    ac = 0b1010
    ad = 0b1001
    bc = 0b0110
    bd = 0b0101
    cd = 0b0011

    abc = 0b1110
    abd = 0b1101
    acd = 0b1011
    bcd = 0b0111

    abcd = 0b1111


td = TestDimension


TEST_SETS = [
    (
        'full',  # dataset name
        [td.a, td.b, td.c, td.d],  # layers
        [2, 3, 4, 5],  # size of layers a-d respectively
        ['file1.txt', 'file2.nii.gz', 'dir1'],  # files present at bottom layer
        {}  # id_inference dict
    ), (
        'one_layer',
        [td.abcd],
        [1, 1, 1, 5],
        ['file1.nii.gz', 'file1.json', 'file2.nii', 'file2.json'],
        {}
    ), (
        'skip_single',
        [td.a, td.bc, td.d],
        [2, 1, 2, 3],
        ['doubledir1', 'doubledir2'],
        {}
    ), (
        'skip_with_inference',
        [td.bc, td.ad],
        [2, 3, 2, 4],
        ['file1.img', 'file1.hdr', 'file2.hdr'],
        {td.bc: r'b(?P<b>\d+)c\d+',
         td.ad: r'a(?P<a>\d+)d\d+'}
    ), (
        'redundant',
        [td.abc, td.abcd],  # e.g. XNAT where session ID is unique in project but final layer is organised by timepoint
        [3, 4, 5, 6],
        ['doubledir', 'file1.x', 'file1.y', 'file1.z'],
        {td.abc: r'a(?P<a>\d+)b(?P<b>\d+)c(?P<c>\d+)',
         td.abcd: r'a\d+b\d+c\d+d(?P<d>\d+)'})]


@pytest.mark.parametrize('dataset_args', TEST_SETS)
def test_construct_tree(dataset_args, work_dir):
    dataset = _create_dataset(*dataset_args, work_dir=work_dir)
    for freq in TestDimension:
        # For all non-zero bases in the frequency, multiply the dim lengths
        # together to get the combined number of nodes expected for that
        # frequency
        num_nodes = reduce(
            op.mul, (l for l, b in zip(dataset.dim_lengths, freq) if b), 1)
        assert len(dataset.nodes(freq)) == num_nodes, (
            f"{freq} doesn't match {len(dataset.nodes(freq))} vs {num_nodes}")


def _create_dataset(name, hierarchy, dim_lengths, files, id_inference,
                    work_dir):
    "Creates a dataset from parameters in TEST_SETS"

    dataset_path = os.path.join(work_dir, name)
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

    dataset = FileSystem().dataset(dataset_path, hierarchy=hierarchy,
                                   id_inference=id_inference)
    dataset.dim_lengths = dim_lengths
    dataset.files = files
    return dataset
