import os.path
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
        [2, 2, 2, 2],
        ['file1.nii.gz', 'file1.json', 'file2.nii', 'file2.json'],
        {}
    ), (
        'skip',
        [td.a, td.bc, td.d],
        [2, 2, 2, 2],
        ['doubledir1', 'doubledir2'],
        {}
    ), (
        'skip_with_inference',
        [td.bc, td.ad],
        [2, 2, 2, 2],
        ['file1.img', 'file1.hdr', 'file2.hdr'],
        {td.bcd: r'b\d+c(?P<c>\d+)d\d+'}
    ),
    # (
    #     'redundant',
    #     [td.abc, td.abcd],
    #     [2, 2, 2, 2],
    #     ['doubledir', 'file1.x', 'file1.y', 'file1.z'],
    #     {}
    # ),
    (
        'resolved_redundant',
        [td.abc, td.abcd],  # e.g. XNAT where session ID is unique in project but final layer is organised by timepoint
        [2, 2],
        ['doubledir', 'file1.x', 'file1.y', 'file1.z'],
        {td.abcd: r'a(?P<a>\d+)b\d+c\d+(?P<d>\d+)'})]


@pytest.mark.parametrize('dataset_args', TEST_SETS)
def test_construct_tree(dataset_args, work_dir):
    dataset = _create_dataset(*dataset_args, work_dir=work_dir)
    for i, layer in enumerate(dataset.hierarchy):
        assert len(dataset.nodes(layer)) == dataset.dim_lengths[i], (
            f"{layer} doesn't match {len(dataset.nodes(layer))} vs "
            f"{dataset.dim_lengths[i]}")


def _create_dataset(name, hierarchy, dim_lengths, files, id_inference,
                    work_dir):
    "Creates a dataset from parameters in TEST_SETS"

    def create_layer_dirs(layer_path, layer_stack, ids=None):
        ids = copy(ids) if ids else {}
        "Recursive creation of layer structure"
        if layer_stack:  # non-leaf node
            layer, dim_length = layer_stack[0]
            for i in range(dim_length):
                ids[str(layer)[-1]] = i + 1
                dname = ''
                for c in str(layer):
                    dname += c
                    try:
                        dname += str(ids[c])
                    except KeyError:
                        pass
                dpath = os.path.join(layer_path, dname)
                os.mkdir(dpath)
                create_layer_dirs(dpath, layer_stack[1:], ids)
        else:  # leaf node
            for fname in files:
                fpath = os.path.join(layer_path, fname)
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

    dataset_path = os.path.join(work_dir, name)
    # Create test directory according to parameters
    os.mkdir(dataset_path)
    create_layer_dirs(dataset_path, list(zip(hierarchy, dim_lengths)))

    dataset = FileSystem().dataset(dataset_path, hierarchy=hierarchy,
                                   id_inference=id_inference)
    dataset.dim_lengths = dim_lengths
    dataset.files = files
    return dataset
