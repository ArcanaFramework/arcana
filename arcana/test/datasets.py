import os
import typing as ty
from itertools import product
import zipfile
from pathlib import Path
from dataclasses import dataclass
from arcana.core.utils import set_cwd
from arcana.core.data.space import DataSpace
from arcana.core.data.format import WithSideCars
from arcana.data.stores.common import FileSystem


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


class Xyz(WithSideCars):

    ext = 'x'
    side_car_exts = ('y', 'z')


# -----------------------
# Test dataset structures
# -----------------------

@dataclass
class TestDatasetBlueprint():

    hierarchy: ty.List[DataSpace]
    dim_lengths: ty.List[int]  # size of layers a-d respectively
    files: ty.List[str]  # files present at bottom layer
    id_inference: ty.Dict[DataSpace, str]  # id_inference dict
    expected_formats: ty.Dict[str, ty.Tuple[type, ty.List[str]]]  # expected formats
    to_insert: ty.List[ty.Tuple[str, ty.Tuple[DataSpace, type, ty.List[str]]]]  # files to insert as derivatives



def make_dataset(blueprint, dataset_path):
    create_dataset_data_in_repo(blueprint, dataset_path)
    return access_dataset(blueprint, dataset_path)


def create_dataset_data_in_repo(blueprint, dataset_path):
    "Creates a dataset from parameters in TEST_DATASETS"
    dataset_path.mkdir(exist_ok=True, parents=True)
    for id_tple in product(*(list(range(d)) for d in blueprint.dim_lengths)):
        ids = dict(zip(TestDataSpace.axes(), id_tple))
        dpath = dataset_path
        for layer in blueprint.hierarchy:
            dpath /= ''.join(f'{b}{ids[b]}' for b in layer.span())
        os.makedirs(dpath)
        for fname in blueprint.files:
            create_test_file(fname, dpath)


def access_dataset(blueprint, dataset_path):
    space = type(blueprint.hierarchy[0])
    dataset = FileSystem().new_dataset(
        dataset_path,
        space=space,
        hierarchy=blueprint.hierarchy,
        id_inference=blueprint.id_inference)
    dataset.blueprint = blueprint
    return dataset


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
