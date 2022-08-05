import os
import shutil
import typing as ty
from itertools import product
import zipfile
from pathlib import Path
from dataclasses import dataclass, field as dataclass_field
from arcana.core.utils import set_cwd, path2varname
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

    # Leaf rows
    abcd = 0b1111


class Xyz(WithSideCars):

    ext = "x"
    side_car_exts = ("y", "z")


# -----------------------
# Test dataset structures
# -----------------------


@dataclass
class TestDatasetBlueprint:

    hierarchy: ty.List[DataSpace]
    dim_lengths: ty.List[int]  # size of layers a-d respectively
    files: ty.List[str]  # files present at bottom layer
    id_inference: ty.List[ty.Tuple[DataSpace, str]] = dataclass_field(
        default_factory=list
    )  # id_inference dict
    expected_formats: ty.Dict[str, ty.Tuple[type, ty.List[str]]] = dataclass_field(
        default_factory=dict
    )  # expected formats
    derivatives: ty.List[
        ty.Tuple[str, DataSpace, type, ty.List[str]]
    ] = dataclass_field(
        default_factory=list
    )  # files to insert as derivatives

    @property
    def space(self):
        return type(self.hierarchy[0])


def make_dataset(
    blueprint: TestDatasetBlueprint, dataset_path: Path, source_data: Path = None
):
    create_dataset_data_in_repo(blueprint, dataset_path, source_data=source_data)
    return access_dataset(blueprint, dataset_path)


def create_dataset_data_in_repo(
    blueprint: TestDatasetBlueprint, dataset_path: Path, source_data: Path = None
):
    "Creates a dataset from parameters in TEST_DATASETS"
    dataset_path.mkdir(exist_ok=True, parents=True)
    for id_tple in product(*(list(range(d)) for d in blueprint.dim_lengths)):
        ids = dict(zip(blueprint.space.axes(), id_tple))
        dpath = dataset_path
        for layer in blueprint.hierarchy:
            dpath /= "".join(f"{b}{ids[b]}" for b in layer.span())
        os.makedirs(dpath)
        for fname in blueprint.files:
            if source_data is not None:
                src_path = source_data.joinpath(*fname.split("/"))
                parts = fname.split(".")
                dst_path = dpath / (path2varname(parts[0]) + "." + ".".join(parts[1:]))
                dst_path.parent.mkdir(exist_ok=True)
                if src_path.is_dir():
                    shutil.copytree(src_path, dst_path)
                else:
                    shutil.copyfile(src_path, dst_path, follow_symlinks=True)
            else:
                create_test_file(fname, dpath)


def access_dataset(blueprint, dataset_path):
    space = type(blueprint.hierarchy[0])
    dataset = FileSystem().new_dataset(
        dataset_path,
        space=space,
        hierarchy=blueprint.hierarchy,
        id_inference=blueprint.id_inference,
    )
    dataset.__annotations__["blueprint"] = blueprint
    return dataset


def create_test_file(fname, dpath):
    dpath = Path(dpath)
    os.makedirs(dpath, exist_ok=True)
    next_part = fname
    if next_part.endswith(".zip"):
        next_part = next_part.strip(".zip")
    fpath = Path(next_part)
    # Make double dir
    if next_part.startswith("doubledir"):
        os.makedirs(dpath / fpath, exist_ok=True)
        next_part = "dir"
        fpath /= next_part
    if next_part.startswith("dir"):
        os.makedirs(dpath / fpath, exist_ok=True)
        next_part = "test.txt"
        fpath /= next_part
    if not fpath.suffix:
        fpath = fpath.with_suffix(".txt")
    with open(dpath / fpath, "w") as f:
        f.write(f"{fname}")
    if fname.endswith(".zip"):
        with zipfile.ZipFile(dpath / fname, mode="w") as zfile, set_cwd(dpath):
            zfile.write(fpath)
        (dpath / fpath).unlink()
        fpath = Path(fname)
    return fpath


def save_dataset(work_dir, name=None):
    blueprint = TestDatasetBlueprint(
        [
            TestDataSpace.abcd
        ],  # e.g. XNAT where session ID is unique in project but final layer is organised by timepoint
        [1, 1, 1, 1],
        ["file1.txt", "file2.txt"],
        {},
        {},
        [],
    )

    dataset_path = work_dir / "saved_dataset"
    dataset = make_dataset(blueprint, dataset_path)
    dataset.save(name)
    return dataset
