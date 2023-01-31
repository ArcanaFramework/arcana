import os
import os.path as op
from pathlib import Path
import re
import errno
import logging
import json
import attrs
import yaml
from fasteners import InterProcessLock
from fileformats.core.base import DataType, FileSet, Field
from arcana.core.exceptions import ArcanaMissingDataException, ArcanaUsageError
from arcana.core.data.set import Dataset
from arcana.core.data.cell import DataCell
from arcana.core.data.store import DataStore, TestDatasetBlueprint
from arcana.core.utils.misc import get_home_dir
from arcana.core.data import Samples


logger = logging.getLogger("arcana")


# Matches directory names used for summary rows with dunder beginning and
# end (e.g. '__visit_01__') and hidden directories (i.e. starting with '.' or
# '~')
special_dir_re = re.compile(r"(__.*__$|\..*|~.*)")


@attrs.define
class DirTree(DataStore):
    """
    A Repository class for data stored hierarchically within sub-directories
    of a file-system directory. The depth and which layer in the data tree
    the sub-directories correspond to is defined by the `hierarchy` argument.

    Parameters
    ----------
    base_dir : str
        Path to the base directory of the "store", i.e. datasets are
        arranged by name as sub-directories of the base dir.

    """

    PROV_SUFFIX = ".prov"
    FIELDS_FNAME = "__fields__.json"
    FIELDS_PROV_FNAME = "__fields_prov__.json"
    LOCK_SUFFIX = ".lock"
    VALUE_KEY = "__value__"
    METADATA_DIR = ".arcana"
    SITE_LICENSES_DIR = "site-licenses"

    name: str = "file"  # Name is constant, as there is only ever one store, which covers whole FS

    def new_dataset(self, id, *args, **kwargs):
        if not Path(id).exists():
            raise ArcanaUsageError(f"Path to dataset root '{id}'' does not exist")
        return super().new_dataset(id, *args, **kwargs)

    def save_dataset_definition(self, dataset_id, definition, name):
        definition_path = self.definition_save_path(dataset_id, name)
        definition_path.parent.mkdir(exist_ok=True)
        with open(definition_path, "w") as f:
            yaml.dump(definition, f)

    def load_dataset_definition(self, dataset_id, name):
        fpath = self.definition_save_path(dataset_id, name)
        if fpath.exists():
            with open(fpath) as f:
                definition = yaml.load(f, Loader=yaml.Loader)
        else:
            definition = None
        return definition

    def definition_save_path(self, dataset_id, name):
        return Path(dataset_id) / self.METADATA_DIR / (name + ".yaml")

    def find_rows(self, dataset: Dataset):
        """
        Find all rows within the dataset stored in the store and
        construct the data tree within the dataset

        Parameters
        ----------
        dataset : Dataset
            The dataset to construct the tree dimensions for
        """
        if not os.path.exists(dataset.id):
            raise ArcanaUsageError(
                f"Could not find a directory at '{dataset.id}' to be the "
                "root row of the dataset"
            )

        for dpath, _, _ in os.walk(dataset.id):
            tree_path = Path(dpath).relative_to(dataset.id).parts
            if len(tree_path) != len(dataset.hierarchy):
                continue
            if special_dir_re.match(tree_path[-1]):
                continue
            dataset.add_leaf(tree_path)

    def find_cells(self, row):
        # First ID can be omitted
        self.find_cells_from_dir(self.root_dir(row) / self.row_path(row), row)

    def get(self, cell: DataCell):
        if cell.datatype.is_fileset:
            item = self.get_fileset_path(cell, ext=cell.datatype.ext)
        elif cell.datatype.is_field:
            item = self.read_from_json(self.get_fields_path(cell), cell.id)
        else:
            raise RuntimeError(
                f"Don't know how to retrieve {cell.datatype} data from {type(self)} stores"
            )
        return item

    def put(self, cell: DataCell, item: DataType, provenance: dict):
        if cell.datatype.is_fileset:
            self._put_fileset(cell, item)
        elif cell.datatype.is_field:
            self._put_field(cell, item)
        else:
            raise RuntimeError(
                f"Don't know how to store {cell.datatype} data in {type(self)} stores"
            )

    def _put_fileset(self, cell: DataCell, fileset: FileSet, provenance: dict = None):
        """
        Inserts or updates a fileset in the store
        """
        fileset_path = self.get_fileset_path(cell)
        # Create target directory if it doesn't exist already
        copied_fileset = fileset.copy_to(
            dest_dir=fileset_path.parent, stem=fileset_path.name, make_dirs=True
        )
        if provenance:
            with open(self.get_fileset_prov_path(cell), "w") as f:
                json.dump(cell.provenance, f)
        return copied_fileset

    def _put_field(self, cell: DataCell, value, provenance: dict = None):
        """
        Inserts or updates a field in the store
        """
        self.update_json(self.get_fields_path(cell), cell.id, value)
        if provenance:
            self.update_json(self.get_fields_prov_path(cell), cell.id, provenance)

    def find_cells_from_dir(self, dpath, row):
        if not op.exists(dpath):
            return
        # Filter contents of directory to omit fields JSON and provenance
        filtered = []
        for subpath in dpath.iterdir():
            if not (
                subpath.name.startswith(".")
                or subpath.name == self.FIELDS_FNAME
                or subpath.name.endswith(self.PROV_SUFFIX)
            ):
                filtered.append(subpath.name)
        # Add data cells corresponding to files. We add a new cell for each possible
        # extension (including no extension) to handle cases where "." periods are used
        # as part of the filename
        file_stems = set()
        for fname in filtered:
            fname_parts = fname.split(".")
            for i in range(len(fname_parts)):
                file_stems.add(".".join(fname_parts[: (i + 1)]))

        for path in file_stems:
            prov_path = dpath / (path + self.PROV_SUFFIX)
            if prov_path.exists():
                with open(prov_path) as f:
                    provenance = json.load(f)
            else:
                provenance = {}
            row.add_cell(
                path=path,
                provenance=provenance,
                datatype=FileSet,
            )
        # Add fields
        try:
            with open(op.join(dpath, self.FIELDS_PROV_FNAME)) as f:
                fields_prov_dict = json.load(f)
        except FileNotFoundError:
            fields_prov_dict = {}
        try:
            with open(op.join(dpath, self.FIELDS_FNAME)) as f:
                fields_dict = json.load(f)
        except FileNotFoundError:
            pass
        else:
            for name in fields_dict:
                row.add_cell(
                    name_path=name,
                    provenance=fields_prov_dict.get(name),
                    datatype=Field,
                )

    def row_path(self, row):
        path = Path()
        accounted_freq = row.dataset.space(0)
        for layer in row.dataset.hierarchy:
            if not (layer.is_parent(row.frequency) or layer == row.frequency):
                break
            path /= row.ids[layer]
            accounted_freq |= layer
        # If not "leaf row" then
        if row.frequency != max(row.dataset.space):
            unaccounted_freq = (row.frequency ^ accounted_freq) & row.frequency
            unaccounted_id = row.ids[unaccounted_freq]
            if unaccounted_id is None:
                path /= f"__{unaccounted_freq}__"
            elif isinstance(unaccounted_id, str):
                path /= f"__{unaccounted_freq}_{unaccounted_id}__"
            else:
                path /= f"__{unaccounted_freq}_" + "_".join(unaccounted_id) + "__"
        return path

    def root_dir(self, row) -> Path:
        return Path(row.dataset.id)

    @classmethod
    def absolute_row_path(cls, row) -> Path:
        return cls().root_dir(row) / cls().row_path(row)

    def get_fileset_path(self, cell: DataCell, ext=None):
        """The path to the stem of the paths (i.e. the path without
        file extension) where the files are saved in the file-system.
        NB: this method is overridden in Bids store.

        Parameters
        ----------
        fileset: FileSet
            the file set stored or to be stored
        """
        row_path = self.absolute_row_path(cell.row)
        fileset_path = row_path.joinpath(*cell.path.split("/"))
        if ext:
            fileset_path += ext
        return fileset_path

    def get_fields_path(self, field):
        return self.root_dir(field.row) / self.row_path(field.row) / self.FIELDS_FNAME

    def get_fields_prov_path(self, field):
        return (
            self.root_dir(field.row) / self.row_path(field.row) / self.FIELDS_PROV_FNAME
        )

    def get_fileset_prov_path(self, fileset):
        return self.get_fileset_path(fileset) + self.PROV_SUFFX

    def site_licenses_dataset(self):
        """Provide a place to store hold site-wide licenses"""
        dataset_root = get_home_dir() / self.SITE_LICENSES_DIR
        if not dataset_root.exists():
            dataset_root.mkdir(parents=True)
        try:
            dataset = self.load_dataset(dataset_root)
        except KeyError:
            dataset = self.new_dataset(dataset_root, space=Samples)
        return dataset

    def update_json(self, fpath: Path, key, value):
        """Updates a JSON file in a multi-process safe way"""
        # Open fields JSON, locking to prevent other processes
        # reading or writing
        with InterProcessLock(fpath + self.LOCK_SUFFIX, logger=logger):
            try:
                with open(fpath) as f:
                    dct = json.load(f)
            except IOError as e:
                if e.errno == errno.ENOENT:
                    dct = {}
                else:
                    raise
            dct[key] = value
            with open(fpath, "w") as f:
                json.dump(dct, f, indent=4)

    def read_from_json(self, fpath, key):
        """
        Load fields JSON, locking to prevent read/write conflicts
        Would be better if only checked if locked to allow
        concurrent reads but not possible with multi-process
        locks (in my understanding at least).
        """
        try:
            with InterProcessLock(fpath + self.LOCK_SUFFIX, logger=logger), open(
                fpath, "r"
            ) as f:
                dct = json.load(f)
            return dct[key]
        except (KeyError, IOError) as e:
            try:
                # Check to see if the IOError wasn't just because of a
                # missing file
                if e.errno != errno.ENOENT:
                    raise
            except AttributeError:
                pass
            raise ArcanaMissingDataException(
                "{} does not exist in the local store {}".format(key, self)
            )

    def create_test_dataset_data(
        self, blueprint: TestDatasetBlueprint, dataset_id: str, source_data: Path = None
    ):
        """Creates the actual data in the store, from the provided blueprint, which
        can be used to run test routines against

        Parameters
        ----------
        blueprint
            the test dataset blueprint
        dataset_path : Path
            the pat
        """
        dataset_path = Path(dataset_id)
        dataset_path.mkdir()
        for ids in self.iter_test_blueprint(blueprint):
            dpath = dataset_path.joinpath(*[ids[h] for h in blueprint.hierarchy])
            dpath.mkdir(parents=True)
            for fname in blueprint.files:
                self.create_test_data_item(fname, dpath, source_data=source_data)
