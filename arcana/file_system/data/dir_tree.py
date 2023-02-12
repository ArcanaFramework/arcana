from __future__ import annotations
import os
import os.path as op
from pathlib import Path
import re
import typing as ty
import errno
import logging
import json
import attrs
import yaml
from fasteners import InterProcessLock
from fileformats.core.base import DataType, FileSet, Field
from arcana.core.exceptions import (
    ArcanaMissingDataException,
    ArcanaUsageError,
    DatatypeUnsupportedByStoreError,
)
from arcana.core.data.set import DataTree
from arcana.core.data.row import DataRow
from arcana.core.data.entry import DataEntry
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

    def populate_tree(self, tree: DataTree):
        """
        Find all rows within the dataset stored in the store and
        populate the data tree within the dataset

        Parameters
        ----------
        dataset : Dataset
            The dataset to construct the tree dimensions for
        """
        if not os.path.exists(tree.dataset_id):
            raise ArcanaUsageError(
                f"Could not find a directory at '{tree.dataset_id}' to be the "
                "root row of the dataset"
            )
        for dpath, _, _ in os.walk(tree.dataset_id):
            tree_path = tuple(Path(dpath).relative_to(tree.dataset_id).parts)
            if len(tree_path) != len(tree.hierarchy):
                continue
            if special_dir_re.match(tree_path[-1]):
                continue
            tree.add_leaf(tree_path)

    def populate_row(self, row):
        # First ID can be omitted
        return self.add_entries_from_dir(self.root_dir(row) / self.row_path(row), row)

    def get(self, entry: DataEntry) -> DataType:
        if entry.datatype.is_fileset:
            item = self.get_fileset(entry)
        elif entry.datatype.is_field:
            item = self.get_field(entry)
        else:
            raise RuntimeError(
                f"Don't know how to retrieve {entry.datatype} data from {type(self)} stores"
            )
        return item

    def put(self, item: DataType, entry: DataEntry):
        if entry.datatype.is_fileset:
            cpy = self.put_fileset(item, entry)
        elif entry.datatype.is_field:
            cpy = self.put_field(item, entry)
        else:
            raise RuntimeError(
                f"Don't know how to store {entry.datatype} data in {type(self)} stores"
            )
        return cpy

    def post(
        self, item: DataType, path: str, datatype: type, row: DataRow
    ) -> DataEntry:
        if datatype.is_fileset:
            entry = self.post_fileset(item, path, datatype, row)
        elif datatype.is_field:
            entry = self.put_field(item, path, datatype, row)
        else:
            raise RuntimeError(
                f"Don't know how to store {datatype} data in {type(self)} stores"
            )
        return entry

    def get_provenance(self, entry: DataEntry) -> dict[str, ty.Any]:
        if entry.datatype.is_fileset:
            with open(self.get_fileset_prov_path(entry)) as f:
                provenance = json.load(f)
        elif entry.datatype.is_field:
            with open(self.get_fields_prov_path(entry)) as f:
                fields_provenance = json.load(f)
            provenance = fields_provenance[entry.path]
        else:
            raise DatatypeUnsupportedByStoreError(entry.datatype, self)
        return provenance

    def put_provenance(self, provenance: dict[str, ty.Any], entry: DataEntry):
        if entry.datatype.is_fileset:
            with open(self.get_fileset_prov_path(entry), "w") as f:
                json.dump(provenance, f)
        elif entry.datatype.is_field:
            self.update_json(self.get_fields_prov_path(entry), entry.path, provenance)
        else:
            raise DatatypeUnsupportedByStoreError(entry.datatype, self)

    def get_field(self, entry: DataEntry) -> Field:
        return entry.datatype(self.read_from_json(*entry.uri.split("@")))

    def get_fileset(self, entry: DataEntry) -> FileSet:
        return entry.datatype(self.get_all_fileset_paths(self.get_fileset_path(entry)))

    def put_fileset(self, fileset: FileSet, entry: DataEntry) -> FileSet:
        """
        Inserts or updates a fileset in the store
        """
        # Create target directory if it doesn't exist already
        copied_fileset = fileset.copy_to(
            dest_dir=entry.uri.parent, stem=entry.uri.name, make_dirs=True
        )
        return copied_fileset

    def put_field(self, field: Field, entry: DataEntry):
        """
        Inserts or updates a field in the store
        """
        self.update_json(self.get_fields_path(entry), entry.path, field)

    def post_fileset(
        self, fileset: FileSet, path: str, datatype: type, row: DataRow
    ) -> DataEntry:
        entry = row.add_entry(path=path, datatype=datatype, uri=None)
        # Need to wait until the entry is created so we can use the get_fileset_path
        # to determine the URI
        entry.uri = self.get_fileset_path(entry)
        self.put(fileset, entry)
        return entry

    def post_field(
        self, field: Field, id: str, datatype: type, row: DataRow
    ) -> DataEntry:
        entry = row.add_entry(
            path=id, datatype=datatype, uri=self.get_fields_path(row) + "@" + id
        )
        self.put(field, entry)
        return entry

    def add_entries_from_dir(self, dpath: Path, row: DataRow):
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

        for stem in file_stems:
            row.add_entry(path=stem, datatype=FileSet, uri=dpath / stem)
        # Add fields
        try:
            with open(op.join(dpath, self.FIELDS_FNAME)) as f:
                fields_dict = json.load(f)
        except FileNotFoundError:
            pass
        else:
            for name in fields_dict:
                row.add_entry(path=name, datatype=Field, uri=None)

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

    def get_fileset_path(self, entry: DataEntry) -> Path:
        """The path to the stem of the paths (i.e. the path without
        file extension) where the files are saved in the file-system.
        NB: this method is overridden in Bids store.

        Parameters
        ----------
        fileset: FileSet
            the file set stored or to be stored
        """
        row_path = self.absolute_row_path(entry.row)
        fileset_path = row_path.joinpath(*entry.path.split("/"))
        return fileset_path

    def get_all_fileset_paths(self, fspath: Path) -> ty.Iterable[Path]:
        return (p for p in fspath.parent.iterdir() if p.name.startswith(fspath.name))

    def get_fields_path(self, row) -> Path:
        return self.root_dir(row) / self.row_path(row) / self.FIELDS_FNAME

    def get_fields_prov_path(self, row) -> Path:
        return self.root_dir(row) / self.row_path(row) / self.FIELDS_PROV_FNAME

    def get_fileset_prov_path(self, entry: DataEntry) -> Path:
        return self.get_fileset_path(entry) + self.PROV_SUFFIX

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
                self.create_test_fsobject(fname, dpath, source_data=source_data)

    def definition_save_path(self, dataset_id, name):
        return Path(dataset_id) / self.METADATA_DIR / (name + ".yaml")

    def new_dataset(self, id, *args, **kwargs):
        if not Path(id).exists():
            raise ArcanaUsageError(f"Path to dataset root '{id}'' does not exist")
        return super().new_dataset(id, *args, **kwargs)
