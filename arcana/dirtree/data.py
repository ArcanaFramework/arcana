from __future__ import annotations
import os
from pathlib import Path
import re
import typing as ty
import logging
import json
import attrs
from fileformats.core.base import FileSet, Field
from arcana.core.exceptions import ArcanaUsageError
from arcana.core.data.set.base import DataTree
from arcana.core.data.row import DataRow
from arcana.core.data.entry import DataEntry
from arcana.core.data.store import LocalStore
from arcana.core.data.testing import TestDatasetBlueprint


logger = logging.getLogger("arcana")


# Matches directory names used for summary rows with dunder beginning and
# end (e.g. '__visit_01__') and hidden directories (i.e. starting with '.' or
# '~')
special_dir_re = re.compile(r"(__.*__$|\..*|~.*)")


@attrs.define
class DirTree(LocalStore):
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

    PROV_SUFFIX = ".provenance"
    FIELDS_FNAME = "__fields__"
    FIELDS_PROV_FNAME = "__fields_provenance__"

    # Note this name will be constant, as there is only ever one store,
    # which covers whole FS
    name: str = "dirtree"

    #################################
    # Abstract-method implementations
    #################################

    def scan_tree(self, tree: DataTree):
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

    def scan_row(self, row: DataRow):
        """Scans a data row and populates it with entries

        Parameters
        ----------
        row : DataRow
            the data row to populate
        """
        row_dir = Path(row.dataset.id) / self._row_relpath(row)
        if not row_dir.exists():
            return
        # Filter contents of directory to omit fields JSON and provenance
        for entry_path in row_dir.iterdir():
            if (
                not entry_path.name.startswith(".")
                and entry_path.name not in (self.FIELDS_FNAME, self.FIELDS_PROV_FNAME)
                and not entry_path.name.endswith(self.PROV_SUFFIX)
            ):
                row.add_entry(
                    path=str(entry_path.relative_to(row_dir)),
                    datatype=FileSet,
                    uri=str(entry_path),
                )
        # Add fields
        try:
            with open(row_dir / self.FIELDS_FNAME) as f:
                fields_dict = json.load(f)
        except FileNotFoundError:
            pass
        else:
            for name in fields_dict:
                row.add_entry(path=name, datatype=Field, uri=None)

    def get_field(self, entry: DataEntry, datatype: type) -> Field:
        fspath, key = self._fields_fspath_and_key(entry)
        return datatype(self.read_from_json(fspath, key))

    def get_fileset(self, entry: DataEntry, datatype: type) -> FileSet:
        return datatype(self._fileset_fspath(entry))

    def put_fileset(self, fileset: FileSet, entry: DataEntry) -> FileSet:
        """
        Inserts or updates a fileset in the store
        """
        fspath = self._fileset_fspath(entry)
        # Create target directory if it doesn't exist already
        copied_fileset = fileset.copy_to(
            dest_dir=fspath.parent,
            stem=fspath.name[: -len(fileset.ext)] if fileset.ext else fspath.name,
            make_dirs=True,
        )
        return copied_fileset

    def put_field(self, field: Field, entry: DataEntry):
        """
        Inserts or updates a field in the store
        """
        fspath, key = self._fields_fspath_and_key(entry)
        self.update_json(fspath, key, field.raw_type(field))

    def get_fileset_provenance(self, entry: DataEntry) -> dict[str, ty.Any]:
        with open(self._fileset_prov_fspath(entry)) as f:
            provenance = json.load(f)
        return provenance

    def put_fileset_provenance(self, provenance: dict[str, ty.Any], entry: DataEntry):
        with open(self._fileset_prov_fspath(entry), "w") as f:
            json.dump(provenance, f)

    def get_field_provenance(self, entry: DataEntry) -> dict[str, ty.Any]:
        fspath, key = self._fields_prov_fspath_and_key(entry)
        with open(fspath) as f:
            fields_provenance = json.load(f)
        return fields_provenance[key]

    def put_field_provenance(self, provenance: dict[str, ty.Any], entry: DataEntry):
        fspath, key = self._fields_prov_fspath_and_key(entry)
        self.update_json(fspath, key, provenance)

    def fileset_uri(self, path: str, datatype: type, row: DataRow) -> str:
        """The path to the stem of the paths (i.e. the path without
        file extension) where the files are saved in the file-system.
        NB: this method is overridden in Bids store.

        Parameters
        ----------
        fileset: FileSet
            the file set stored or to be stored
        """
        path = path.lstrip("@")  # We don't put derivatives anywhere special
        return str(self._row_relpath(row).joinpath(*path.split("/"))) + datatype.ext

    def field_uri(self, path: str, datatype: type, row: DataRow) -> str:
        return str(self._row_relpath(row) / self.FIELDS_FNAME) + "::" + path

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

    def create_empty_dataset(
        self,
        id: str,
        hierarchy: list[str],
        row_ids: list[list[str]],
        space: type,
        name: str = None,
        **kwargs,
    ):
        raise NotImplementedError

    ##################
    # Helper functions
    ##################

    def _row_relpath(self, row):
        """Get the file-system path to the dataset root for the given row, taking into
        account non-leaf rows

        Parameters
        ----------
        row : DataRow
            the row to get the relative path for

        Returns
        -------
        relpath : Path
            the relative path to the row directory
        """
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

    def _fileset_fspath(self, entry):
        return Path(entry.row.dataset.id) / entry.uri

    def _fields_fspath_and_key(self, entry):
        relpath, key = entry.uri.split("::")
        fspath = Path(entry.row.dataset.id) / relpath
        return fspath, key

    def _fileset_prov_fspath(self, entry):
        return self._fileset_fspath(entry).with_suffix(self.PROV_SUFFIX)

    def _fields_prov_fspath_and_key(self, entry):
        fields_fspath, key = self._fields_fspath_and_key(entry)
        return fields_fspath.parent / self.FIELDS_PROV_FNAME, key
