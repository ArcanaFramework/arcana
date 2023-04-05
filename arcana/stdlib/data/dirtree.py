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
from arcana.core.utils.misc import full_path


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

    def populate_tree(self, tree: DataTree):
        """
        Scans the data present in the dataset and populates the data tree with nodes

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
        dpaths = sorted(d for d, _, _ in os.walk(tree.dataset_id))
        for dpath in dpaths:
            tree_path = tuple(Path(dpath).relative_to(tree.dataset_id).parts)
            if len(tree_path) != len(tree.hierarchy):
                continue
            if self.ARCANA_DIR in tree_path:
                continue
            tree.add_leaf(tree_path)

    def populate_row(self, row: DataRow):
        """Scans the node in the data tree corresponding to the data row and populates
        the row with data entries found in the tree node

        Parameters
        ----------
        row : DataRow
            the data row to populate
        """

        def filter_entry_dir(entry_dir):
            for subpath in entry_dir.iterdir():
                entry_name = subpath.name
                if (
                    not entry_name.startswith(".")
                    and entry_name != self.ARCANA_DIR
                    and entry_name
                    not in (
                        self.FIELDS_FNAME,
                        self.FIELDS_PROV_FNAME,
                        self.FIELDS_FNAME + self.LOCK_SUFFIX,
                    )
                    and not entry_name.endswith(self.PROV_SUFFIX)
                ):
                    yield subpath

        root_dir = full_path(row.dataset.id)

        # Iterate through all directories saved for the source and dataset derivatives
        for dataset_name in self._row_dataset_names(row):
            row_dir = root_dir / self._row_relpath(row, dataset_name=dataset_name)
            if row_dir.exists():
                # Filter contents of directory to omit fields JSON and provenance and
                # add file-set entries
                for entry_fspath in filter_entry_dir(row_dir):
                    path = str(entry_fspath.relative_to(row_dir))
                    if dataset_name is not None:
                        path += "@" + dataset_name
                    row.add_entry(
                        path=path,
                        datatype=FileSet,
                        uri=str(entry_fspath.relative_to(root_dir)),
                    )
                # Add field entries
                fields_json = row_dir / self.FIELDS_FNAME
                try:
                    with open(fields_json) as f:
                        fields_dict = json.load(f)
                except FileNotFoundError:
                    pass
                else:
                    for name in fields_dict:
                        path = (
                            f"{name}@{dataset_name}"
                            if dataset_name is not None
                            else name
                        )
                        row.add_entry(
                            path=path,
                            datatype=Field,
                            uri=str(fields_json.relative_to(root_dir)) + "::" + name,
                        )

    def get_field(self, entry: DataEntry, datatype: type) -> Field:
        """Retrieve the field associated with the given entry and return it cast
        to the specified datatype

        Parameters
        ----------
        entry : DataEntry
            the entry to retrieve the field for
        datatype : type (subclass DataType)
            the datatype to return the field as

        Returns
        -------
        Field
            the retrieved field
        """
        fspath, key = self._fields_fspath_and_key(entry)
        return datatype(self.read_from_json(fspath, key))

    def get_fileset(self, entry: DataEntry, datatype: type) -> FileSet:
        """Retrieve the file-set associated with the given entry and return it cast
        to the specified datatype

        Parameters
        ----------
        entry : DataEntry
            the entry to retrieve the file-set for
        datatype : type (subclass DataType)
            the datatype to return the file-set as

        Returns
        -------
        FileSet
            the retrieved file-set
        """
        return datatype(self._fileset_fspath(entry))

    def put_fileset(self, fileset: FileSet, entry: DataEntry) -> FileSet:
        """Put a file-set into the specified data entry

        Parameters
        ----------
        fileset : FileSet
            the file-set to store
        entry : DataEntry
            the entry to store the file-set in

        Returns
        -------
        FileSet
            the copy of the file-set that has been stored within the data entry
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

        """Put a field into the specified data entry

        Parameters
        ----------
        field : Field
            the field to store
        entry : DataEntry
            the entry to store the field in
        """
        fspath, key = self._fields_fspath_and_key(entry)
        self.update_json(fspath, key, field.raw_type(field))

    def get_fileset_provenance(
        self, entry: DataEntry
    ) -> ty.Union[dict[str, ty.Any], None]:
        """Retrieves provenance associated with a file-set data entry

        Parameters
        ----------
        entry : DataEntry
            the entry of the file-set to retrieve the provenance for

        Returns
        -------
        dict[str, ty.Any] or None
            the retrieved provenance or None if it doesn't exist
        """
        with open(self._fileset_prov_fspath(entry)) as f:
            provenance = json.load(f)
        return provenance

    def put_fileset_provenance(self, provenance: dict[str, ty.Any], entry: DataEntry):
        """Puts provenance associated with a file-set data entry into the store

        Parameters
        ----------
        provenance : dict[str, ty.Any]
            the provenance to store
        entry : DataEntry
            the entry to associate the provenance with
        """
        with open(self._fileset_prov_fspath(entry), "w") as f:
            json.dump(provenance, f)

    def get_field_provenance(
        self, entry: DataEntry
    ) -> ty.Union[dict[str, ty.Any], None]:
        """Retrieves provenance associated with a field data entry

        Parameters
        ----------
        entry : DataEntry
            the entry of the field to retrieve the provenance for

        Returns
        -------
        dict[str, ty.Any] or None
            the retrieved provenance or None if it doesn't exist
        """
        fspath, key = self._fields_prov_fspath_and_key(entry)
        with open(fspath) as f:
            fields_provenance = json.load(f)
        return fields_provenance[key]

    def put_field_provenance(self, provenance: dict[str, ty.Any], entry: DataEntry):
        """Puts provenance associated with a field data entry into the store

        Parameters
        ----------
        provenance : dict[str, ty.Any]
            the provenance to store
        entry : DataEntry
            the entry to associate the provenance with
        """
        fspath, key = self._fields_prov_fspath_and_key(entry)
        self.update_json(fspath, key, provenance)

    def fileset_uri(self, path: str, datatype: type, row: DataRow) -> str:
        """Returns the "uri" (e.g. file-system path relative to root dir) of a file-set
        entry at the given path relative to the given row

        Parameters
        ----------
        path : str
            path to the entry relative to the row
        datatype : type
            the datatype of the entry
        row : DataRow
            the row of the entry

        Returns
        -------
        uri : str
            the "uri" to the file-set entry relative to the data store
        """
        path, dataset_name = DataEntry.split_dataset_name_from_path(path)
        row_dir = self._row_relpath(row, dataset_name=dataset_name)
        return str(row_dir.joinpath(*path.split("/"))) + datatype.ext

    def field_uri(self, path: str, datatype: type, row: DataRow) -> str:
        """Returns the "uri" (e.g. file-system path relative to root dir) of a field
        entry at the given path relative to the given row

        Parameters
        ----------
        path : str
            path to the entry relative to the row
        datatype : type
            the datatype of the entry
        row : DataRow
            the row of the entry

        Returns
        -------
        uri : str
            the "uri" to the field entry relative to the data store
        """
        path, dataset_name = DataEntry.split_dataset_name_from_path(path)
        row_dir = self._row_relpath(row, dataset_name=dataset_name)
        return str(row_dir / self.FIELDS_FNAME) + "::" + path

    def create_data_tree(self, id: str, leaves: list[tuple[str, ...]], **kwargs):
        """creates a new empty dataset within in the store. Used in test routines and
        importing/exporting datasets between stores

        Parameters
        ----------
        id : str
            ID for the newly created dataset
        leaves : list[tuple[str, ...]]
                        list of IDs for each leaf node to be added to the dataset. The IDs for each
            leaf should be a tuple with an ID for each level in the tree's hierarchy, e.g.
            for a hierarchy of [subject, timepoint] ->
            [("SUBJ01", "TIMEPOINT01"), ("SUBJ01", "TIMEPOINT02"), ....]
        hierarchy: list[str]
            the hierarchy of the dataset to be created
        space : type(DataSpace)
            the data space of the dataset
        """
        root_dir = Path(id)
        root_dir.mkdir(parents=True)
        # Create sub-directories corresponding to rows of the dataset
        for ids_tuple in leaves:
            root_dir.joinpath(*ids_tuple).mkdir(parents=True)

    ##################
    # Helper functions
    ##################

    def _row_relpath(self, row: DataRow, dataset_name=None):
        """Get the file-system path to the dataset root for the given row, taking into
        account non-leaf rows

        Parameters
        ----------
        row : DataRow
            the row to get the relative path for
        derivatives : bool
            whether to return the directory containing derivatives or originals

        Returns
        -------
        relpath : Path
            the relative path to the row directory
        """
        relpath = Path()
        if row.frequency is max(row.dataset.space):  # leaf node
            for freq in row.dataset.hierarchy:
                relpath /= row.frequency_id(freq)
            if dataset_name is not None:
                relpath /= self.ARCANA_DIR
                if dataset_name:
                    relpath /= dataset_name
                else:
                    relpath /= self.EMPTY_DATASET_NAME
        else:
            relpath = relpath.joinpath(
                self.ARCANA_DIR,
                str(row.frequency),
            )
            if isinstance(row.id, tuple):
                relpath /= ".".join(row.id)
            elif row.id:
                relpath /= row.id
            if dataset_name is None:
                relpath /= self.ARCANA_DIR
            elif not dataset_name:
                relpath /= self.EMPTY_DATASET_NAME
            else:
                relpath /= dataset_name
        return relpath

    def _row_dataset_names(self, row: DataRow):
        """list all dataset names stored in the given row

        Parameters
        ----------
        row : DataRow
            row to return the dataset names for

        Returns
        -------
        dataset_names : list[str]
            list of dataset names stored in the given row
        """
        dataset_names = [None]  # The source data
        derivs_dir = (
            Path(row.dataset.id) / self._row_relpath(row, dataset_name="").parent
        )
        if derivs_dir.exists():
            dataset_names.extend(
                ("" if d.name == self.EMPTY_DATASET_NAME else d.name)
                for d in derivs_dir.iterdir()
                if d.name != self.ARCANA_DIR
            )
        return dataset_names

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
