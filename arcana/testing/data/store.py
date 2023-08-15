from __future__ import annotations
import typing as ty
import json
import shutil
from pathlib import Path
import attrs
import time
import yaml
from fileformats.core import FileSet, Field
from arcana.core.data.store import RemoteStore
from arcana.core.data.row import DataRow
from arcana.core.data.tree import DataTree
from arcana.core.data.entry import DataEntry
from arcana.core.data.space import DataSpace
from arcana.core.utils.misc import full_path


@attrs.define(kw_only=True)
class MockRemote(RemoteStore):
    """A simple data store to test store CLI.

    "Leaf" rows are stored in a separate sub-directories of the dataset id, with
    each part of the row id "<base-freq>=<id-part>" separated by '.', e.g.

    /path/to/dataset/leaves/a=a1.bc=b3c1.d=d7

    Under the row directory, items are stored in a directory named by their path, e.g.

    /path/to/dataset/leaves/a=a1.bc=b3c1.d=d7/scan1

    Non-leaf "rows" (e.g. higher up in the data hierarchy) are stored in a separate
    base directory and are stored by the "span" of their frequency in the dataspace, e.g.
    a row of frequency TestDataSpace.abc, would be stored at

    /path/to/dataset/nodes/a=1.b=3.c=1/
    """

    remote_dir: Path = attrs.field(converter=full_path)
    mock_delay: int = 0
    connected: bool = False
    "Mock delay used to simulate time it takes to download from remote"

    SITE_LICENSES_DIR = "LICENSE"
    METADATA_DIR = ".definition"
    LEAVES_DIR = "leaves"
    NON_LEAVES_DIR = "non-leaves"
    FIELDS_FILE = "__FIELD__"
    CHECKSUMS_FILE = "__CHECKSUMS__.json"

    #############################
    # DataStore abstractmethods #
    #############################

    def populate_tree(self, tree: DataTree):
        """
        Find all data rows for a dataset in the store and populate the
        Dataset object using its `add_leaf` method.

        Parameters
        ----------
        dataset : Dataset
            The dataset to populate with rows
        """
        with self.connection:
            self._check_connected()
            leaves_dir = self.dataset_fspath(tree.dataset_id) / self.LEAVES_DIR
            if not leaves_dir.exists():
                raise RuntimeError(
                    f"Leaves dir {leaves_dir} for flat-dir data store doesn't exist, which "
                    "means it hasn't been initialised properly"
                )
            for row_dir in self.iterdir(leaves_dir):
                ids = self.get_ids_from_row_dirname(row_dir)
                tree.add_leaf([ids[h] for h in tree.hierarchy])

    def populate_row(self, row: DataRow):
        """
        Find all data items within a data row and populate the DataRow object
        with them using the `add_fileset` and `add_field` methods.

        Parameters
        ----------
        row : DataRow
            The data row to populate with items
        """
        with self.connection:
            self._check_connected()
            try:
                row_dir = self.get_row_path(row)
            except NotInHierarchyException:
                return
            if not row_dir.exists():
                return
            for path in self.iterdir(row_dir, skip_suffixes=[".json"]):
                datatype = (
                    Field if path / self.FIELDS_FILE in path.iterdir() else FileSet
                )
                row.add_entry(
                    path=path.name,
                    datatype=datatype,
                    uri=full_path(path).relative_to(self.remote_dir),
                )

    def save_dataset_definition(
        self, dataset_id: str, definition: ty.Dict[str, ty.Any], name: str
    ):
        """Save definition of dataset within the store

        Parameters
        ----------
        dataset_id: str
            The ID/path of the dataset within the store
        definition: ty.Dict[str, Any]
            A dictionary containing the dct Dataset to be saved. The
            dictionary is in a format ready to be dumped to file as JSON or
            YAML.
        name: str
            Name for the dataset definition to distinguish it from other
            definitions for the same directory/project"""
        self._check_connected()
        definition_path = self.definition_save_path(dataset_id, name)
        definition_path.parent.mkdir(exist_ok=True)
        with open(definition_path, "w") as f:
            yaml.dump(definition, f)

    def load_dataset_definition(
        self, dataset_id: str, name: str
    ) -> ty.Dict[str, ty.Any]:
        """Load definition of a dataset saved within the store

        Parameters
        ----------
        dataset_id: str
            The ID (e.g. file-system path, XNAT project ID) of the project
        name: str
            Name for the dataset definition to distinguish it from other
            definitions for the same directory/project

        Returns
        -------
        definition: ty.Dict[str, Any]
            A dct Dataset object that was saved in the data store
        """
        self._check_connected()
        fpath = self.definition_save_path(dataset_id, name)
        if fpath.exists():
            with open(fpath) as f:
                definition = yaml.load(f, Loader=yaml.Loader)
        else:
            definition = None
        return definition

    def connect(self):
        """
        If a connection session is required to the store manage it here
        """
        self.connected = True

    def disconnect(self, session):
        """
        If a connection session is required to the store manage it here
        """
        self.connected = False

    def get_provenance(self, entry: DataEntry) -> ty.Dict[str, ty.Any]:
        self._check_connected()
        prov_path = entry.uri.with_suffix(".json")
        if prov_path.exists():
            with open(prov_path) as f:
                provenance = json.load(f)
        else:
            provenance = None
        return provenance

    def put_provenance(self, provenance: ty.Dict[str, ty.Any], entry: DataEntry):
        self._check_connected()
        prov_path = entry.uri.with_suffix(".json")
        with open(prov_path, "w") as f:
            json.dump(provenance, f)

    def create_data_tree(
        self,
        id: str,
        leaves: ty.List[ty.Tuple[str, ...]],
        hierarchy: ty.List[str],
        **kwargs,
    ):
        """reate test data within store with rows specified by row_ids

        Parameters
        ----------
        id : str
            ID of the dataset
        leaves : list[tuple[str, ...]]
            list of IDs for each leaf node to be added to the dataset. The IDs for each
            leaf should be a tuple with an ID for each level in the tree's hierarchy, e.g.
            for a hierarchy of [subject, timepoint] ->
            [("SUBJ01", "TIMEPOINT01"), ("SUBJ01", "TIMEPOINT02"), ....]
        hierarchy : list[str]
            the hierarchy of the dataset to be created
        space : type
            the dataspace of the dataset to be created
        """
        dataset_path = self.dataset_fspath(id) / self.LEAVES_DIR
        dataset_path.mkdir(parents=True)
        for ids_tuple in leaves:
            ids = dict(zip(hierarchy, ids_tuple))
            row_path = dataset_path / self.get_row_dirname_from_ids(ids, hierarchy)
            row_path.mkdir(parents=True)

    ################################
    # RemoteStore-specific methods #
    ################################

    def download_files(self, entry: DataEntry, download_dir: Path) -> Path:
        self._check_connected()
        fileset = FileSet(self.iterdir(self.entry_fspath(entry)))
        data_path = download_dir / "downloaded"
        fileset.copy(data_path, make_dirs=True, mode=fileset.CopyMode.link)
        time.sleep(self.mock_delay)
        return data_path

    def upload_files(self, cache_path: Path, entry: DataEntry):
        self._check_connected()
        entry_fspath = self.entry_fspath(entry)
        if entry_fspath.exists():
            shutil.rmtree(entry_fspath)
        entry_fspath.parent.mkdir(exist_ok=True)
        shutil.copytree(cache_path, entry_fspath)
        checksums = self.calculate_checksums(FileSet(cache_path.iterdir()))
        with open(self.remote_dir / entry.uri / self.CHECKSUMS_FILE, "w") as f:
            json.dump(checksums, f)

    def download_value(
        self, entry: DataEntry
    ) -> ty.Union[float, int, str, ty.List[float], ty.List[int], ty.List[str]]:
        """
        Retrieves a fields value

        Parameters
        ----------
        field : Field
            The field to retrieve

        Returns
        -------
        value : ty.Union[float, int, str, ty.List[float], ty.List[int], ty.List[str]]
            The value of the field
        """
        self._check_connected()
        with open(self.entry_fspath(entry) / self.FIELDS_FILE) as f:
            value = f.read()
        return value

    def upload_value(self, value, entry: DataEntry):
        self._check_connected()
        with open(self.entry_fspath(entry) / self.FIELDS_FILE, "w") as f:
            f.write(str(value))

    def create_fileset_entry(
        self, path: str, datatype: type, row: DataRow
    ) -> DataEntry:
        return self._create_entry(path=path, datatype=datatype, row=row)

    def create_field_entry(self, path: str, datatype: type, row: DataRow) -> DataEntry:
        return self._create_entry(path=path, datatype=datatype, row=row)

    def get_checksums(self, uri: str) -> ty.Dict[str, str]:
        """
        Downloads the checksum digests associated with the files in the file-set.
        These are saved with the downloaded files in the cache and used to
        check if the files have been updated on the server

        Parameters
        ----------
        uri: str
            uri of the data item to download the checksums for
        """
        fspath = self.remote_dir / uri / self.CHECKSUMS_FILE
        if not fspath.exists():
            return None
        with open(fspath) as f:
            checksums = json.load(f)
        return checksums

    def calculate_checksums(self, fileset: FileSet) -> ty.Dict[str, str]:
        """
        Downloads the checksum digests associated with the files in the file-set.
        These are saved with the downloaded files in the cache and used to
        check if the files have been updated on the server

        Parameters
        ----------
        uri: str
            uri of the data item to download the checksums for
        """
        return fileset.hash_files()

    ##################
    # Helper methods #
    ##################

    def dataset_fspath(self, dataset):
        dataset_id = (
            dataset.id if not isinstance(dataset, (str, bytes, Path)) else dataset
        )
        return self.remote_dir / dataset_id

    def entry_fspath(self, entry):
        return self.remote_dir / entry.uri

    def _create_entry(self, path: str, datatype: type, row: DataRow) -> DataEntry:
        self._check_connected()
        entry = row.add_entry(
            path=path,
            datatype=datatype,
            uri=self.get_row_path(row) / path,
        )
        self.entry_fspath(entry).mkdir(parents=True)
        return entry

    def definition_save_path(self, dataset_id, name):
        if not name:
            name = self.EMPTY_DATASET_NAME
        return self.dataset_fspath(dataset_id) / self.METADATA_DIR / (name + ".yml")

    def get_row_path(self, row: DataRow):
        dataset_fspath = self.dataset_fspath(row.dataset)
        try:
            row_path = (
                dataset_fspath
                / self.LEAVES_DIR
                / self.get_row_dirname_from_ids(row.ids, row.dataset.hierarchy)
            )
        except NotInHierarchyException:
            if not row.frequency:  # root frequency
                row_dirname = str(row.frequency)
            else:
                row_dirname = self.get_row_dirname_from_ids(
                    row.ids, [str(h) for h in row.frequency.span()]
                )
            row_path = dataset_fspath / self.NON_LEAVES_DIR / row_dirname
        return row_path

    @classmethod
    def get_row_dirname_from_ids(
        cls, ids: ty.Dict[ty.Union[str, DataSpace], str], hierarchy: ty.List[str]
    ):
        # Ensure that ID keys are strings not DataSpace enums
        ids = {str(f): i for f, i in ids.items()}
        try:
            row_dirname = ".".join(f"{h}={ids[h]}" for h in hierarchy)
        except KeyError:
            raise NotInHierarchyException
        return row_dirname

    @classmethod
    def get_ids_from_row_dirname(cls, row_dir: Path):
        parts = row_dir.name.split(".")
        return dict(p.split("=") for p in parts)

    @classmethod
    def iterdir(cls, dr, skip_suffixes=()):
        """Iterate a directory, skipping any hidden files (i.e. starting with '.'
        or any files ending in the provided suffixes)

        Parameters
        ----------
        dr : str or Path
            the directory path to iterate
        skip_suffixes : tuple, optional
            file suffixes to skip, by default ()

        Returns
        -------
        iterator
            iterator over all paths in the directory
        """
        return (
            d
            for d in Path(dr).iterdir()
            if not (
                d.name == cls.CHECKSUMS_FILE
                or d.name.startswith(".")
                or any(d.name.endswith(s) for s in skip_suffixes)
            )
        )

    def _check_connected(self):
        if not self.connected:
            raise RuntimeError("Mock data store has not been connected")


class AlternateMockRemote(MockRemote):
    """An alternative mock remote with `put_checksums` implemented for store types like
    Flywheel that don't implement internal checksums
    """

    def put_checksums(self, uri: str, fileset: FileSet) -> ty.Dict[str, str]:
        return self.calculate_checksums(fileset)


class NotInHierarchyException(Exception):
    pass
