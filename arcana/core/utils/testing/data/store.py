from __future__ import annotations
import typing as ty
import json
import shutil
from pathlib import Path
import attrs
import yaml
from arcana.core.data.store import DataStore, TestDatasetBlueprint
from arcana.core.data.row import DataRow
from arcana.core.data.set import Dataset
from arcana.core.data.space import DataSpace
from .space import TestDataSpace


@attrs.define
class SimpleStore(DataStore):
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

    server: str = None  # Not used, just there to test `arcana store add` CLI
    name: str = None
    user: str = None  # Not used, just there to test `arcana store add` CLI
    password: str = None  # Not used, just there to test `arcana store add` CLI
    cache_dir: Path = attrs.field(
        converter=lambda x: Path(x) if x is not None else x, default=None
    )  # Only used to store the site-licenses in

    SITE_LICENSES_DIR = "LICENSE"
    METADATA_DIR = ".definition"
    LEAVES_DIR = "leaves"
    NODES_DIR = "nodes"

    def find_rows(self, dataset: Dataset):
        """
        Find all data rows for a dataset in the store and populate the
        Dataset object using its `add_row` method.

        Parameters
        ----------
        dataset : Dataset
            The dataset to populate with rows
        """
        for row_dir in self.iterdir(Path(dataset.id) / self.LEAVES_DIR):
            ids = self.get_ids_from_row_dirname(row_dir)
            dataset.add_leaf([ids[str(h)] for h in dataset.hierarchy])

    def find_items(self, row: DataRow):
        """
        Find all data items within a data row and populate the DataRow object
        with them using the `add_file_group` and `add_field` methods.

        Parameters
        ----------
        row : DataRow
            The data row to populate with items
        """
        row_dir = self.get_row_path(row)
        if not row_dir.exists():
            return
        for item_path in self.iterdir(row_dir, skip_suffixes=[".json"]):
            prov_path = item_path.with_suffix(".json")
            if prov_path.exists():
                with open(prov_path) as f:
                    provenance = json.load(f)
            else:
                provenance = None
            row.add_file_group(
                path=item_path.name,
                file_paths=list(self.iterdir(item_path)),
                provenance=provenance,
            )

    def get_file_group_paths(self, file_group, cache_only=False):
        """
        Cache the file_group locally (if required) and return the locations
        of the cached primary file and side cars

        Parameters
        ----------
        file_group : FileGroup
            The file_group to cache locally
        cache_only : bool
            Whether to attempt to extract the file groups from the local cache
            (if applicable) and raise an error otherwise

        Returns
        -------
        fs_paths : list[str]
            The file-system path to the cached files

        Raises
        ------
        ArcanaCacheError
            If cache_only is set and there is a mismatch between the cached
            and remote versions
        """
        return list(self.iterdir(self.get_item_path(file_group)))

    def put_file_group_paths(self, file_group, fs_paths: list[Path]):
        """
        Inserts or updates the file_group into the store

        Parameters
        ----------
        file_group : FileGroup
            The file_group to insert into the store
        fs_paths : list[Path]
            The file-system paths to the files/directories to sync

        Returns
        -------
        cached_paths : list[str]
            The paths of the files where they are cached in the file system
        """
        item_dir = self.get_item_path(file_group)
        cached_paths = []
        for fs_path in fs_paths:
            dst_path = item_dir / fs_path.name
            dst_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(fs_path, dst_path)
            cached_paths.append(dst_path)
        return cached_paths

    def save_dataset_definition(
        self, dataset_id: str, definition: ty.Dict[str, ty.Any], name: str
    ):
        """Save definition of dataset within the store

        Parameters
        ----------
        dataset_id: str
            The ID/path of the dataset within the store
        definition: dict[str, Any]
            A dictionary containing the dct Dataset to be saved. The
            dictionary is in a format ready to be dumped to file as JSON or
            YAML.
        name: str
            Name for the dataset definition to distinguish it from other
            definitions for the same directory/project"""
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
        definition: dict[str, Any]
            A dct Dataset object that was saved in the data store
        """
        fpath = self.definition_save_path(dataset_id, name)
        if fpath.exists():
            with open(fpath) as f:
                definition = yaml.load(f, Loader=yaml.Loader)
        else:
            definition = None
        return definition

    def definition_save_path(self, dataset_id, name):
        return Path(dataset_id) / self.METADATA_DIR / (name + ".yml")

    def put_provenance(self, item, provenance: ty.Dict[str, ty.Any]):
        """Stores provenance information for a given data item in the store

        Parameters
        ----------
        item: DataType
            The item to store the provenance data for
        provenance: dict[str, Any]
            The provenance data to store"""
        with open(self.get_item_path(self, item) / ".json", "w") as f:
            json.dump(provenance, f)

    def get_provenance(self, item) -> ty.Dict[str, ty.Any]:
        """Stores provenance information for a given data item in the store

        Parameters
        ----------
        item: DataType
            The item to store the provenance data for

        Returns
        -------
        provenance: dict[str, Any] or None
            The provenance data stored in the repository for the data item.
            None if no provenance data has been stored"""
        with open(self.get_item_path(self, item) / ".json") as f:
            provenance = json.load(f)
        return provenance

    def site_licenses_dataset(self):
        """Provide a place to store hold site-wide licenses"""
        if self.cache_dir is None:
            raise Exception("Cache dir needs to be set")
        dataset_root = self.cache_dir / self.SITE_LICENSES_DIR
        if not dataset_root.exists():
            (dataset_root / self.LEAVES_DIR).mkdir(parents=True)
        try:
            dataset = self.load_dataset(dataset_root)
        except KeyError:
            dataset = self.new_dataset(dataset_root, space=TestDataSpace)
        return dataset

    def get_field_value(self, field):
        """
        Extract and return the value of the field from the store

        Parameters
        ----------
        field : Field
            The field to retrieve the value for

        Returns
        -------
        value : int | float | str | ty.List[int] | ty.List[float] | ty.List[str]
            The value of the Field
        """
        raise NotImplementedError

    def put_field_value(self, field, value):
        """
        Inserts or updates the fields into the store

        Parameters
        ----------
        field : Field
            The field to insert into the store
        """
        raise NotImplementedError

    @classmethod
    def get_row_path(cls, row: DataRow):
        dataset_path = Path(row.dataset.id)
        if row.frequency == max(row.dataset.space):
            row_path = (
                dataset_path
                / cls.LEAVES_DIR
                / cls.get_row_dirname_from_ids(row.ids, row.dataset.hierarchy)
            )
        else:
            if not row.frequency:  # root frequency
                row_dirname = str(row.frequency)
            else:
                row_dirname = cls.get_row_dirname_from_ids(
                    row.ids, row.frequency.span()
                )
            row_path = dataset_path / cls.NODES_DIR / row_dirname
        return row_path

    @classmethod
    def get_row_dirname_from_ids(
        cls, ids: dict[ty.Union[str, DataSpace], str], hierarchy: list[DataSpace]
    ):
        space = type(hierarchy[0])
        # Ensure that ID keys are DataSpace enums not strings
        ids = {space[str(f)]: i for f, i in ids.items()}
        row_dirname = ".".join(f"{h}={ids[h]}" for h in hierarchy)
        return row_dirname

    @classmethod
    def get_ids_from_row_dirname(cls, row_dir: Path):
        parts = row_dir.name.split(".")
        return dict(p.split("=") for p in parts)

    @classmethod
    def get_item_path(cls, item):
        return cls.get_row_path(item.row) / item.path

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
                d.name.startswith(".") or any(d.name.endswith(s) for s in skip_suffixes)
            )
        )

    def create_test_dataset_data(
        self, blueprint: TestDatasetBlueprint, dataset_id: str, source_data: Path = None
    ):
        """Create test data within store for test routines"""
        dataset_path = Path(dataset_id) / self.LEAVES_DIR
        dataset_path.mkdir(parents=True)
        for ids in self.iter_test_blueprint(blueprint):
            row_path = dataset_path / self.get_row_dirname_from_ids(
                ids, blueprint.hierarchy
            )
            row_path.mkdir(parents=True)
            for fname in blueprint.files:
                cell_path = row_path / fname.split(".")[0]
                self.create_test_data_item(fname, cell_path, source_data=source_data)
