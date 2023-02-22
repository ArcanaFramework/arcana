from __future__ import annotations
import logging
from abc import abstractmethod, ABCMeta
from pathlib import Path
import zipfile
import shutil
from itertools import product
import attrs
import typing as ty
import yaml
from arcana.core.utils.serialize import (
    asdict,
    fromdict,
)
import arcana
from fileformats.core import DataType
from arcana.core.utils.misc import (
    get_config_file_path,
    set_cwd,
    path2varname,
    NestedContext,
)
from arcana.core.utils.packaging import list_subclasses
from arcana.core.exceptions import ArcanaUsageError, ArcanaNameError, ArcanaError


DS = ty.TypeVar("DS", bound="DataStore")

logger = logging.getLogger("arcana")


if ty.TYPE_CHECKING:
    from ..set import Dataset
    from ..tree import DataTree
    from ..entry import DataEntry
    from ..row import DataRow
    from ..testing import TestDatasetBlueprint


@attrs.define
class ConnectionManager(NestedContext):

    store: ty.Any = None
    session: ty.Any = attrs.field(default=None, init=False)

    def __getattr__(self, attr_name):
        return getattr(self.session, attr_name)

    def enter(self):
        self.session = self.store.connect()

    def exit(self):
        self.store.disconnect(self.session)
        self.session = None


@attrs.define
class DataStore(metaclass=ABCMeta):
    # """
    # Abstract base class for all Repository systems, DaRIS, XNAT and
    # local file system. Sets out the interface that all Repository
    # classes should implement.
    # """

    # name: str = None
    connection: ConnectionManager = attrs.field(
        factory=ConnectionManager, init=False, hash=False, repr=False, eq=False
    )

    def __attrs_post_init__(self):
        self.connection.store = self

    CONFIG_NAME = "stores"
    SUBPACKAGE = "data"
    VERSION_KEY = "store-version"
    VERSION = "1.0.0"

    @abstractmethod
    def scan_tree(self, tree: DataTree):
        """
        Populates the nodes of the data tree with those found in the dataset

        Parameters
        ----------
        tree : DataTree
            The tree to populate with nodes via the ``DataTree.add_leaf`` method
        """

    @abstractmethod
    def scan_row(self, row: DataRow):
        """
        Populate a row with all data entries found in the corresponding node in the data
        store (e.g. files within a directory, scans within an XNAT session).

        Parameters
        ----------
        row : DataRow
            The row to populate with entries using the ``DataRow.add_entry`` method
        """

    @abstractmethod
    def get(self, entry: DataEntry, datatype: type) -> DataType:
        """
        Gets the data item corresponding to the given entry

        Parameters
        ----------
        entry : DataEntry
            the data entry to update
        datatype : type
            the datatype to interpret the entry's item as

        Returns
        -------
        item : DataType
            the item stored within the specified entry
        """

    @abstractmethod
    def put(self, item: DataType, entry: DataEntry) -> DataType:
        """
        Updates the item in the data store corresponding to the given data entry

        Parameters
        ----------
        item : DataType
            the item to replace the current item in the data store
        entry: DataEntry
            the data entry to update

        Returns
        -------
        cached : DataType
            returns the cached version of the item, if applicable
        """

    @abstractmethod
    def post(
        self, item: DataType, path: str, datatype: type, row: DataRow
    ) -> DataEntry:
        """Inserts the item within a newly created entry in the data store

        Parameters
        ----------
        item : DataType
            the item to insert
        path : str
            the path to the entry relative to the data row
        datatype : type
            the datatype of the entry
        row : DataRow
            the data row to insert the entry into

        Returns
        -------
        entry : DataEntry
            the inserted entry
        """

    @abstractmethod
    def put_provenance(self, entry: DataEntry, provenance: dict[str, ty.Any]):
        """Stores provenance information for a given data item in the store

        Parameters
        ----------
        item: DataType
            The item to store the provenance data for
        provenance: dict[str, Any]
            The provenance data to store"""

    @abstractmethod
    def get_provenance(self, entry: DataEntry) -> dict[str, ty.Any]:
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

    @abstractmethod
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

    @abstractmethod
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

    def create_empty_dataset(
        self,
        id: str,
        hierarchy: list[str],
        row_ids: list[list[str]],
        space: type,
        name: str = None,
        **kwargs,
    ):
        """create a new empty dataset within the store, used in test routines and
        dataset imports

        Parameters
        ----------
        id : str
            ID for the newly created dataset
        space : type(DataSpace)
            the data space of the dataset
        hierarchy : list[str]
            the hierarchy of row frequencies the tree of the dataset
        row_ids : list[list[str]]
            the empty data rows to create within the newly created dataset. Outer list
            corresponds to levels in the hierarchy, inner lists correspond to the row
            ids repeated in each branch of the corresponding level of the data tree.
        name : str, optional
            name to give to the dataset definition returned
        **kwargs
            store sub-class relevant arguments
        """
        raise NotImplementedError(
            f"{type(self).__name__} stores don't implement the `create_empty_dataset` "
            "method"
        )

    def import_dataset(self, id: str, dataset: Dataset, **kwargs):
        """Import a dataset from another store, transferring metadata and columns
        defined on the original dataset

        Parameters
        ----------
        id : str
            the ID of the dataset within this store
        dataset : Dataset
            the dataset to import
        **kwargs:
            keyword arguments passed through to the `create_empty_dataset` method
        """
        raise NotImplementedError
        # imported = self.create_empty_dataset(id, **kwargs)

    def connect(self):
        """
        If a connection session is required to the store manage it here
        """

    def disconnect(self, session):
        """
        If a connection session is required to the store manage it here
        """

    def site_licenses_dataset(self):
        """Can be overridden by subclasses to provide a dataset to hold site-wide licenses"""
        return None

    def save(self, name: str = None, config_path: Path = None):
        """Saves the configuration of a DataStore in 'stores.yaml'

        Parameters
        ----------
        name
            The name under which to save the data store
        config_path : Path, optional
            the path to save the config file to, defaults to `~/.arcana/stores.yaml`
        """
        if name in self.singletons():
            raise ArcanaNameError(
                name, f"Name '{name}' clashes with built-in type of store"
            )
        if name is None:
            if self.name is None:
                raise ArcanaNameError(
                    f"Must provide name to save store {self} as as it doesn't have one "
                    "already"
                )
        else:
            self.name = name
        entries = self.load_saved_entries()
        # connect to store in case it is needed in the asdict method and to
        # test the connection in general before it is saved
        dct = self.asdict()
        with self.connection:
            entries[dct.pop("name")] = dct
        self.save_entries(entries, config_path=config_path)

    def asdict(self, **kwargs):
        return asdict(self, **kwargs)

    @classmethod
    def load(
        cls: DataStore, name: str, config_path: Path = None, **kwargs
    ) -> DataStore:
        """Loads a DataStore from that has been saved in the configuration file.
        If no entry is saved under that name, then it searches for DataStore
        sub-classes with aliases matching `name` and checks whether they can
        be initialised without any parameters.

        Parameters
        ----------
        name : str
            Name that the store was saved under
        config_path : Path, optional
            path to the config file, defaults to `~/.arcana/stores.yaml`
        **kwargs
            keyword args passed to the store, overriding values stored in the
            entry

        Returns
        -------
        DataStore
            The data store retrieved from the stores.yaml file

        Raises
        ------
        ArcanaNameError
            If the name is not found in the saved stores
        """
        entries = cls.load_saved_entries(config_path)
        try:
            entry = entries[name]
        except KeyError:
            try:
                return cls.singletons()[name]
            except KeyError:
                raise ArcanaNameError(
                    name, f"No saved data store or built-in type matches '{name}'"
                )
        else:
            entry.update(kwargs)
            entry["name"] = name
            store = fromdict(entry)  # Would be good to use a class resolver here
        return store

    @classmethod
    def remove(cls, name: str, config_path: Path = None):
        """Removes the entry saved under 'name' in the config file

        Parameters
        ----------
        name
            Name of the configuration to remove
        """
        entries = cls.load_saved_entries(config_path)
        del entries[name]
        cls.save_entries(entries)

    def define_dataset(self, id, space=None, hierarchy=None, **kwargs):
        """
        Creates a Arcana dataset definition for an existing data in the
        data store.

        Parameters
        ----------
        id : str
            The ID (or file-system path) of the project (or directory) within
            the store
        space: DataSpace
            The data space of the dataset
        hierarchy: list[DataSpace or str]
            The hierarchy of the dataset
        space : EnumMeta
            The DataSpace enum that defines the frequencies (e.g.
            per-session, per-subject,...) present in the dataset.
        **kwargs:
            Keyword args passed on to the Dataset init method
        """
        from ..space import DataSpace

        if not hierarchy:
            if space:
                hierarchy = [max(space)]
            else:
                try:
                    hierarchy = self.DEFAULT_HIERARCHY
                except AttributeError as e:
                    raise ArcanaUsageError(
                        "'hierarchy' kwarg must be specified for datasets in "
                        f"{type(self)} stores"
                    ) from e
        if not space:
            if hierarchy and isinstance(hierarchy[0], DataSpace):
                space = type(hierarchy[0])
            else:
                try:
                    space = self.DEFAULT_SPACE
                except AttributeError as e:
                    raise ArcanaUsageError(
                        "'space' kwarg must be specified for datasets in "
                        f"{type(self)} stores"
                    ) from e
        from arcana.core.data.set import (
            Dataset,
        )  # avoid circular imports it is imported here rather than at the top of the file

        dataset = Dataset(id, store=self, space=space, hierarchy=hierarchy, **kwargs)
        return dataset

    def save_dataset(self, dataset: Dataset, name: str = None):
        """Save metadata in project definition file for future reference

        Parameters
        ----------
        dataset : Dataset
            the dataset to save
        name : str, optional
            the name for the definition to distinguish from other definitions on
            the same data, by default None
        """
        definition = asdict(dataset, omit=["store", "name"])
        definition[self.VERSION_KEY] = self.VERSION
        if name is None:
            name = dataset.name
        self.save_dataset_definition(dataset.id, definition, name=name)

    def load_dataset(self, id, name=None, **kwargs):
        """Load an existing dataset definition

        Parameters
        ----------
        id : str
            ID of the dataset within the store
        name : str, optional
            name of the dataset definition, which distinguishes it from alternative
            definitions on the same data, by default None

        Returns
        -------
        Dataset
            the loaded dataset

        Raises
        ------
        KeyError
            if the dataset is not found
        """
        from arcana.core.data.set import (
            Dataset,
        )  # avoid circular imports it is imported here rather than at the top of the file

        if name is None:
            name = Dataset.DEFAULT_NAME
        dct = self.load_dataset_definition(id, name)
        if dct is None:
            raise KeyError(f"Did not find a dataset '{id}@{name}'")
        store_version = dct.pop(self.VERSION_KEY)
        self.check_store_version(store_version)
        return fromdict(dct, id=id, name=name, store=self, **kwargs)

    @classmethod
    def singletons(cls):
        """Returns stores in a dictionary indexed by their aliases, for which there
        only needs to be a single instance"""
        try:
            return cls._singletons
        except AttributeError:
            pass
        # If not saved in the configuration file search for sub-classes
        # whose alias matches `name` and can be initialised without params
        cls._singletons = {}
        for store_cls in list_subclasses(arcana, DataStore):
            try:
                store = store_cls()
            except Exception:
                pass
            else:
                cls._singletons[store.name] = store
        return cls._singletons

    @classmethod
    def load_saved_entries(cls, config_path: Path = None):
        if config_path is None:
            config_path = get_config_file_path(cls.CONFIG_NAME)
        if config_path.exists():
            with open(config_path) as f:
                entries = yaml.load(f, Loader=yaml.Loader)
        else:
            entries = {}
        return entries

    @classmethod
    def save_entries(cls, entries, config_path: Path = None):
        if config_path is None:
            config_path = get_config_file_path(cls.CONFIG_NAME)
        with open(config_path, "w") as f:
            yaml.dump(entries, f)

    def create_test_dataset_data(
        self, blueprint: TestDatasetBlueprint, dataset_id: str, source_data: Path = None
    ):
        """Creates the test data in the store, from the provided blueprint, which
        can be used to run test routines against

        Parameters
        ----------
        blueprint
            the test dataset blueprint
        dataset_path : Path
            the pat
        """
        raise NotImplementedError(
            f"'create_test_dataset_data' method hasn't been implemented for {type(self)} "
            "class please create it to use 'make_test_dataset' method in your test "
            "routines"
        )

    @classmethod
    def create_test_fsobject(
        cls,
        fname: str,
        dpath: Path,
        source_data: Path = None,
        source_fallback: bool = False,
        escape_source_name: bool = True,
    ):
        """For use in test routines, this classmethod creates a simple text file,
        zip file or nested directory at the given path

        Parameters
        ----------
        fname : str
            name of the file to create, a file or directory will be created depending
            on the name given
        dpath : Path
            the path at which to create the file/directory
        source_data : Path, optional
            path to a directory containing source data to use instead of the dummy
            data
        source_fallback : bool
            whether to fall back to the generated file if fname isn't in the source
            data dir
        escape_source_name : bool
            whether to escape the source name or simple use the file name of the source

        Returns
        -------
        Path
            path to the created file/directory
        """
        dpath = Path(dpath)
        dpath.mkdir(parents=True, exist_ok=True)
        if source_data is not None:
            src_path = source_data.joinpath(*fname.split("/"))
            if src_path.exists():
                if escape_source_name:
                    parts = fname.split(".")
                    out_fname = path2varname(parts[0]) + "." + ".".join(parts[1:])
                else:
                    out_fname = Path(fname).name
                out_path = dpath / out_fname
                if src_path.is_dir():
                    shutil.copytree(src_path, out_path)
                else:
                    shutil.copyfile(src_path, out_path, follow_symlinks=True)
                return out_path
            elif not source_fallback:
                raise ArcanaError(
                    f"Couldn't find {fname} in source data directory {source_data}"
                )
        out_path = dpath / fname
        next_part = fname
        if next_part.endswith(".zip"):
            next_part = next_part.strip(".zip")
        next_path = Path(next_part)
        # Make double dir
        if next_part.startswith("doubledir"):
            (dpath / next_path).mkdir(exist_ok=True)
            next_part = "dir"
            next_path /= next_part
        if next_part.startswith("dir"):
            (dpath / next_path).mkdir(exist_ok=True)
            next_part = "test.txt"
            next_path /= next_part
        if not next_path.suffix:
            next_path = next_path.with_suffix(".txt")
        if next_path.suffix == ".json":
            contents = '{"a": 1.0}'
        else:
            contents = fname
        with open(dpath / next_path, "w") as f:
            f.write(contents)
        if fname.endswith(".zip"):
            with zipfile.ZipFile(out_path, mode="w") as zfile, set_cwd(dpath):
                zfile.write(next_path)
            (dpath / next_path).unlink()
        return out_path

    def make_test_dataset(
        self,
        blueprint: TestDatasetBlueprint,
        dataset_id: str,
        source_data: Path = None,
        **kwargs,
    ):
        """For use in tests, this method creates a test dataset from the provided
        blueprint"""
        self.create_test_dataset_data(
            blueprint, dataset_id, source_data=source_data, **kwargs
        )
        return self.access_test_dataset(blueprint, dataset_id)

    def access_test_dataset(self, blueprint, dataset_id):
        dataset = self.define_dataset(
            dataset_id,
            hierarchy=blueprint.hierarchy,
            id_inference=blueprint.id_inference,
        )
        dataset.__annotations__["blueprint"] = blueprint
        return dataset

    @classmethod
    def iter_test_blueprint(cls, blueprint: TestDatasetBlueprint):
        """Iterate all leaves of the data tree specified by the test blueprint"""
        for id_tple in product(*(list(range(d)) for d in blueprint.dim_lengths)):
            base_ids = dict(zip(blueprint.space.axes(), id_tple))
            ids = {}
            for layer in blueprint.hierarchy:
                ids[layer] = "".join(f"{b}{base_ids[b]}" for b in layer.span())
            yield ids

    def check_store_version(self, store_version: str):
        """Check whether version store used to save the dataset is compatible with the
        current version of the software. Can be overridden by store subclasses where
        appropriate

        Parameters
        ----------
        store_version : str
            version of the store used to save the dataset

        Raises
        ------
        ArcanaError
            if the saved version isn't compatible
        """
        if store_version != self.VERSION:
            raise ArcanaError(
                f"Stored version of dataset ({store_version}) does not match current "
                f"version of {type(self).__name__} ({self.VERSION})"
            )
