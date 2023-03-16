from __future__ import annotations
import logging
from abc import abstractmethod, ABCMeta
from pathlib import Path
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

    ####################
    # Abstract methods #
    ####################

    @abstractmethod
    def populate_tree(self, tree: DataTree):
        """
        Populates the nodes of the data tree with those found in the dataset

        Parameters
        ----------
        tree : DataTree
            The tree to populate with nodes via the ``DataTree.add_leaf`` method
        """

    @abstractmethod
    def populate_row(self, row: DataRow):
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
    def put_provenance(self, provenance: dict[str, ty.Any], entry: DataEntry):
        """Stores provenance information for a given data item in the store

        Parameters
        ----------
        entry: DataEntry
            The item to store the provenance data for
        provenance: dict[str, Any]
            The provenance data to store
        """

    @abstractmethod
    def get_provenance(self, entry: DataEntry) -> dict[str, ty.Any]:
        """Stores provenance information for a given data item in the store

        Parameters
        ----------
        entry: DataEntry
            The item to store the provenance data for

        Returns
        -------
        provenance: dict[str, Any] or None
            The provenance data stored in the repository for the data item.
            None if no provenance data has been stored
        """

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
            definitions for the same directory/project
        """

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

    @abstractmethod
    def connect(self) -> ty.Any:
        """
        If a connection session is required to the store manage it here

        Returns
        ----------
        session : Any
            a session object that will be stored in the connection manager and
            accessible at `DataStore.connection`
        """

    @abstractmethod
    def disconnect(self, session: ty.Any):
        """
        If a connection session is required to the store manage it here

        Parameters
        ----------
        session : Any
            the session object returned by `connect` to be closed gracefully
        """

    @abstractmethod
    def site_licenses_dataset(self):
        """Can be overridden by subclasses to provide a dataset to hold site-wide licenses"""

    @abstractmethod
    def create_data_tree(
        self,
        id: str,
        leaves: list[tuple[str, ...]],
        hierarchy: list[str],
        space: type,
        id_composition: dict[str, str],
        **kwargs,
    ):
        """Creates a new empty dataset within in the store. Used in test routines and
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
        id_composition : dict[str, str]
            Not all IDs will appear explicitly within the hierarchy of the data
            tree, and some will need to be inferred by extracting components of
            more specific labels.

            For example, given a set of subject IDs that combination of the ID of
            the group that they belong to and the member ID within that group
            (i.e. matched test & control would have same member ID)

                CONTROL01, CONTROL02, CONTROL03, ... and TEST01, TEST02, TEST03

            the group ID can be extracted by providing the a list of tuples
            containing ID to source the inferred IDs from coupled with a regular
            expression with named groups

                id_composition = {
                    'subject': r'(?P<group>[A-Z]+)(?P<member>[0-9]+)')
                }
        **kwargs
            implementing methods should take wildcard **kwargs to allow compatibility
            with future arguments that might be added
        """
        raise NotImplementedError

    @abstractmethod
    def create_entry(self, path: str, datatype: type, row: DataRow) -> DataEntry:
        """Creates an "entry" in the store to hold a new data item

        Parameters
        ----------
        path : str
            path to the entry relative to the data "row"
        datatype : type
            the datatype of the entry
        row : DataRow
            the row (tree node) to create the entry in

        Returns
        -------
        entry : DataEntry
            the newly created entry
        """

    # Can be overridden if necessary (e.g. the underlying store only returns new URI
    # when a new item is added)
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
        with self.connection:
            entry = self.create_entry(path, datatype, row)
            self.put(item, entry)

    ###############
    # General API #
    ###############

    def import_dataset(self, id: str, dataset: Dataset, name="", **kwargs):
        """Import a dataset from another store, transferring metadata and columns
        defined on the original dataset

        Parameters
        ----------
        id : str
            the ID of the dataset within this store
        dataset : Dataset
            the dataset to import
        name : str
            the name to save the specification under
        **kwargs:
            keyword arguments passed through to the `create_data_tree` method
        """
        raise NotImplementedError
        # imported = self.create_data_tree(id, **kwargs)

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
        entries = self.load_saved_configs()
        # connect to store in case it is needed in the asdict method and to
        # test the connection in general before it is saved
        dct = self.asdict()
        with self.connection:
            entries[dct.pop("name")] = dct
        self.save_configs(entries, config_path=config_path)

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
        entries = cls.load_saved_configs(config_path)
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
            entry.update({k: v for k, v in kwargs.items() if v is not None})
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
        entries = cls.load_saved_configs(config_path)
        del entries[name]
        cls.save_configs(entries)

    def define_dataset(self, id, space=None, hierarchy=None, **kwargs) -> Dataset:
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
        hierarchy: list[str]
            The hierarchy of the dataset
        space : EnumMeta
            The DataSpace enum that defines the frequencies (e.g.
            per-session, per-subject,...) present in the dataset.
        **kwargs:
            Keyword args passed on to the Dataset init method

        Returns
        -------
        Dataset
            the newly defined dataset
        """
        if not space:
            try:
                space = self.DEFAULT_SPACE
            except AttributeError as e:
                raise ArcanaUsageError(
                    "'space' kwarg must be specified for datasets in "
                    f"{type(self)} stores"
                ) from e
        if not hierarchy:
            try:
                hierarchy = self.DEFAULT_HIERARCHY
            except AttributeError:
                hierarchy = [str(max(space))]  # one-layer with only leaf nodes
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
        with self.connection:
            self.save_dataset_definition(dataset.id, definition, name=name)

    def load_dataset(self, id, name="", **kwargs) -> Dataset:
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
        with self.connection:
            dct = self.load_dataset_definition(id, name)
        if dct is None:
            raise KeyError(f"Did not find a dataset '{id}@{name}'")
        store_version = dct.pop(self.VERSION_KEY)
        self.check_store_version(store_version)
        return fromdict(dct, id=id, name=name, store=self, **kwargs)

    def create_dataset(
        self,
        id: str,
        leaves: list[tuple[str, ...]],
        hierarchy: list[str],
        space: type,
        name: str = None,
        id_composition: dict[str, str] = None,
        **kwargs,
    ) -> Dataset:
        """Creates a new dataset with new rows to store data in

        Parameters
        ----------
        id : str
            ID of the dataset
        leaves : list[tuple[str, ...]]
            the list of tuple IDs (at each level of the tree)
        name : str, optional
            name of the dataset, if provided the dataset definition will be saved. To
            save the dataset with the default name pass an empty string.
        hierarchy : list[str], optional
            hierarchy of the dataset tree
        space : type, optional
            the space of the dataset
        id_composition : dict[str, str]
            Not all IDs will appear explicitly within the hierarchy of the data
            tree, and some will need to be inferred by extracting components of
            more specific labels.

            For example, given a set of subject IDs that combination of the ID of
            the group that they belong to and the member ID within that group
            (i.e. matched test & control would have same member ID)

                CONTROL01, CONTROL02, CONTROL03, ... and TEST01, TEST02, TEST03

            the group ID can be extracted by providing the a list of tuples
            containing ID to source the inferred IDs from coupled with a regular
            expression with named groups

                id_composition = {
                    'subject': r'(?P<group>[A-Z]+)(?P<member>[0-9]+)')
                }

        Returns
        -------
        Dataset
            the newly created dataset
        """
        self.create_data_tree(
            id=id,
            leaves=leaves,
            hierarchy=hierarchy,
            space=space,
            id_composition=id_composition,
        )
        dataset = self.define_dataset(
            id=id,
            hierarchy=hierarchy,
            space=space,
            id_composition=id_composition,
            **kwargs,
        )
        if name is not None:
            dataset.save(name=name)
        return dataset

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
    def load_saved_configs(cls, config_path: Path = None) -> dict[str, ty.Any]:
        """Loads the saved data store configurations from the the user's home
        directory

        Parameters
        ----------
        config_path : Path, optional
            the file-system path to the configuration, by default uses one in ~/.arcana

        Returns
        -------
        dict[str, ty.Any]
            dictionary containing the saved configs
        """
        if config_path is None:
            config_path = get_config_file_path(cls.CONFIG_NAME)
        if config_path.exists():
            with open(config_path) as f:
                configs = yaml.load(f, Loader=yaml.Loader)
        else:
            configs = {}
        return configs

    @classmethod
    def save_configs(cls, configs: dict[str, ty.Any], config_path: Path = None):
        """_summary_

        Parameters
        ----------
        configs : dict[str, ty.Any]
            dictionary containing the configs to save
        config_path : Path, optional
            the file-system path to the configuration, by default uses one in ~/.arcana
        """
        if config_path is None:
            config_path = get_config_file_path(cls.CONFIG_NAME)
        with open(config_path, "w") as f:
            yaml.dump(configs, f)

    ##################
    # Helper methods #
    ##################

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
