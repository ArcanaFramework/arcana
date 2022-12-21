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
from arcana.core.utils.misc import get_config_file_path
from arcana.core.utils.packaging import list_subclasses
from arcana.core.exceptions import ArcanaUsageError, ArcanaNameError

DS = ty.TypeVar("DS", bound="DataStore")

logger = logging.getLogger("arcana")


@attrs.define
class DataStore(metaclass=ABCMeta):
    # """
    # Abstract base class for all Repository systems, DaRIS, XNAT and
    # local file system. Sets out the interface that all Repository
    # classes should implement.
    # """

    _connection_depth = attrs.field(
        default=0, init=False, hash=False, repr=False, eq=False
    )

    CONFIG_NAME = "stores"

    SUBPACKAGE = "data"

    @abstractmethod
    def find_rows(self, dataset):
        """
        Find all data rows for a dataset in the store and populate the
        Dataset object using its `add_row` method.

        Parameters
        ----------
        dataset : Dataset
            The dataset to populate with rows
        """

    @abstractmethod
    def find_items(self, row):
        """
        Find all data items within a data row and populate the DataRow object
        with them using the `add_file_group` and `add_field` methods.

        Parameters
        ----------
        row : DataRow
            The data row to populate with items
        """

    @abstractmethod
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

    @abstractmethod
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

    @abstractmethod
    def put_file_group_paths(self, file_group, fs_paths):
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

    @abstractmethod
    def put_field_value(self, field, value):
        """
        Inserts or updates the fields into the store

        Parameters
        ----------
        field : Field
            The field to insert into the store
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

    @abstractmethod
    def put_provenance(self, item, provenance: ty.Dict[str, ty.Any]):
        """Stores provenance information for a given data item in the store

        Parameters
        ----------
        item: DataType
            The item to store the provenance data for
        provenance: dict[str, Any]
            The provenance data to store"""

    @abstractmethod
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

    def get_checksums(self, file_group):
        """
        Override this method to return checksums for files that are stored
        with remote files (e.g. in XNAT). If no checksums are stored in the
        store then just leave this method to just access the file and
        recalculate them.

        Parameters
        ----------
        file_group : FileGroup
            The file_group to return the checksums for

        Returns
        -------
        checksums : dct[str, str]
            A dictionary with keys corresponding to the relative paths of all
            files in the file_group from the base path and values equal to the
            MD5 hex digest. The primary file in the file-set (i.e. the one that
            the path points to) should be specified by '.'.
        """
        return file_group.calculate_checksums()

    def connect(self):
        """
        If a connection session is required to the store manage it here
        """

    def disconnect(self):
        """
        If a connection session is required to the store manage it here
        """

    def save(self, name: str, config_path: Path = None):
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
        entries = self.load_saved_entries()
        # connect to store in case it is needed in the asdict method and to
        # test the connection in general before it is saved
        with self:
            entries[name] = self.asdict()
        self.save_entries(entries, config_path=config_path)

    def asdict(self, **kwargs):
        return asdict(self, **kwargs)

    def site_licenses_dataset(self):
        """Can be overridden by subclasses to provide a dataset to hold site-wide licenses"""
        return None

    @classmethod
    def load(cls: ty.Type[DS], name: str, config_path: Path = None, **kwargs) -> DS:
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

    def new_dataset(self, id, hierarchy=None, space=None, **kwargs):
        """
        Returns a dataset from the XNAT repository

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
        from .space import DataSpace

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

    def load_dataset(self, id, name=None):
        from arcana.core.data.set import (
            Dataset,
        )  # avoid circular imports it is imported here rather than at the top of the file

        if name is None:
            name = Dataset.DEFAULT_NAME
        dct = self.load_dataset_definition(id, name)
        if dct is None:
            raise KeyError(f"Did not find a dataset '{id}::{name}'")
        return fromdict(dct, id=id, name=name, store=self)

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
        for store_cls in list_subclasses(arcana, DataStore, subpkg="data"):
            try:
                cls._singletons[store_cls.get_alias()] = store_cls()
            except Exception:
                pass
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

    def __enter__(self):
        # This allows the store to be used within nested contexts
        # but still only use one connection. This is useful for calling
        # methods that need connections, and therefore control their
        # own connection, in batches using the same connection by
        # placing the batch calls within an outer context.
        if self._connection_depth == 0:
            self.connect()
        self._connection_depth += 1
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self._connection_depth -= 1
        if self._connection_depth == 0:
            self.disconnect()

    @classmethod
    def get_alias(cls):
        try:
            alias = cls.alias
        except AttributeError:
            alias = cls.__name__.lower()
        return alias
