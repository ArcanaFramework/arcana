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
from arcana.core.exceptions import ArcanaUsageError, ArcanaNameError

DS = ty.TypeVar("DS", bound="DataStore")

logger = logging.getLogger("arcana")


if ty.TYPE_CHECKING:
    from .space import DataSpace
    from .set import DataTree
    from .entry import DataEntry
    from .row import DataRow


@attrs.define(kw_only=True)
class ExpDatatypeBlueprint:

    datatype: type
    filenames: list[str]


@attrs.define(kw_only=True)
class DerivBlueprint:

    name: str
    row_frequency: DataSpace
    datatype: type
    filenames: ty.List[str]


@attrs.define(slots=False, kw_only=True)
class TestDatasetBlueprint:

    hierarchy: ty.List[DataSpace]
    dim_lengths: ty.List[int]  # size of layers a-d respectively
    files: ty.List[str]  # files present at bottom layer
    id_inference: ty.List[ty.Tuple[DataSpace, str]] = attrs.field(
        factory=list
    )  # id_inference dict
    expected_datatypes: ty.Dict[str, ExpDatatypeBlueprint] = attrs.field(
        factory=dict
    )  # expected formats
    derivatives: ty.List[DerivBlueprint] = attrs.field(
        factory=list
    )  # files to insert as derivatives

    @property
    def space(self):
        return type(self.hierarchy[0])


@attrs.define
class ConnectionManager(NestedContext):

    store: ty.Any = None
    _connection: ty.Any = attrs.field(default=None, init=False)

    def __getattr__(self, attr_name):
        return getattr(self._connection, attr_name)

    def enter(self):
        self._connection = self.store.connect()

    def disconnect(self):
        self.store.disconnect(self._connection)
        self._connection = None


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
    def get(self, entry: DataEntry) -> DataType:
        """
        Cache the fileset locally (if required) and return the locations
        of the cached primary file and side cars

        Parameters
        ----------
        fileset : FileSet
            The fileset to cache locally
        cache_only : bool
            Whether to attempt to extract the file sets from the local cache
            (if applicable) and raise an error otherwise

        Returns
        -------
        fspaths : list[str]
            The file-system path to the cached files

        Raises
        ------
        ArcanaCacheError
            If cache_only is set and there is a mismatch between the cached
            and remote versions
        """

    @abstractmethod
    def put(self, entry: DataEntry, item: DataType):
        """
        Inserts or updates the fileset into the store

        Parameters
        ----------
        fileset : FileSet
            The fileset to insert into the store
        fspaths : list[Path]
            The file-system paths to the files/directories to sync

        Returns
        -------
        cached_paths : list[str]
            The paths of the files where they are cached in the file system
        """

    # @abstractmethod
    # def put_provenance(self, item, provenance: ty.Dict[str, ty.Any]):
    #     """Stores provenance information for a given data item in the store

    #     Parameters
    #     ----------
    #     item: DataType
    #         The item to store the provenance data for
    #     provenance: dict[str, Any]
    #         The provenance data to store"""

    # @abstractmethod
    # def get_provenance(self, item) -> ty.Dict[str, ty.Any]:
    #     """Stores provenance information for a given data item in the store

    #     Parameters
    #     ----------
    #     item: DataType
    #         The item to store the provenance data for

    #     Returns
    #     -------
    #     provenance: dict[str, Any] or None
    #         The provenance data stored in the repository for the data item.
    #         None if no provenance data has been stored"""

    def connect(self):
        """
        If a connection session is required to the store manage it here
        """

    def disconnect(self):
        """
        If a connection session is required to the store manage it here
        """

    def site_licenses_dataset(self):
        """Can be overridden by subclasses to provide a dataset to hold site-wide licenses"""
        return None

    def get_checksums(self, entry: DataEntry):
        """
        Override this method to return checksums for files that are stored
        with remote files (e.g. in XNAT). If no checksums are stored in the
        store then just leave this method to just access the file and
        recalculate them.

        Parameters
        ----------
        entry : DataEntry
            The entry to return the checksums for

        Returns
        -------
        checksums : dct[str, str]
            A dictionary with keys corresponding to the relative paths of all
            files in the fileset from the base path and values equal to the
            MD5 hex digest. The primary file in the file-set (i.e. the one that
            the path points to) should be specified by '.'.
        """

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
        with self:
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

    def new_dataset(self, id, space=None, hierarchy=None, **kwargs):
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
            raise KeyError(f"Did not find a dataset '{id}@{name}'")
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

    # def __enter__(self):
    #     # This allows the store to be used within nested contexts
    #     # but still only use one connection. This is useful for calling
    #     # methods that need connections, and therefore control their
    #     # own connection, in batches using the same connection by
    #     # placing the batch calls within an outer context.
    #     if self._connection_depth == 0:
    #         self.connect()
    #     self._connection_depth += 1
    #     return self

    # def __exit__(self, exception_type, exception_value, traceback):
    #     self._connection_depth -= 1
    #     if self._connection_depth == 0:
    #         self.disconnect()

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
    def create_test_data_item(cls, fname: str, dpath: Path, source_data: Path = None):
        """For use in test routines, this classmethod creates a simple text file
        or nested directory at the given path

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

        Returns
        -------
        Path
            path to the created file/directory
        """
        dpath = Path(dpath)
        dpath.mkdir(parents=True, exist_ok=True)
        if source_data is not None:
            src_path = source_data.joinpath(*fname.split("/"))
            parts = fname.split(".")
            fpath = dpath / (path2varname(parts[0]) + "." + ".".join(parts[1:]))
            fpath.parent.mkdir(exist_ok=True)
            if src_path.is_dir():
                shutil.copytree(src_path, fpath)
            else:
                shutil.copyfile(src_path, fpath, follow_symlinks=True)
        else:
            next_part = fname
            if next_part.endswith(".zip"):
                next_part = next_part.strip(".zip")
            fpath = Path(next_part)
            # Make double dir
            if next_part.startswith("doubledir"):
                (dpath / fpath).mkdir(exist_ok=True)
                next_part = "dir"
                fpath /= next_part
            if next_part.startswith("dir"):
                (dpath / fpath).mkdir(exist_ok=True)
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
        dataset = self.new_dataset(
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
