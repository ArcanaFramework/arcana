from __future__ import annotations
import logging
import re
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
from fileformats.text import Plain as PlainText
from arcana.core.utils.misc import (
    get_config_file_path,
    NestedContext,
)
from arcana.core.utils.packaging import list_subclasses
from arcana.core.exceptions import (
    ArcanaUsageError,
    ArcanaNameError,
    ArcanaError,
    ArcanaDataTreeConstructionError,
)


DS = ty.TypeVar("DS", bound="DataStore")

logger = logging.getLogger("arcana")


if ty.TYPE_CHECKING:  # pragma: no cover
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
    """
    Abstract base class for all data store adapters. A data store can be an external
    data management system, e.g. XNAT, OpenNeuro, Datalad or just a defined structure
    of how to lay out data within a file-system, e.g. BIDS.

    For a data management system/data structure to be compatible with Arcana, it must
    meet a number of criteria. In Arcana, a store is assumed to

        * contain multiple projects/datasets addressable by unique IDs.
        * organise data within each project/dataset in trees
        * store arbitrary numbers of data "items" (e.g. "file-sets" and fields) within
          each tree node (including non-leaf nodes) addressable by unique "paths" relative
          to the node.
        * allow derivative data to be stored within in separate namespaces for different
          analyses on the same data
    """

    # name: ty.Optional[str] = None
    connection: ConnectionManager = attrs.field(
        factory=ConnectionManager, init=False, hash=False, repr=False, eq=False
    )

    def __attrs_post_init__(self):
        self.connection.store = self

    CONFIG_NAME = "stores"
    SUBPACKAGE = "data"
    VERSION_KEY = "store-version"
    VERSION = "1.0.0"
    # alternative name used to save datasets that are named "" in cases where "" is
    # not appropriate
    EMPTY_DATASET_NAME = "_"

    ##############
    # Public API #
    ##############

    def save(
        self, name: ty.Optional[str] = None, config_path: ty.Optional[Path] = None
    ):
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
                    None,
                    f"Must provide name to save store {self} as as it doesn't have one "
                    "already",
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
        cls, name: str, config_path: ty.Optional[Path] = None, **kwargs
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
    def remove(cls, name: str, config_path: ty.Optional[Path] = None):
        """Removes the entry saved under 'name' in the config file

        Parameters
        ----------
        name
            Name of the configuration to remove
        """
        entries = cls.load_saved_configs(config_path)
        del entries[name]
        cls.save_configs(entries)

    def define_dataset(
        self, id, space=None, hierarchy=None, id_patterns=None, **kwargs
    ) -> Dataset:
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
        hierarchy: ty.List[str]
            The hierarchy of the dataset
        id_patterns : dict[str, str], optional
            Patterns used to infer row IDs not explicitly within the hierarchy of the
            data tree, e.g. groups and timepoints in an XNAT project with subject>session
            hierarchy
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
        if space is None:
            try:
                space = self.DEFAULT_SPACE
            except AttributeError as e:
                raise ArcanaUsageError(
                    "'space' kwarg must be specified for datasets in "
                    f"{type(self)} stores"
                ) from e
        if hierarchy is None:
            try:
                hierarchy = list(self.DEFAULT_HIERARCHY)
            except AttributeError:
                hierarchy = [str(max(space))]  # one-layer with only leaf nodes
        # if id_patterns is None:
        #     try:
        #         id_patterns = dict(self.DEFAULT_ID_PATTERNS)
        #     except AttributeError:
        #         pass
        from arcana.core.data.set import (
            Dataset,
        )  # avoid circular imports it is imported here rather than at the top of the file

        dataset = Dataset(
            id=id,
            store=self,
            space=space,
            hierarchy=hierarchy,
            id_patterns=id_patterns,
            **kwargs,
        )
        return dataset

    def save_dataset(self, dataset: Dataset, name: str = ""):
        """Save metadata in project definition file for future reference

        Parameters
        ----------
        dataset : Dataset
            the dataset to save
        name : str, optional
            the name for the definition to distinguish from other definitions on
            the same data, by default None
        """
        if name is None:
            name = ""
        save_name = name if name else self.EMPTY_DATASET_NAME
        definition = asdict(dataset, omit=["store", "name"])
        definition[self.VERSION_KEY] = self.VERSION
        if name is None:
            name = dataset.name
        with self.connection:
            self.save_dataset_definition(dataset.id, definition, name=save_name)

    def load_dataset(self, id, name: str = "", **kwargs) -> Dataset:
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
        if name is None:
            name = ""
        saved_name = name if name else self.EMPTY_DATASET_NAME
        with self.connection:
            dct = self.load_dataset_definition(id, saved_name)
        if dct is None:
            raise KeyError(f"Did not find a dataset '{id}@{name}'")
        store_version = dct.pop(self.VERSION_KEY)
        self.check_store_version(store_version)
        return fromdict(dct, id=id, name=name, store=self, **kwargs)

    def create_dataset(
        self,
        id: str,
        leaves: ty.Iterable[ty.Tuple[str, ...]],
        hierarchy: ty.List[str],
        space: type,
        name: ty.Optional[str] = None,
        id_patterns: ty.Optional[ty.Dict[str, str]] = None,
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
        id_patterns : dict[str, str]
            Patterns for inferring IDs of rows not explicitly present in the hierarchy of
            the data tree. See ``DataStore.infer_ids()`` for syntax

        Returns
        -------
        Dataset
            the newly created dataset
        """
        self.create_data_tree(
            id=id,
            leaves=list(leaves),
            hierarchy=hierarchy,
            space=space,
        )
        dataset = self.define_dataset(
            id=id,
            hierarchy=hierarchy,
            space=space,
            id_patterns=id_patterns,
            **kwargs,
        )
        if name is not None:
            dataset.save(name=name)
        return dataset

    def import_dataset(
        self,
        id: str,
        dataset: Dataset,
        column_names: ty.Optional[ty.List[ty.Union[str, ty.Tuple[str, type]]]] = None,
        hierarchy: ty.Optional[ty.List[str]] = None,
        id_patterns: ty.Optional[ty.Dict[str, str]] = None,
        use_original_paths: bool = False,
        **kwargs,
    ):
        """Import a dataset from another store, transferring metadata and columns
        defined on the original dataset

        Parameters
        ----------
        id : str
            the ID of the dataset within this store
        dataset : Dataset
            the dataset to import
        column_names : list[str or tuple[str, type]], optional
            list of columns to the to be included in the imported dataset. Items of the
            list are either a tuple corresponding to the name of a column to import and the
            datatype to import it as. If the datatype isn't provided and the store has
            a `DEFAULT_DATATYPE` attribute it would be used instead otherwise the original
            datatype will be used, by default all columns are imported
        hierarchy : list[str], optional
            the hierarchy of the imported dataset, by default either the default
            hierarchy of the target store if applicable or the hierarchy of the original
            dataset
        id_patterns : dict[str, str]
            Patterns for inferring IDs of rows not explicitly present in the hierarchy of
            the data tree. See ``DataStore.infer_ids()`` for syntax
        use_original_paths : bool, optional
            use the original paths in the source store instead of renaming the imported
            entries to match their column names
        **kwargs:
            keyword arguments passed through to the `create_data_tree` method
        """
        with self.connection, dataset.store.connection:
            if use_original_paths:
                raise NotImplementedError
            if hierarchy is None:
                try:
                    hierarchy = self.DEFAULT_HIERARCHY
                except AttributeError:
                    hierarchy = dataset.hierarchy
            hierarchy = ty.cast(ty.List[str], hierarchy)
            if id_patterns is None:
                id_patterns = {}
                for freq, pattern in dataset.id_patterns.items():
                    source_labels = re.findall(r"(\w+):.*:[^#]+", pattern)
                    if freq not in hierarchy and set(source_labels).issubset(hierarchy):
                        id_patterns[freq] = pattern
            # Create a new dataset in the store to import the data into
            imported = self.create_dataset(
                id,
                space=dataset.space,
                hierarchy=hierarchy,
                leaves=[
                    tuple(r.frequency_id(h) for h in hierarchy) for r in dataset.rows()
                ],
                id_patterns=id_patterns,
                metadata=dataset.metadata,
                **kwargs,
            )
            # Loop through columns
            if column_names is None:
                column_names = list(dataset.columns)
            for col_name in column_names:
                try:
                    col_name, col_dtype = col_name
                except ValueError:
                    try:
                        col_dtype = self.DEFAULT_DATATYPE
                    except AttributeError:
                        col_dtype = None
                column = dataset.columns[col_name]
                if col_dtype is None:
                    col_dtype = column.datatype
                path = column.name if not column.is_sink else column.path
                # Create columns in imported dataset
                imported_col = imported.add_sink(
                    name=column.name,
                    datatype=col_dtype,
                    path=path,
                    row_frequency=column.row_frequency,
                )
                # Copy across data from dataset to import
                for cell in column.cells():
                    item = cell.item
                    if not isinstance(item, imported_col.datatype):
                        item = imported_col.datatype.convert(item)
                    imported_col[
                        tuple(cell.row.frequency_id(a) for a in dataset.space.axes())
                    ] = item
            imported.save(name="")

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
    def load_saved_configs(
        cls, config_path: ty.Optional[Path] = None
    ) -> ty.Dict[str, ty.Any]:
        """Loads the saved data store configurations from the the user's home
        directory

        Parameters
        ----------
        config_path : Path, optional
            the file-system path to the configuration, by default uses one in ~/.arcana

        Returns
        -------
        ty.Dict[str, ty.Any]
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
    def save_configs(
        cls, configs: ty.Dict[str, ty.Any], config_path: ty.Optional[Path] = None
    ):
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

    @classmethod
    def infer_ids(
        cls,
        ids: ty.Dict[str, str],
        id_patterns: ty.Dict[str, str],
        metadata: ty.Optional[ty.Dict[str, ty.Dict[str, str]]] = None,
    ):
        """Infer IDs from those explicitly provided by using the inference patterns
        provided to the dataset definition.

        Not all IDs will appear explicitly within the hierarchy of the data
        tree, and some will need to be inferred by extracting components of
        more composite labels or associated metadata.

        For example, given a set of subject IDs that combination of the ID of
        the group that they belong to and the member ID within that group
        (i.e. matched test & control would have same member ID)

            CONTROL01, CONTROL02, CONTROL03, ... and TEST01, TEST02, TEST03

        the group ID can be extracted by providing a dictionary of the IDs to
        be inferred and the patterns to derive them from. Extracted components are
        specified by 2 or 3-tuples delimited by ':', where

            1. frequency of the layer to extract the component from.
            2. the metadata value to extract. "id" is a reserved key, which refers to the layer label
            3. a regular expression with a single capturing group

        ::

            id_patterns = {
                'group': r"subject:id:([A-Z]+)[0-9]+',
                'member': r"subject:id:[A-Z]+([0-9]+)',
            }

        If you want to compose an ID out of one or more extracted patterns, they can
        be enclosed by ``#`` symbols::

            id_patterns = {
                "timepoint": r"T#session:order#"
            }

        where "order" is a special metadata field added by the data store designating
        the order in which the session was acquired within the subject. This pattern will
        produce timepoint IDs "T1", "T2", "T3", ...

        Parameters
        ----------
        ids : dict[str, str]
            explicitly provided IDs
        id_patterns : dict[str, str]
            patterns used to infer IDs not explicitly in the hierarchy of the dataset
        metadata : dict[str, ty.Dict[str, str]]
            metadata associated with the nodes in each layer. Can be used as an input
            to a pattern

        Return
        ------
        inferred_ids : dict[str, str]
            IDs inferred from the decomposition
        """
        if metadata is None:
            metadata = {}
        inferred_ids = {}
        if id_patterns is not None:
            conflicting = set(ids) & set(id_patterns)
            if conflicting:
                raise ArcanaDataTreeConstructionError(
                    "Inferred IDs from decomposition conflict with explicitly provided IDs: "
                    + str(conflicting)
                )
            for freq, pattern in id_patterns.items():
                comps = cls.pattern_comp_re.findall(pattern)
                if not comps:
                    comps = [pattern]
                    whole_str = True
                else:
                    whole_str = False
                substitutions = []
                for comp in comps:
                    parts = comp.strip("#").split(":")
                    source_freq = parts[0] if parts[0] else freq
                    attr_name = parts[1] if len(parts) >= 2 and parts[1] else "ID"
                    regex = ":".join(parts[2:])
                    if attr_name.lower() == "id":
                        attr = ids[source_freq]
                        attr_name = attr_name.upper()
                    else:
                        try:
                            attr = str(metadata[source_freq][attr_name])
                        except KeyError:
                            raise ArcanaDataTreeConstructionError(
                                f"'{ids[source_freq]}' {source_freq} row doesn't have "
                                f"the metadata field '{attr_name}'"
                            )
                    if regex:
                        match = re.fullmatch(regex, attr)
                        if not match or len(match.groups()) != 1:
                            match_msg = (
                                f"matched {len(match.groups())} groups"
                                if match
                                else "didn't match the pattern"
                            )
                            raise ArcanaDataTreeConstructionError(
                                f"Provided ID-pattern component,'{regex}', needs to match "
                                f"exactly one group on '{attr_name}' attribute of "
                                f"'{ids[source_freq]}' {source_freq} row, '{attr}', when it "
                                + match_msg
                            )
                        attr = match.group(1)
                    substitutions.append(attr)
                if whole_str:
                    assert len(substitutions) == 1
                    inferred_id = substitutions[0]
                else:
                    inferred_id = pattern
                    for sub in substitutions:
                        inferred_id = cls.pattern_comp_re.subn(
                            sub, inferred_id, count=1
                        )[0]
                inferred_ids[freq] = inferred_id
        return inferred_ids

    def get_site_license_file(self, name: str, **kwargs) -> PlainText:
        """Access the site-wide license file

        Parameters
        ----------
        name : str
            name of the license
        user : str, optional
            the site-licenses user, by default None
        password : str, optional
            the site-licenses password, by default None

        Returns
        -------
        PlainText
            the license file
        """
        return self.site_licenses_dataset(**kwargs).get_license_file(name)

    def __bytes_repr__(self, cache):
        """Bytes representation of store to be used in Pydra input hashing"""
        yield type(self).__module__.encode()
        yield type(self).__name__.encode()

    ####################
    # Abstract methods #
    ####################

    @abstractmethod
    def populate_tree(self, tree: DataTree):
        """
        Populates the nodes of the data tree with those found in the dataset using
        the ``DataTree.add_leaf`` method for every "leaf" node of the dataset tree.

        The order that the tree leaves are added is important and should be consistent
        between reads, because it is used to give default values to the ID's of data
        space axes not explicitly in the hierarchy of the tree.

        Parameters
        ----------
        tree : DataTree
            The tree to populate with nodes
        """

    @abstractmethod
    def populate_row(self, row: DataRow):
        """
        Populate a row with all data entries found in the corresponding node in the data
        store (e.g. files within a directory, scans within an XNAT session) using the
        ``DataRow.add_entry`` method. Within a node/row there are assumed to be two types
        of entries, "primary" entries (e.g. acquired scans) common to all analyses performed
        on the dataset and "derivative" entries corresponding to intermediate outputs
        of previously performed analyses. These types should be stored in separate
        namespaces so there is no chance of a derivative overriding a primary data item.

        The name of the dataset/analysis a derivative was generated by is appended to
        to a base path, delimited by "@", e.g. "brain_mask@my_analysis". The dataset
        name is left blank by default, in which case "@" is just appended to the
        derivative path, i.e. "brain_mask@".

        Parameters
        ----------
        row : DataRow
            The row to populate with entries
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
    def put_provenance(self, provenance: ty.Dict[str, ty.Any], entry: DataEntry):
        """Stores provenance information for a given data item in the store

        Parameters
        ----------
        entry: DataEntry
            The item to store the provenance data for
        provenance: ty.Dict[str, Any]
            The provenance data to store
        """

    @abstractmethod
    def get_provenance(self, entry: DataEntry) -> ty.Dict[str, ty.Any]:
        """Stores provenance information for a given data item in the store

        Parameters
        ----------
        entry: DataEntry
            The item to store the provenance data for

        Returns
        -------
        provenance: ty.Dict[str, Any] or None
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
        definition: ty.Dict[str, Any]
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
        definition: ty.Dict[str, Any]
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
        leaves: ty.List[ty.Tuple[str, ...]],
        hierarchy: ty.List[str],
        space: type,
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
        hierarchy: ty.List[str]
            the hierarchy of the dataset to be created
        space : type(DataSpace)
            the data space of the dataset
        id_patterns : dict[str, str]
            Patterns for inferring IDs of rows not explicitly present in the hierarchy of
            the data tree. See ``DataStore.infer_ids()`` for syntax
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

    pattern_comp_re = re.compile(r"#[^\#]+#")
