import logging
from abc import abstractmethod, ABCMeta
import attr
from arcana.exceptions import ArcanaUsageError
from . import set as set_module


logger = logging.getLogger('arcana')

@attr.s
class DataStore(metaclass=ABCMeta):
    """
    Abstract base class for all Repository systems, DaRIS, XNAT and
    local file system. Sets out the interface that all Repository
    classes should implement.

    Parameters

    """

    _connection_depth = attr.ib(default=0, init=False, hash=False, repr=False,
                                eq=False)

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

    def dataset(self, name, hierarchy=None, **kwargs):
        """
        Returns a dataset from the XNAT repository

        Parameters
        ----------
        name : str
            The name, path or ID of the dataset within the store
        sources : Dict[Str, DataSource]
            A dictionary that maps "name-paths" of input "columns" in the
            dataset to criteria in a Selector object that select the
            corresponding items in the dataset
        sinks : Dict[str, Spec]
            A dictionary that maps "name-paths" of sinks analysis
            workflows to be stored in the dataset
        dimensions : EnumMeta
            The DataDimensions enum that defines the frequencies (e.g.
            per-session, per-subject,...) present in the dataset.                       
        **kwargs:
            Keyword args passed on to the Dataset init method
        """
        if not hierarchy:
            try:
                hierarchy = self.DEFAULT_HIERARCHY
            except AttributeError:
                raise ArcanaUsageError(
                    "'hierarchy' kwarg must be specified for datasets in "
                    f"{type(self)} stores")
        return set_module.Dataset(name,
                                  store=self,
                                  hierarchy=hierarchy,
                                  **kwargs)

    @abstractmethod
    def find_nodes(self, dataset):
        """
        Find all data nodes for a dataset in the store and populate the
        Dataset object using its `add_node` method.

        Parameters
        ----------
        dataset : Dataset
            The dataset to populate with nodes
        """

    def find_items(self, data_node):
        """
        Find all data items within a data node and populate the DataNode object
        with them using the `add_file_group` and `add_field` methods.
        
        Parameters
        ----------
        data_node : DataNode
            The data node to populate with items
        """        

    @abstractmethod
    def get_file_group(self, file_group, cache_only=False):
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
        path : str
            The file-system path to the cached file
        side_cars : Dict[str, str] or None
            The file-system paths to the cached side-cars if present

        Raises
        ------
        ArcanaCacheError
            If cache_only is set and there is a mismatch between the cached
            and remote versions
        """

    @abstractmethod
    def get_field(self, field):
        """
        Extract and return the value of the field from the store

        Parameters
        ----------
        field : Field
            The field to retrieve the value for

        Returns
        -------
        value : int | float | str | list[int] | list[float] | list[str]
            The value of the Field
        """

    @abstractmethod
    def put_file_group(self, file_group, fs_path, side_cars):
        """
        Inserts or updates the file_group into the store

        Parameters
        ----------
        file_group : FileGroup
            The file_group to insert into the store
        """

    @abstractmethod
    def put_field(self, field, value):
        """
        Inserts or updates the fields into the store

        Parameters
        ----------
        field : Field
            The field to insert into the store
        """

    def get_checksums(self, file_group):
        """
        Returns the checksums for the files in the file_group that are stored
        in the store. If no checksums are stored in the store then
        this method should be left to return None and the checksums will be
        calculated by downloading the files and taking calculating the digests

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
        If a connection session is required to the store,
        manage it here
        """

    def disconnect(self):
        """
        If a connection session is required to the store,
        manage it here
        """

    # @abstractmethod
    # def save(self, dataset):
    #     """Save metadata associated with the dataset in the store"""


    # @abstractmethod
    # def load(self, dataset):
    #     """Load metadata associated with the dataset in the store"""
