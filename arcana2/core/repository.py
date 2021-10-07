import logging
from abc import abstractmethod, ABCMeta
import attr
from arcana2.exceptions import ArcanaUsageError
from .data import set as dataset


logger = logging.getLogger('arcana')

@attr.s
class Repository(metaclass=ABCMeta):
    """
    Abstract base class for all Repository systems, DaRIS, XNAT and
    local file system. Sets out the interface that all Repository
    classes should implement.

    Parameters

    """

    _connection_depth = attr.ib(default=0, init=False)

    def __enter__(self):
        # This allows the repository to be used within nested contexts
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
            The name, path or ID of the dataset within the repository
        sources : Dict[Str, DataSource]
            A dictionary that maps "name-paths" of input "columns" in the
            dataset to criteria in a Selector object that select the
            corresponding items in the dataset
        sinks : Dict[str, Spec]
            A dictionary that maps "name-paths" of sinks analysis
            workflows to be stored in the dataset
        dimensions : EnumMeta
            The DataDimension enum that defines the frequencies (e.g.
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
                    f"{type(self)} repositories")
        return dataset.Dataset(name,
                               repository=self,
                               hierarchy=hierarchy,
                               **kwargs)

    @abstractmethod
    def find_nodes(self, dataset):
        """
        Find all data nodes for a dataset in the repository and populate the
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
    def get_file_group_paths(self, file_group):
        """
        Cache the file_group locally (if required) and return the locations
        of the cached primary file and side cars

        Parameters
        ----------
        file_group : FileGroup
            The file_group to cache locally

        Returns
        -------
        path : str
            The file-system path to the cached file
        side_cars : Dict[str, str] or None
            The file-system paths to the cached side-cars if present
        """

    @abstractmethod
    def get_field_value(self, field):
        """
        Extract and return the value of the field from the repository

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
    def put_file_group(self, file_group):
        """
        Inserts or updates the file_group into the repository

        Parameters
        ----------
        file_group : FileGroup
            The file_group to insert into the repository
        """

    @abstractmethod
    def put_field(self, field):
        """
        Inserts or updates the fields into the repository

        Parameters
        ----------
        field : Field
            The field to insert into the repository
        """

    def get_checksums(self, file_group):
        """
        Returns the checksums for the files in the file_group that are stored
        in the repository. If no checksums are stored in the repository then
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
        return None

    def connect(self):
        """
        If a connection session is required to the repository,
        manage it here
        """

    def disconnect(self):
        """
        If a connection session is required to the repository,
        manage it here
        """
