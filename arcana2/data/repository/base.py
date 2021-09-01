import logging
from abc import abstractmethod, ABCMeta
import attr
from arcana2.exceptions import ArcanaUsageError
from ..dataset import Dataset


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

    def standardise_name(self, name):
        return name

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

    def dataset(self, name, selectors, derivatives=None, structure=None,
                **kwargs):
        """
        Returns a dataset from the XNAT repository

        Parameters
        ----------
        name : str
            The name, path or ID of the dataset within the repository
        selectors : Dict[Str, DataSelector]
            A dictionary that maps "name-paths" of input "columns" in the
            dataset to criteria in a Selector object that select the
            corresponding items in the dataset
        derivatives : Dict[str, Spec]
            A dictionary that maps "name-paths" of derivatives analysis workflows
            to be stored in the dataset
        structure : Enum
            The DataFrequency enum that defines the frequencies (e.g.
            per-session, per-subject,...) present in the dataset.                       
        **kwargs:
            Keyword args passed on to the Dataset init method
        """
        if not structure:
            try:
                structure = self.DEFAULT_FREQUENCY_ENUM
            except AttributeError:
                raise ArcanaUsageError(
                    "'structure' kwarg must be specified for datasets in "
                    f"{type(self)} repositories")
        return Dataset(name, repository=self, selectors=selectors,
                       derivatives=derivatives, structure=structure, **kwargs)

    @abstractmethod
    def populate_tree(self, dataset, ids=None, **kwargs):
        """
        Find all data within a repository, registering file_groups, fields and
        provenance with the found_file_group, found_field and found_provenance
        methods, respectively

        Parameters
        ----------
        dataset : Dataset
            The dataset to return the data for
        ids : Dict[DataFrequency, str]
            List of subject IDs with which to filter the tree with. If
            None all are returned
        """

    @abstractmethod
    def get_file_group_paths(self, file_group):
        """
        Cache the file_group locally if required

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
        Extract the value of the field from the repository

        Parameters
        ----------
        field : Field
            The field to retrieve the value for

        Returns
        -------
        value : int | float | str | list[int] | list[float] | list[str]
            The value of the Field
        """

    def get_checksums(self, file_group):
        """
        Returns the checksums for the files in the file_group that are stored in
        the repository. If no checksums are stored in the repository then this
        method should be left to return None and the checksums will be
        calculated by downloading the files and taking calculating the digests

        Parameters
        ----------
        file_group : FileGroup
            The file_group to return the checksums for

        Returns
        -------
        checksums : dct[str, str]
            A dictionary with keys corresponding to the relative paths of all
            files in the file_group from the base path and values equal to the MD5
            hex digest. The primary file in the file-set (i.e. the one that the
            path points to) should be specified by '.'.
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
