from __future__ import annotations
import os.path
import typing as ty
import attr
from collections import defaultdict
from abc import ABCMeta, abstractmethod
from arcana2.exceptions import (
    ArcanaUsageError, ArcanaUnresolvableFormatException, ArcanaFileFormatError,
    ArcanaError, ArcanaNameError, ArcanaWrongFrequencyError)
from arcana2.core.utils import split_extension
from ..file_format import FileFormat
from .item import DataItem
from .provenance import DataProvenance
from .enum import DataQuality, DataFrequency
from . import set as dataset


@attr.s
class DataNode():
    """A "node" in a data tree where file-groups and fields can be placed, e.g.
    a session or subject.

    Parameters
    ----------
    ids : Dict[DataFrequency, str]
        The ids for each provided frequency need to specify the data node
        within the tree
    frequency : DataFrequency
        The frequency of the node
    dataset : Dataset
        A reference to the root of the data tree
    """

    dataset: dataset.Dataset = attr.ib()
    ids: ty.Dict[DataFrequency, str] = attr.ib()
    frequency: DataFrequency = attr.ib()
    path: str = attr.ib()
    subnodes: ty.DefaultDict[str, ty.Dict] = attr.ib(
        factory=lambda: defaultdict(dict))
    supranodes: ty.DefaultDict[str, ty.Dict] = attr.ib(factory=dict)
    _unresolved = attr.ib(default=None)
    _items = attr.ib(factory=dict, init=False)

    def __getitem__(self, name):
        """Get's the item that matches the dataset's selector

        Parameters
        ----------
        name : str
            Name of the selected item or derivative, specified per dataset,
            that is used to select a file-group or field in the node

        Returns
        -------
        DataItem
            The item matching the provided name, specified by either a
            selector or derivative registered with the dataset
        """
        try:
            item = self._items[name]
        except KeyError:
            if name in self.dataset.selectors:
                selector = self.dataset.selectors[name]
                frequency = selector.frequency
                item = selector.match(self)
            elif name in self.dataset.derivatives:
                spec = self.dataset.derivatives[name]
                frequency = spec.frequency
                try:
                    # Check to see if derivative was created previously
                    item = spec.match(self)
                except KeyError:
                    # Create new derivative
                    item = spec.create_item(self)
            else:
                raise ArcanaNameError(
                    name,
                    f"'{name}' is not the name of a \"column\" (either as a "
                    f"selected input or derived) in the {self.dataset}")
            if frequency != self.frequency:
                raise ArcanaWrongFrequencyError(
                    name,
                    f"'{name}'' is only present in \"{frequency}\" nodes "
                    f"column where as {self} is of {self.frequency} frequency")
            self._items[name] = item
        return item

    @property
    def dataset(self):
        return self._dataset

    @property
    def items(self):
        return self._items.items()

    @property
    def unresolved(self):
        if self._unresolved is None:
            self.dataset.repository.populate_items(self)
        return self._unresolved

    @property
    def ids_tuple(self):
        return self.dataset.ids_tuple(self.ids)

    def add_file_group(self, path, **kwargs):
        if self._unresolved is None:
            self._unresolved = []
        self._unresolved.append(UnresolvedFileGroup(
            path=path, **kwargs))

    def add_field(self, path, value, **kwargs):
        if self._unresolved is None:
            self._unresolved = []
        self._unresolved.append(UnresolvedField(
            path=path, value=value, **kwargs))

    def get_file_group_paths(self, file_group):
        return self.dataset.repository.get_file_group_paths(file_group, self)

    def get_field_value(self, field):
        return self.dataset.repository.get_field_value(field, self)

    def put_file_group(self, file_group):
        self.dataset.repository.put_file_group(file_group, self)

    def put_field(self, field):
        self.dataset.repository.put_field(field, self)


@attr.s
class UnresolvedDataItem(metaclass=ABCMeta):
    """A file-group stored in, potentially multiple, unknown file formats.
    File formats are resolved by providing a list of candidates to the
    'resolve' method

    Parameters
    ----------
    path : str
        The name_path to the relative location of the file group, i.e. excluding
        information about which node in the data tree it belongs to
    order : int | None
        The ID of the file_group in the session. To be used to
        distinguish multiple file_groups with the same scan type in the
        same session, e.g. scans taken before and after a task. For
        datasets where this isn't stored (i.e. Local), id can be None
    quality : DataQuality
        The quality label assigned to the file_group (e.g. as is saved on XNAT)
    provenance : Provenance | None
        The provenance for the pipeline that generated the file-group,
        if applicable
    """

    path: str = attr.ib()
    order: int = attr.ib(default=None)
    quality: DataQuality = attr.ib(default=DataQuality.usable)
    provenance: DataProvenance = attr.ib(default=None)
    _matched: ty.Dict[str, DataItem] = attr.ib(factory=dict, init=False)

    def resolve(self, data_format, data_node):
        """
        Detects the format of the file-group from a list of possible
        candidates and returns a corresponding FileGroup object. If multiple
        candidates match the potential files, e.g. NiFTI-X (see dcm2niix) and
        NiFTI, then the first matching candidate is selected.

        If 'uris' were specified when the multi-format file-group was
        created then that is used to select between the candidates. Otherwise
        the file extensions of the local name_paths, and extensions of the files
        within the directory will be used instead.

        Parameters
        ----------
        data_format : FileFormat or type
            A list of file-formats to try to match. The first matching format
            in the sequence will be used to create a file-group

        Returns
        -------
        DataItem
            The data item resolved into the requested format

        Raises
        ------
        ArcanaUnresolvableFormatException
            If 
        """
        # If multiple formats are specified via resource names
        
        if not (self.uris or self.file_paths):
            raise ArcanaError(
                "Either uris or local name_paths must be provided "
                f"to UnresolvedFileGroup('{self.name_path}') in before "
                "attempting to resolve a file-groups format")
        try:
            # Attempt to access previously saved
            item = self._matched[format]
        except KeyError:
            if isinstance(data_format, FileFormat):
                item = self._resolve(data_format, data_node)
            else:
                item = self._resolve(data_format, data_node)
        return item

    @abstractmethod
    def _resolve(self, data_format):
        raise NotImplementedError

    @property
    def item_kwargs(self):
        return {
            'path': self.path,
            'frequency': self.frequency,
            'order': self.order,
            'dataset': self.dataset,
            'quality': self.quality}


def normalise_paths(file_paths):
    "Convert all file paths to absolute real paths"
    if file_paths:
        file_paths = [os.path.abspath(os.path.realpath(p)) for p in file_paths]
    return file_paths


@attr.s
class UnresolvedFileGroup(UnresolvedDataItem):
    """A file-group stored in, potentially multiple, unknown file formats.
    File formats are resolved by providing a list of candidates to the
    'resolve' method

    Parameters
    ----------
    name_path : str
        The name_path to the relative location of the file group, i.e. excluding
        information about which node in the data tree it belongs to
    order : int | None
        The ID of the file_group in the session. To be used to
        distinguish multiple file_groups with the same scan type in the
        same session, e.g. scans taken before and after a task. For
        datasets where this isn't stored (i.e. Local), id can be None
    quality : DataQuality
        The quality label assigned to the file_group (e.g. as is saved on XNAT)
    provenance : Provenance | None
        The provenance for the pipeline that generated the file-group,
        if applicable
    data_node : DataNode
        The data node that the field belongs to
    file_paths : Sequence[str] | None
        Path to the file-group in the local cache
    uris : Dict[str, str] | None
        For repositories where the name of the file format is saved with the
        data (i.e. XNAT), the name of the resource enables straightforward
        format identification. It is stored here along with URIs corresponding
        to each resource        
    """

    file_paths: ty.Sequence[str] = attr.ib(factory=list,
                                           converter=normalise_paths)
    uris: ty.Sequence[str] = attr.ib(factory=list, converter=list)

    def _resolve(self, data_format, data_node):
        # Perform matching based on resource names in multi-format
        # file-group
        if self.uris is not None:   
            for data_format_name, uri in self.uris.items():
                if data_format_name in data_format.names:
                    item = data_format(uri=uri, **self.item_kwargs)
            if item is None:
                raise ArcanaUnresolvableFormatException(
                    f"Could not file a matching resource in {self} for"
                    f" the given data_format ({data_format.name}), found "
                    "('{}')".format("', '".join(self.uris)))
        # Perform matching based on file-extensions of local name_paths
        # in multi-format file-group
        else:
            file_path = None
            side_cars = []
            if data_format.directory:
                if (len(self.file_paths) == 1
                    and os.path.isdir(self.file_paths[0])
                    and (data_format.within_dir_exts is None
                        or (data_format.within_dir_exts == frozenset(
                            split_extension(f)[1]
                            for f in os.listdir(self.file_paths)
                            if not f.startswith('.'))))):
                    file_path = self.file_paths[0]
            else:
                try:
                    file_path, side_cars = data_format.assort_files(
                        self.file_paths)[0]
                except ArcanaFileFormatError:
                    pass
            if file_path is not None:
                item = data_format(
                    file_path=file_path, side_cars=side_cars,
                    **self.item_kwargs)
            else:
                raise ArcanaUnresolvableFormatException(
                    f"Paths in {self} (" + "', '".join(self.file_paths) + ") "
                    f"did not match the naming conventions expected by "
                    f"data_format {data_format.name} , found:" + '\n    '.join(self.uris))
        return item


@attr.s
class UnresolvedField(UnresolvedDataItem):
    """A file-group stored in, potentially multiple, unknown file formats.
    File formats are resolved by providing a list of candidates to the
    'resolve' method

    Parameters
    ----------
    path : str
        The name_path to the relative location of the file group, i.e. excluding
        information about which node in the data tree it belongs to
    value : str
        The value assigned to the unresolved data item (for fields instead of 
        file groups)
    order : int | None
        The ID of the file_group in the session. To be used to
        distinguish multiple file_groups with the same scan type in the
        same session, e.g. scans taken before and after a task. For
        datasets where this isn't stored (i.e. Local), id can be None
    quality : DataQuality
        The quality label assigned to the file_group (e.g. as is saved on XNAT)
    provenance : Provenance | None
        The provenance for the pipeline that generated the file-group,
        if applicable
    data_node : DataNode
        The data node that the field belongs to
    """

    value: (int or float or str or ty.Sequence[int] or ty.Sequence[float]
            or ty.Sequence[str]) = attr.ib(default=None)

    def _resolve(self, data_format, data_node):
        try:
            if data_format._name == 'Sequence':
                if len(data_format.__args__) > 1:
                    raise ArcanaUsageError(
                        f"Sequence datatypes with more than one arg "
                        "are not supported ({data_format})")
                subtype = data_format.__args__[0]
                value = [subtype(v)
                            for v in self.value[1:-1].split(',')]
            else:
                    value = data_format(self.value)
        except ValueError:
            raise ArcanaUnresolvableFormatException(
                    f"Could not convert value of {self} ({self.value}) "
                    f"to data_format {data_format}")
        else:
            item = DataItem(value=value, **self.item_kwargs)
        return item
