from __future__ import annotations
from pathlib import Path
import typing as ty
import os
import attr
from collections import defaultdict
from abc import ABCMeta, abstractmethod
import arcana.core.data.set
from arcana.exceptions import (
    ArcanaNameError, ArcanaUsageError, ArcanaUnresolvableFormatException,
    ArcanaWrongFrequencyError, ArcanaFileFormatError, ArcanaError)
from arcana.core.utils import split_extension
from .format import FileFormat
from .item import DataItem
from .provenance import DataProvenance
from ..enum import DataQuality
from .space import DataSpace


@attr.s(auto_detect=True)
class DataNode():
    """A "node" in a data tree where file-groups and fields can be placed, e.g.
    a session or subject.

    Parameters
    ----------
    ids : Dict[DataSpace, str]
        The ids for the frequency of the node and all "parent" frequencies
        within the tree
    frequency : DataSpace
        The frequency of the node
    dataset : Dataset
        A reference to the root of the data tree
    """

    ids: ty.Dict[DataSpace, str] = attr.ib()
    frequency: DataSpace = attr.ib()
    dataset: arcana.core.data.set.Dataset = attr.ib(repr=False)    
    children: ty.DefaultDict[DataSpace,
                             ty.Dict[ty.Union[str, ty.Tuple[str]], str]] = attr.ib(
        factory=lambda: defaultdict(dict), repr=False)
    _unresolved = attr.ib(default=None, repr=False)
    _items = attr.ib(factory=dict, init=False, repr=False)

    def __getitem__(self, column_name):
        """Gets the item for the current node

        Parameters
        ----------
        column_name : str
            Name of a selected column in the dataset

        Returns
        -------
        DataItem
            The item matching the provided name specified by the column name
        """
        if column_name in self._items:
            return self._items[column_name]
        else:
            try:
                spec = self.dataset.column_specs[column_name]
            except KeyError as e:
                raise ArcanaNameError(
                    column_name,
                    f"{column_name} is not the name of a column in "
                    f"{self.dataset.id} dataset ('" + "', '".join(
                        self.dataset.column_specs) + "')") from e
            if spec.frequency != self.frequency:
                return ArcanaWrongFrequencyError(
                    column_name,
                    f"'column_name' ({column_name}) is of {spec.frequency} "
                    f"frequency and therefore not in nodes of {self.frequency}"
                    " frequency")
            item = self._items[column_name] = spec.match(self)
            return item

    def __setitem__(self, column_name, value):
        item = self[column_name]
        item.put(value)
        return item

    def __repr__(self):
        return f"{type(self).__name__}(id={self.id}, frequency={self.frequency})"

    @property
    def id(self):
        return self.ids[self.frequency]

    @property
    def label(self):
        return self.path[-1]

    def __iter__(self):
        return iter(self.keys())

    def keys(self):
        return (n for n, _ in self.items())

    def values(self):
        return (i for _, i in self.items())

    def items(self):
        return ((n, self[n]) for n, s in self.dataset.column_specs.items()
                if s.frequency == self.frequency)

    def column_items(self, column_name):
        """Get's the item for the current node if item's frequency matches
        otherwise gets all the items that are related to the current node (
        i.e. are in child nodes)

        Parameters
        ----------
        column_name : str
            Name of a selected column in the dataset

        Returns
        -------
        Sequence[DataItem]
            The item matching the provided name specified by the column name
            if the column is of matching or ancestor frequency, or list of
            items if a descendent or unrelated frequency.
        """
        try:
            return [self[column_name]]
        except ArcanaWrongFrequencyError:
            # If frequency is not a ancestor node then return the
            # items in the children of the node (if they are child
            # nodes) or the whole dataset
            spec = self.dataset.column_specs[column_name]
            try:
                return self.children[spec.frequency].values()
            except KeyError:
                return self.dataset.column(spec.frequency)

    @property
    def unresolved(self):
        if self._unresolved is None:
            self._unresolved = []
            self.dataset.store.find_items(self)
        return self._unresolved

    def resolved(self, format):
        """
        Items in the node that are able to be resolved to the given format

        Parameters
        ----------
        format : FileFormat or type
            The file format or type to reolve the item to
        """
        matches = []
        for potential in self.unresolved:
            try:
                matches.append(potential.resolve(format))
            except ArcanaUnresolvableFormatException:
                pass
        return matches

    @property
    def ids_tuple(self):
        return self.dataset.ids_tuple(self.ids)

    def add_file_group(self, path, **kwargs):
        if self._unresolved is None:
            self._unresolved = []
        self._unresolved.append(UnresolvedFileGroup(
            path=path, data_node=self, **kwargs))

    def add_field(self, path, value, **kwargs):
        if self._unresolved is None:
            self._unresolved = []
        self._unresolved.append(UnresolvedField(
            path=path, data_node=self, value=value, **kwargs))

    def get_file_group(self, file_group, **kwargs):
        return self.dataset.store.get_file_group(file_group, **kwargs)

    def get_field(self, field):
        return self.dataset.store.get_field(field)

    def put_file_group(self, file_group, fs_path, side_cars):
        self.dataset.store.put_file_group(
            file_group, fs_path=fs_path, side_cars=side_cars)

    def put_field(self, field, value):
        self.dataset.store.put_field(field, value)


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
    data_node: DataNode = attr.ib()
    order: int = attr.ib(default=None)
    quality: DataQuality = attr.ib(default=DataQuality.usable)
    provenance: DataProvenance = attr.ib(default=None)
    _matched: ty.Dict[str, DataItem] = attr.ib(factory=dict, init=False)

    def resolve(self, format):
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
        format : FileFormat or type
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
            if isinstance(format, FileFormat):
                item = self._resolve(format)
            else:
                item = self._resolve(format)
        return item

    @abstractmethod
    def _resolve(self, format):
        raise NotImplementedError

    @property
    def item_kwargs(self):
        return {
            'path': self.path,
            'order': self.order,
            'data_node': self.data_node,
            'quality': self.quality}


def normalise_paths(file_paths):
    "Convert all file paths to absolute real paths"
    if file_paths:
        file_paths = [Path(p).absolute() for p in file_paths]
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
        For stores where the name of the file format is saved with the
        data (i.e. XNAT), the name of the resource enables straightforward
        format identification. It is stored here along with URIs corresponding
        to each resource        
    """

    file_paths: ty.Sequence[Path] = attr.ib(factory=list,
                                            converter=normalise_paths)
    uris: ty.Dict[str] = attr.ib(default=None)

    def _resolve(self, format):
        # Perform matching based on resource names in multi-format
        # file-group
        if self.uris is not None:
            item = None
            for format_name, uri in self.uris.items():
                if format_name.lower() in format.all_names:
                    item = format(uri=uri, **self.item_kwargs)
            if item is None:
                raise ArcanaUnresolvableFormatException(
                    f"Could not file a matching resource in {self.path} for"
                    f" the given format ({format.name}), found "
                    "('{}')".format("', '".join(self.uris)))
        # Perform matching based on file-extensions of local name_paths
        # in multi-format file-group
        else:
            file_path = None
            side_cars = None
            if format.directory:
                if (len(self.file_paths) == 1
                    and self.file_paths[0].is_dir()
                    and (format.within_dir_exts is None
                        or (format.within_dir_exts == frozenset(
                            split_extension(f)[1]
                            for f in self.file_paths[0].iterdir()
                            if not str(f).startswith('.'))))):
                    file_path = self.file_paths[0]
            else:
                try:
                    file_path, side_cars = format.assort_files(
                        self.file_paths)
                except ArcanaFileFormatError:
                    pass
            if file_path is not None:
                item = format(
                    fs_path=file_path, side_cars=side_cars,
                    **self.item_kwargs)
            else:
                raise ArcanaUnresolvableFormatException(
                    f"Paths in {self.path} in node {self.data_node.frequency}:"
                    f"{self.data_node.id} ('" + "', '".join(
                        str(p) for p in self.file_paths) 
                    + "') did not match the naming conventions expected by "
                    f"format '{format.name}'")
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

    value: ty.Union[
        float, int, str, ty.List[float], ty.List[int], ty.List[str]
    ] = attr.ib(default=None)

    def _resolve(self, format):
        try:
            if format._name == 'Sequence':
                if len(format.__args__) > 1:
                    raise ArcanaUsageError(
                        f"Sequence formats with more than one arg "
                        "are not supported ({format})")
                subtype = format.__args__[0]
                value = [subtype(v)
                            for v in self.value[1:-1].split(',')]
            else:
                value = format(self.value)
        except ValueError as e:
            raise ArcanaUnresolvableFormatException(
                    f"Could not convert value of {self} ({self.value}) "
                    f"to format {format}") from e
        else:
            item = DataItem(value=value, **self.item_kwargs)
        return item
