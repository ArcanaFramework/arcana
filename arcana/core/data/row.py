from __future__ import annotations
from pathlib import Path
import typing as ty
import attrs
from collections import defaultdict
from abc import ABCMeta
from arcana.core.exceptions import (
    ArcanaNameError,
    ArcanaWrongFrequencyError,
    ArcanaFileFormatError,
)
from .type.base import DataType
from ..analysis.salience import DataQuality
from .space import DataSpace

if ty.TYPE_CHECKING:
    import arcana.core.data.set


@attrs.define(auto_detect=True)
class DataRow:
    """A "row" in a dataset "frame" where file-groups and fields can be placed, e.g.
    a session or subject.

    Parameters
    ----------
    ids : Dict[DataSpace, str]
        The ids for the frequency of the row and all "parent" frequencies
        within the tree
    frequency : DataSpace
        The frequency of the row
    dataset : Dataset
        A reference to the root of the data tree
    """

    ids: ty.Dict[DataSpace, str] = attrs.field()
    frequency: DataSpace = attrs.field()
    dataset: arcana.core.data.set.Dataset = attrs.field(repr=False)
    children: ty.DefaultDict[
        DataSpace, ty.Dict[ty.Union[str, ty.Tuple[str]], str]
    ] = attrs.field(factory=lambda: defaultdict(dict), repr=False)
    _unresolved = attrs.field(default=None, repr=False)
    _items = attrs.field(factory=dict, init=False, repr=False)

    def __getitem__(self, column_name):
        """Gets the item for the current row

        Parameters
        ----------
        column_name : str
            Name of a selected column in the dataset

        Returns
        -------
        DataType
            The item matching the provided name specified by the column name
        """
        if column_name in self._items:
            return self._items[column_name]
        else:
            try:
                spec = self.dataset[column_name]
            except KeyError as e:
                raise ArcanaNameError(
                    column_name,
                    f"{column_name} is not the name of a column in "
                    f"{self.dataset.id} dataset ('"
                    + "', '".join(self.dataset.columns)
                    + "')",
                ) from e
            if spec.row_frequency != self.frequency:
                return ArcanaWrongFrequencyError(
                    column_name,
                    f"'column_name' ({column_name}) is of {spec.row_frequency} "
                    f"frequency and therefore not in rows of {self.frequency}"
                    " frequency",
                )
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
        return (
            (c.name, self[c.name])
            for c in self.dataset.columns.values()
            if c.row_frequency == self.frequency
        )

    def column_items(self, column_name):
        """Gets the item for the current row if item's frequency matches
        otherwise gets all the items that are related to the current row (
        i.e. are in child rows)

        Parameters
        ----------
        column_name : str
            Name of a selected column in the dataset

        Returns
        -------
        Sequence[DataType]
            The item matching the provided name specified by the column name
            if the column is of matching or ancestor frequency, or list of
            items if a descendent or unrelated frequency.
        """
        try:
            return [self[column_name]]
        except ArcanaWrongFrequencyError:
            # If frequency is not a ancestor row then return the
            # items in the children of the row (if they are child
            # rows) or the whole dataset
            spec = self.dataset.columns[column_name]
            try:
                return self.children[spec.row_frequency].values()
            except KeyError:
                return self.dataset.column(spec.row_frequency)

    @property
    def unresolved(self):
        if self._unresolved is None:
            self._unresolved = []
            self.dataset.store.find_items(self)
        return self._unresolved

    def resolved(self, datatype):
        """
        Items in the row that are able to be resolved to the given datatype

        Parameters
        ----------
        datatype : type
            The file datatype or type to reolve the item to
        """
        matches = []
        for potential in self.unresolved:
            try:
                matches.append(datatype.resolve(potential))
            except ArcanaFileFormatError:
                pass
        return matches

    @property
    def ids_tuple(self):
        return self.dataset.ids_tuple(self.ids)

    def add_file_group(self, path, **kwargs):
        if self._unresolved is None:
            self._unresolved = []
        self._unresolved.append(UnresolvedFileGroup(path=path, row=self, **kwargs))

    def add_field(self, path, value, **kwargs):
        if self._unresolved is None:
            self._unresolved = []
        self._unresolved.append(
            UnresolvedField(path=path, row=self, value=value, **kwargs)
        )


@attrs.define
class UnresolvedDataType(metaclass=ABCMeta):
    """A file-group stored in, potentially multiple, unknown file formats.
    File formats are resolved by providing a list of candidates to the
    'resolve' method

    Parameters
    ----------
    path : str
        The name_path to the relative location of the file group, i.e. excluding
        information about which row in the data tree it belongs to
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

    path: str = attrs.field(default=None)
    row: DataRow = attrs.field(default=None)
    order: int = attrs.field(default=None)
    quality: DataQuality = attrs.field(default=DataQuality.usable)
    provenance: ty.Dict[str, ty.Any] = attrs.field(default=None)
    _matched: ty.Dict[str, DataType] = attrs.field(factory=dict, init=False)

    @property
    def item_kwargs(self):
        return {
            "path": self.path,
            "order": self.order,
            "row": self.row,
            "quality": self.quality,
        }


def normalise_paths(file_paths):
    "Convert all file paths to absolute real paths"
    if file_paths:
        file_paths = [Path(p).absolute() for p in file_paths]
    return file_paths


@attrs.define
class UnresolvedFileGroup(UnresolvedDataType):
    """A file-group stored in, potentially multiple, unknown file formats.
    File formats are resolved by providing a list of candidates to the
    'resolve' method

    Parameters
    ----------
    name_path : str
        The name_path to the relative location of the file group, i.e. excluding
        information about which row in the data tree it belongs to
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
    row : DataRow
        The data row that the field belongs to
    file_paths : Sequence[str] | None
        Path to the file-group in the local cache
    uris : Dict[str, str] | None
        For stores where the name of the file datatype is saved with the
        data (i.e. XNAT), the name of the resource enables straightforward
        datatype identification. It is stored here along with URIs corresponding
        to each resource
    """

    file_paths: ty.Sequence[Path] = attrs.field(factory=list, converter=normalise_paths)
    uris: ty.Dict[str] = attrs.field(default=None)

    @classmethod
    def from_paths(cls, base_dir: Path, paths: ty.List[Path], **kwargs):
        groups = defaultdict(list)
        for path in paths:
            relpath = path.relative_to(base_dir)
            path_stem = str(relpath)[: -len("".join(relpath.suffixes))]
            groups[path_stem].append(path)  # No extension case
            # Add all possible stems
            for i in range(len(relpath.suffixes)):
                groups["".join([path_stem] + relpath.suffixes[: (i + 1)])].append(path)
        return [cls(path=p, file_paths=g, **kwargs) for p, g in groups.items()]


@attrs.define
class UnresolvedField(UnresolvedDataType):
    """A file-group stored in, potentially multiple, unknown file formats.
    File formats are resolved by providing a list of candidates to the
    'resolve' method

    Parameters
    ----------
    path : str
        The name_path to the relative location of the file group, i.e. excluding
        information about which row in the data tree it belongs to
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
    row : DataRow
        The data row that the field belongs to
    """

    value: ty.Union[
        float, int, str, ty.List[float], ty.List[int], ty.List[str]
    ] = attrs.field(default=None)

    # def _resolve(self, datatype):
    #     try:
    #         if datatype._name == 'Sequence':
    #             if len(datatype.__args__) > 1:
    #                 raise ArcanaUsageError(
    #                     f"Sequence formats with more than one arg "
    #                     "are not supported ({datatype})")
    #             subtype = datatype.__args__[0]
    #             value = [subtype(v)
    #                         for v in self.value[1:-1].split(',')]
    #         else:
    #             value = datatype(self.value)
    #     except ValueError as e:
    #         raise ArcanaUnresolvableFormatException(
    #                 f"Could not convert value of {self} ({self.value}) "
    #                 f"to datatype {datatype}") from e
    #     else:
    #         item = DataType(value=value, **self.item_kwargs)
    #     return item
