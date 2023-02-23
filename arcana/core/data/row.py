from __future__ import annotations
import typing as ty
import attrs
from collections import defaultdict
from arcana.core.exceptions import (
    ArcanaNameError,
    ArcanaWrongFrequencyError,
)
from fileformats.core import DataType
from .quality import DataQuality
from .space import DataSpace
from .cell import DataCell
from .entry import DataEntry


if ty.TYPE_CHECKING:
    from .set.base import Dataset


@attrs.define(auto_detect=True)
class DataRow:
    """A "row" in a dataset "frame" where file-sets and fields can be placed, e.g.
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
    uri : str, optional
        a URI for the row, can be set and used by the data store implementation if
        appropriate, by default None
    """

    ids: ty.Dict[DataSpace, str] = attrs.field()
    frequency: DataSpace = attrs.field()
    dataset: Dataset = attrs.field(repr=False)
    uri: str = None
    metadata: dict = None

    # Automatically populated fields
    children: ty.DefaultDict[
        DataSpace, dict[ty.Union[str, tuple[str]], str]
    ] = attrs.field(factory=lambda: defaultdict(dict), repr=False, init=False)
    _entries_dict: dict[str, DataEntry] = attrs.field(
        default=None, init=False, repr=False
    )
    _cells: dict[str, DataCell] = attrs.field(factory=dict, init=False, repr=False)

    @dataset.validator
    def dataset_validator(self, _, dataset):
        from .set import Dataset

        if not isinstance(dataset, Dataset):
            raise ValueError(f"provided dataset {dataset} is not of type {Dataset}")

    def __getitem__(self, column_name: str) -> DataType:
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
        return self.cell(column_name).item

    def __setitem__(self, column_name: str, value: DataType):
        self.cell(column_name).item = value
        return self

    def entry(self, id: str) -> DataEntry:
        return self._entries_dict[id]

    def cell(self, column_name: str, allow_empty: bool = None) -> DataCell:
        try:
            return self._cells[column_name]
        except KeyError:
            pass
        try:
            column = self.dataset[column_name]
        except KeyError as e:
            raise ArcanaNameError(
                column_name,
                f"{column_name} is not the name of a column in "
                f"{self.dataset.id} dataset ('"
                + "', '".join(self.dataset.columns)
                + "')",
            ) from e
        if column.row_frequency != self.frequency:
            return ArcanaWrongFrequencyError(
                column_name,
                f"'column_name' ({column_name}) is of {column.row_frequency} "
                f"frequency and therefore not in rows of {self.frequency}"
                " frequency",
            )
        cell = DataCell.intersection(column=column, row=self, allow_empty=allow_empty)
        self._cells[column_name] = cell
        return cell

    def cells(self, allow_empty: bool = None) -> ty.Iterable[DataCell]:
        for column_name in self.dataset.columns:
            yield self.cell(column_name, allow_empty=allow_empty)

    @property
    def entries(self) -> ty.Iterable[DataEntry]:
        if self._entries_dict is None:
            self._entries_dict = {}
            self.dataset.store.scan_row(self)
        return self._entries_dict.values()

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
    def ids_tuple(self):
        return self.dataset.ids_tuple(self.ids)

    def add_entry(
        self,
        path: str,
        datatype: type,
        uri: str,
        item_metadata: dict = None,
        order: int = None,
        quality: DataQuality = DataQuality.usable,
        checksums: dict[str, str] = None,
    ):
        """Adds an data entry to a row that has been found while scanning the row in the
        repository.

        Parameters
        ----------
        id : str
            the ID of the entry within the node
        datatype : type (subclass of DataType)
            the type of the data entry
        uri : str
            a URI uniquely identifying the data entry, which can be used by the store
            for convenient/efficient access to the entry. Note that newly added entries
            (i.e. created by data-sink columns) will not have their URI set, so data
            store logic should fallback to using the row+id to identify the entry in
            the dataset.
        item_metadata : dict[str, Any]
            metadata associated with the data item itself (e.g. pulled from a file header).
            Can be supplied either when the entry is initialised (i.e. from previously
            extracted fields stored within the data store), or read from the item itself.
        order : int, optional
            the order in which the entry appears in the node (where applicable)
        provenance : dict, optional
            the provenance associated with the derivation of the entry by Arcana
            (only applicable to derivatives not source data)
        checksums : dict[str, str], optional
            checksums for all of the files in the data entry
        """
        if self._entries_dict is None:
            self._entries_dict = {}
        entry = DataEntry(
            path=path,
            datatype=datatype,
            row=self,
            uri=uri,
            item_metadata=item_metadata,
            order=order,
            quality=quality,
            checksums=checksums,
        )
        if path in self._entries_dict:
            raise KeyError(
                f"Attempting to add multiple entries with the same path, '{path}', to "
                f"{self}, {self._entries_dict[path]} and {entry}"
            )
        self._entries_dict[path] = entry
        return entry


# @attrs.define
# class UnresolvedDataType(metaclass=ABCMeta):
#     """A file-set stored in, potentially multiple, unknown file formats.
#     File formats are resolved by providing a list of candidates to the
#     'resolve' method

#     Parameters
#     ----------
#     path : str
#         The name_path to the relative location of the file set, i.e. excluding
#         information about which row in the data tree it belongs to
#     order : int | None
#         The ID of the fileset in the session. To be used to
#         distinguish multiple filesets with the same scan type in the
#         same session, e.g. scans taken before and after a task. For
#         datasets where this isn't stored (i.e. Local), id can be None
#     quality : DataQuality
#         The quality label assigned to the fileset (e.g. as is saved on XNAT)
#     provenance : Provenance | None
#         The provenance for the pipeline that generated the file-set,
#         if applicable
#     """

#     path: str = attrs.field(default=None)
#     row: DataRow = attrs.field(default=None)
#     order: int = attrs.field(default=None)
#     quality: DataQuality = attrs.field(default=DataQuality.usable)
#     provenance: ty.Dict[str, ty.Any] = attrs.field(default=None)
#     _matched: ty.Dict[str, DataType] = attrs.field(factory=dict, init=False)

#     @property
#     def item_kwargs(self):
#         return {
#             "path": self.path,
#             "order": self.order,
#             "row": self.row,
#             "quality": self.quality,
#         }


# def normalise_paths(file_paths):
#     "Convert all file paths to absolute real paths"
#     if file_paths:
#         file_paths = [Path(p).absolute() for p in file_paths]
#     return file_paths


# @attrs.define
# class UnresolvedFileSet(UnresolvedDataType):
#     """A file-set stored in, potentially multiple, unknown file formats.
#     File formats are resolved by providing a list of candidates to the
#     'resolve' method

#     Parameters
#     ----------
#     name_path : str
#         The name_path to the relative location of the file set, i.e. excluding
#         information about which row in the data tree it belongs to
#     order : int | None
#         The ID of the fileset in the session. To be used to
#         distinguish multiple filesets with the same scan type in the
#         same session, e.g. scans taken before and after a task. For
#         datasets where this isn't stored (i.e. Local), id can be None
#     quality : DataQuality
#         The quality label assigned to the fileset (e.g. as is saved on XNAT)
#     provenance : Provenance | None
#         The provenance for the pipeline that generated the file-set,
#         if applicable
#     row : DataRow
#         The data row that the field belongs to
#     file_paths : Sequence[str] | None
#         Path to the file-set in the local cache
#     uris : Dict[str, str] | None
#         For stores where the name of the file datatype is saved with the
#         data (i.e. XNAT), the name of the resource enables straightforward
#         datatype identification. It is stored here along with URIs corresponding
#         to each resource
#     """

#     file_paths: ty.Sequence[Path] = attrs.field(factory=list, converter=normalise_paths)
#     uris: ty.Dict[str] = attrs.field(default=None)

#     @classmethod
#     def from_paths(cls, base_dir: Path, paths: ty.List[Path], **kwargs):
#         groups = defaultdict(list)
#         for path in paths:
#             relpath = path.relative_to(base_dir)
#             path_stem = str(relpath)[: -len("".join(relpath.suffixes))]
#             groups[path_stem].append(path)  # No extension case
#             # Add all possible stems
#             for i in range(len(relpath.suffixes)):
#                 groups["".join([path_stem] + relpath.suffixes[: (i + 1)])].append(path)
#         return [cls(path=p, file_paths=g, **kwargs) for p, g in groups.items()]


# @attrs.define
# class UnresolvedField(UnresolvedDataType):
#     """A file-set stored in, potentially multiple, unknown file formats.
#     File formats are resolved by providing a list of candidates to the
#     'resolve' method

#     Parameters
#     ----------
#     path : str
#         The name_path to the relative location of the file set, i.e. excluding
#         information about which row in the data tree it belongs to
#     value : str
#         The value assigned to the unresolved data item (for fields instead of
#         file sets)
#     order : int | None
#         The ID of the fileset in the session. To be used to
#         distinguish multiple filesets with the same scan type in the
#         same session, e.g. scans taken before and after a task. For
#         datasets where this isn't stored (i.e. Local), id can be None
#     quality : DataQuality
#         The quality label assigned to the fileset (e.g. as is saved on XNAT)
#     provenance : Provenance | None
#         The provenance for the pipeline that generated the file-set,
#         if applicable
#     row : DataRow
#         The data row that the field belongs to
#     """

#     value: ty.Union[
#         float, int, str, ty.List[float], ty.List[int], ty.List[str]
#     ] = attrs.field(default=None)

#     # def _resolve(self, datatype):
#     #     try:
#     #         if datatype._name == 'Sequence':
#     #             if len(datatype.__args__) > 1:
#     #                 raise ArcanaUsageError(
#     #                     f"Sequence formats with more than one arg "
#     #                     "are not supported ({datatype})")
#     #             subtype = datatype.__args__[0]
#     #             value = [subtype(v)
#     #                         for v in self.value[1:-1].split(',')]
#     #         else:
#     #             value = datatype(self.value)
#     #     except ValueError as e:
#     #         raise ArcanaUnresolvableFormatException(
#     #                 f"Could not convert value of {self} ({self.value}) "
#     #                 f"to datatype {datatype}") from e
#     #     else:
#     #         item = DataType(value=value, **self.item_kwargs)
#     #     return item
