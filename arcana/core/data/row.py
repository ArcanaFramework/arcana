from __future__ import annotations
import typing as ty
import attrs
from arcana.core.exceptions import (
    ArcanaNameError,
    ArcanaWrongFrequencyError,
)
from fileformats.core import DataType
from .quality import DataQuality
from .space import DataSpace
from .cell import DataCell
from .entry import DataEntry


if ty.TYPE_CHECKING:  # pragma: no cover
    from .set.base import Dataset


@attrs.define(kw_only=True)
class DataRow:
    """A "row" in a dataset "frame" where file-sets and fields can be placed, e.g.
    a session or subject.

    Parameters
    ----------
    ids : Dict[DataSpace, str]
        The ids for the frequency of the row and all "parent" frequencies
        within the tree
    dataset : Dataset
        A reference to the root of the data tree
    frequency : str
        The frequency of the row
    tree_path : list[str], optional
        the path to the row within the data tree. None if the row doesn't sit within
        the original tree (e.g. timepoints within a subject>session hierarchy)
    uri : str, optional
        a URI for the row, can be set and used by the data store implementation if
        appropriate, by default None
    """

    ids: ty.Dict[DataSpace, str] = attrs.field()
    dataset: Dataset = attrs.field(repr=False)
    frequency: str = attrs.field()
    tree_path: ty.List[str] = None
    uri: ty.Optional[str] = None
    metadata: ty.Optional[dict] = None

    # Automatically populated fields
    children: ty.Dict[
        DataSpace, ty.Dict[ty.Union[str, ty.Tuple[str]], str]
    ] = attrs.field(factory=dict, repr=False, init=False)
    _entries_dict: ty.Dict[str, DataEntry] = attrs.field(
        default=None, init=False, repr=False
    )
    _cells: ty.Dict[str, DataCell] = attrs.field(factory=dict, init=False, repr=False)

    @dataset.validator
    def dataset_validator(self, _, dataset):
        from .set import Dataset

        if not isinstance(dataset, Dataset):
            raise ValueError(f"provided dataset {dataset} is not of type {Dataset}")

    @frequency.validator
    def frequency_validator(self, _, frequency):
        if frequency not in self.dataset.space:
            raise ValueError(
                f"'{frequency}' frequency is not in the data space of the dataset, "
                f"{self.dataset.space}"
            )

    def __attrs_post_init__(self):
        if isinstance(self.frequency, str):
            self.frequency = self.dataset.space[self.frequency]

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

    def cell(self, column_name: str, allow_empty: ty.Optional[bool] = None) -> DataCell:
        try:
            cell = self._cells[column_name]
        except KeyError:
            pass
        else:
            if not cell.is_empty:
                return cell
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

    def cells(self, allow_empty: ty.Optional[bool] = None) -> ty.Iterable[DataCell]:
        for column_name in self.dataset.columns:
            yield self.cell(column_name, allow_empty=allow_empty)

    @property
    def entries(self) -> ty.Iterable[DataEntry]:
        return self.entries_dict.values()

    def entry(self, id: str) -> DataEntry:
        return self.entries_dict[id]

    @property
    def entries_dict(self):
        if self._entries_dict is None:
            self._entries_dict = {}
            self.dataset.store.populate_row(self)
        return self._entries_dict

    def __repr__(self):
        return f"{type(self).__name__}(id={self.id}, frequency={self.frequency})"

    @property
    def id(self):
        return self.ids[self.frequency]

    @property
    def ids_tuple(self):
        return tuple(self.ids[a] for a in self.dataset.space.axes())

    @property
    def label(self):
        return self.tree_path[-1]

    def frequency_id(self, frequency: ty.Union[str, DataSpace]):
        return self.ids[self.dataset.space[str(frequency)]]

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

    def add_entry(
        self,
        path: str,
        datatype: type,
        uri: str,
        item_metadata: ty.Optional[dict] = None,
        order: ty.Optional[int] = None,
        quality: DataQuality = DataQuality.usable,
        checksums: ty.Dict[str, str] = None,
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
