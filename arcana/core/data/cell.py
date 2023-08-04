from __future__ import annotations
import typing as ty
import attrs
from fileformats.core import DataType
from arcana.core.exceptions import ArcanaError

if ty.TYPE_CHECKING:  # pragma: no cover
    from .row import DataRow
    from .column import DataColumn
    from .entry import DataEntry


@attrs.define
class DataCell:
    """
    An abstract container representing the intersection between a row and a column,
    which points to an item within a dataset.

    Parameters
    ----------
    row : DataRow
        the row the cell belongs to
    column : DataColumn
        the column the cell belongs to
    entry : DataEntry or None
        the entry in the row that matches the column
    provenance : Provenance | None
        The recorded provenance for the item stored within the data cell,
        if applicable
    """

    row: DataRow
    column: DataColumn
    entry: DataEntry
    provenance: ty.Dict[str, ty.Any] = None

    @property
    def datatype(self):
        return self.column.datatype

    @property
    def is_empty(self):
        return self.entry is None

    @property
    def item(self) -> DataType:
        if self.is_empty:
            raise ArcanaError(f"Cannot access item of empty cell, {self}")
        return self.entry.get_item(self.column.datatype)

    @item.setter
    def item(self, item: DataType):
        if not self.column.is_sink:
            raise ArcanaError(
                f"Cannot set data items ({item}) into source column cell {self}"
            )
        item = self.datatype(item)
        if self.is_empty:
            entry = self.row.dataset.store.post(
                item=item, path=self.column.path, datatype=self.datatype, row=self.row
            )
            self.entry = entry
        else:
            self.entry.item = item

    @classmethod
    def intersection(
        cls, column: DataColumn, row: DataRow, allow_empty: ty.Optional[bool] = None
    ) -> DataCell:
        if allow_empty is None:
            allow_empty = column.is_sink
        return cls(
            row=row,
            column=column,
            entry=column.match_entry(row, allow_none=allow_empty),
        )
