from __future__ import annotations
import typing as ty
import attrs
from arcana.core.exceptions import (
    NameError,
    DataNotDerivedYetError,
)
from .quality import DataQuality  # @ignore reshadowedImports

if ty.TYPE_CHECKING:
    from .row import DataRow
    from .column import DataColumn


@attrs.define
class DataCell:
    """
    An abstract container representing the intersection between a row and a column,
    which points to an item within a dataset.

    Parameters
    ----------
    id : str
        The id used to locate the cell within a data row
    row : DataRow
        The row the cell belongs to
    column : DataColumn
        The column the cell belongs to
    is_empty : bool
        Whether the cell refers to an existing data item in the dataset or not. For example,
        a cell is empty when the cell is just a placeholder for a derivative data item
        that hasn't been created yet.
    order : int | None
        The order in which the data cell appears in the row it belongs to
        (starting at 0). Typically corresponds to the acquisition order for
        scans within an imaging session. Can be used to distinguish between
        scans with the same series description (e.g. multiple BOLD or T1w
        scans) in the same imaging sessions.
    quality : str
        The quality label assigned to the fileset (e.g. as is saved on XNAT)
    provenance : Provenance | None
        The recorded provenance for the item stored within the data cell,
        if applicable
    uri : str
        a universal resource identifier, which can be used by in DataStore implementation
        logic to conveniently access the cells contents
    """

    id: str = attrs.field()
    row: DataRow = attrs.field()
    column: DataColumn = attrs.field()
    is_empty: bool = False
    order: int = attrs.field(default=None)
    quality: DataQuality = attrs.field(default=DataQuality.usable)
    provenance: ty.Dict[str, ty.Any] = attrs.field(default=None)
    uri: str = attrs.field(default=None)

    @property
    def recorded_checksums(self):
        if self.provenance is None:
            return None
        else:
            return self.provenance.outputs[self.name_path]

    @provenance.validator
    def check_provenance(self, _, provenance):
        "Checks that the data item path is present in the provenance"
        if provenance is not None:
            if self.path not in provenance.outputs:
                raise NameError(
                    self.path,
                    f"{self.path} was not found in outputs "
                    f"{provenance.outputs.keys()} of provenance provenance "
                    f"{provenance}",
                )

    def _check_exists(self):
        if not self.exists:
            raise DataNotDerivedYetError(
                self.path, f"Cannot access {self} as it hasn't been derived yet"
            )

    def _check_part_of_row(self):
        if self.row is None:
            raise RuntimeError(f"Cannot 'get' {self} as it is not part of a dataset")

    @classmethod
    def class_name(cls):
        return cls.__name__.lower()
