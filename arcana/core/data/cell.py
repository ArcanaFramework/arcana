from __future__ import annotations
import typing as ty
import attrs
from fileformats.core.base import DataType
from arcana.core.exceptions import ArcanaError

if ty.TYPE_CHECKING:
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
    provenance: dict[str, ty.Any] = None

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
        cls, column: DataColumn, row: DataRow, allow_empty: bool = None
    ) -> DataCell:
        if allow_empty is None:
            allow_empty = column.is_sink
        return cls(
            row=row,
            column=column,
            entry=column.match_entry(row, allow_none=allow_empty),
        )

    # @classmethod
    # def resolve(cls, unresolved):
    #     """Resolve file set loaded from a repository to the specific datatype

    #     Parameters
    #     ----------
    #     unresolved : UnresolvedFileSet
    #         A file set loaded from a repository that has not been resolved to
    #         a specific datatype yet

    #     Returns
    #     -------
    #     FileSet
    #         The resolved file-set object

    #     Raises
    #     ------
    #     ArcanaUnresolvableFormatException
    #         If there doesn't exist a unique resolution from the unresolved file
    #         group to the given datatype, then an FileFormatError should be
    #         raised
    #     """
    #     # Perform matching based on resource names in multi-datatype
    #     # file-set
    #     if unresolved.uris is not None:
    #         item = None
    #         for format_name, uri in unresolved.uris.items():
    #             if cls.matches_format_name(format_name):
    #                 item = cls(uri=uri, **unresolved.item_kwargs)
    #         if item is None:
    #             raise FileFormatError(
    #                 f"Could not file a matching resource in {unresolved.path} for"
    #                 f" the given datatype ({cls.class_name()}), found "
    #                 "('{}')".format("', '".join(unresolved.uris))
    #             )
    #     else:
    #         item = cls(**unresolved.item_kwargs)
    #         item.set_fspaths(unresolved.file_paths)
    #     return item

    # def set_fspaths(self, fspaths: list[Path]):
    #     """Set the file paths of the file set

    #     Parameters
    #     ----------
    #     fspaths : list[Path]
    #         The candidate paths from which to set the paths of the
    #         file set from. Note that not all paths need to be set if
    #         they are not relevant.

    #     Raises
    #     ------
    #     FileFormatError
    #         is raised if the required the paths cannot be set from the provided
    #     """

    # @classmethod
    # def from_fspaths(cls, *fspaths: ty.List[Path], path=None):
    #     """Create a FileSet object from a set of file-system paths

    #     Parameters
    #     ----------
    #     fspaths : list[Path]
    #         The candidate paths from which to set the paths of the
    #         file set from. Note that not all paths need to be set if
    #         they are not relevant.
    #     path : str, optional
    #         the location of the file-set relative to the node it (will)
    #         belong to. Defaults to

    #     Returns
    #     -------
    #     FileSet
    #         The created file-set
    #     """
    #     if path is None:
    #         path = fspaths[0].stem
    #     obj = cls(path)
    #     obj.set_fspaths(fspaths)
    #     return obj

    # def get(self, assume_exists=False):
    #     if assume_exists:
    #         self.exists = True
    #     self._check_part_of_row()
    #     fspaths = self.row.dataset.store.get_fileset_paths(self)
    #     self.exists = True
    #     self.set_fspaths(fspaths)
    #     self.validate_file_paths()

    # def put(self, *fspaths):
    #     self._check_part_of_row()
    #     fspaths = [Path(p) for p in fspaths]
    #     dir_paths = list(p for p in fspaths if p.is_dir())
    #     if len(dir_paths) > 1:
    #         dir_paths_str = "', '".join(str(p) for p in dir_paths)
    #         raise FileFormatError(
    #             f"Cannot put more than one directory, {dir_paths_str}, as part "
    #             f"of the same file set {self}"
    #         )
    #     # Make a copy of the file-set to validate the local paths and auto-gen
    #     # any defaults before they are pushed to the store
    #     cpy = copy(self)
    #     cpy.exists = True
    #     cpy.set_fspaths(fspaths)
    #     cache_paths = self.row.dataset.store.put_fileset_paths(self, cpy.fspaths)
    #     # Set the paths to the cached files
    #     self.exists = True
    #     self.set_fspaths(cache_paths)
    #     self.validate_file_paths()
    #     # Save provenance
    #     if self.provenance:
    #         self.row.dataset.store.put_provenance(self)
