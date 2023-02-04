from __future__ import annotations
from abc import abstractmethod, ABCMeta
import re
import typing as ty
import attrs
from operator import attrgetter
from attrs.converters import optional
from fileformats.core import DataType
from arcana.core.exceptions import ArcanaDataMatchError
from ..analysis.salience import ColumnSalience
from .quality import DataQuality
from .space import DataSpace

if ty.TYPE_CHECKING:
    from .row import DataRow
    from .entry import DataEntry
    from .cell import DataCell
    from .set import Dataset


@attrs.define
class DataColumn(metaclass=ABCMeta):

    name: str
    datatype: type = attrs.field()
    row_frequency: DataSpace = attrs.field(
        validator=attrs.validators.instance_of(DataSpace)
    )
    path: str = None
    dataset: Dataset = attrs.field(
        default=None, metadata={"asdict": False}, eq=False, hash=False, repr=False
    )

    is_sink = False

    @datatype.validator
    def datatype_validator(self, _, datatype):
        if not issubclass(datatype, DataType):
            raise TypeError(f"Datatype ({datatype}) must be a subclass of {DataType}")

    def __iter__(self) -> ty.Iterable[DataType]:
        "Iterator over all items in the column, requires none of the cells to be empty"
        return (row[self.name] for row in self.dataset.rows(self.row_frequency))

    def __getitem__(self, id) -> DataType:
        return self.dataset.row(id=id, row_frequency=self.row_frequency)[self.name]

    def __len__(self) -> int:
        return len(list(self.dataset.rows(self.row_frequency)))

    def cells(self, allow_empty: bool = None) -> ty.Iterable[DataCell]:
        """Return an iterator over all cells in the column.

        Parameters
        ----------
        allow_empty : bool, optional
            whether to allow cells to be empty (i.e. if they don't match to an entry
            in the corresponding dataset row). If None, then cells of sink columns can
            be empty (i.e. not derived yet) and source columns can't, by default None

        Returns
        -------
        cells : Iterable[DataCell]
            an iterator over all cells in the column
        """
        return (
            row.cell(self.name, allow_empty=allow_empty)
            for row in self.dataset.rows(self.row_frequency)
        )

    @property
    def ids(self) -> list[str]:
        return [n.id for n in self.dataset.rows(self.row_frequency)]

    @property
    def path(self) -> str:
        return self._path

    def entry(self, row: DataRow, allow_none: bool = False) -> DataEntry:
        """Selects a single entry from a data row that matches the
        criteria/path of the column.

        Parameters
        ----------
        row: DataRow
            the row to match the item from
        allow_none: bool
            whether to return None if there are not matches

        Returns
        -------
        DataType or None
            the data item that matches the criteria/path

        Raises
        ------
        ArcanaDataMatchError
            if none or multiple items match the criteria/path of the column
            within the row
        """
        matches = row.entries.values()
        for method in self.criteria():
            filtered = [m for m in matches if method(m)]
            if not filtered:
                if allow_none:
                    return None
                else:
                    raise ArcanaDataMatchError(
                        "Did not find any entries "
                        + method.__doc__.format(self=self)
                        + self._error_msg(row, matches)
                    )
            matches = filtered
        return self.select_entry_from_matches(row, matches)

    @abstractmethod
    def criteria(self) -> list[ty.Callable]:
        """returns all methods used to filter out potential matches"""

    @abstractmethod
    def format_criteria(self) -> str:
        """Formats the criteria used to match entries for use in informative error messages"""

    def matches_path(self, entry: DataEntry) -> bool:
        "that match the path '{self.path}'"
        id_parts = entry.id.split("/")
        path_parts = self.path.split("/")
        return id_parts[: len(path_parts)] == path_parts

    def matches_datatype(self, entry: DataEntry) -> bool:
        "that matched the datatype {self.datatype}"
        return self.datatype is entry.datatype or (
            issubclass(self.datatype, type(entry)) and self.datatype.matches(entry.item)
        )

    def select_entry_from_matches(
        self, row: DataRow, matches: list[DataEntry]
    ) -> DataEntry:
        if len(matches) > 1:
            raise ArcanaDataMatchError(
                "Found multiple matches " + self._error_msg(row, matches)
            )
        return matches[0]

    def _error_msg(self, row: DataRow, matches: list[DataEntry]) -> str:
        return (
            f" attempting to select '{self.datatype.mime_like}' item for "
            f"the '{row.id}' {row.frequency} in the '{self.name}' column\n\n  Found:"
            + self._format_matches(matches)
            + self.format_criteria()
        )

    def _format_matches(self, matches: list[DataEntry]) -> str:
        out_str = ""
        for match in sorted(matches, key=attrgetter("path")):
            out_str += "\n    "
            if match.order:
                out_str += match.order + ": "
            out_str += match.path
            out_str += f" ({match.quality})"
        return out_str


@attrs.define
class DataSource(DataColumn):
    """
    Specifies the criteria by which an item is selected from a data row to
    be a data source.

    Parameters
    ----------
    path : str
        A regex name_path to match the fileset names with. Must match
        one and only one fileset per <row_frequency>. If None, the name
        is used instead.
    datatype : type
        File format that data will be
    row_frequency : DataSpace
        The row_frequency of the file-set within the dataset tree, e.g. per
        'session', 'subject', 'timepoint', 'group', 'dataset'
    quality_threshold : DataQuality
        The acceptable quality (or above) that should be considered. Data items
        will be considered missing
    order : int | None
        To be used to distinguish multiple filesets that match the
        name_path in the same session. The order of the fileset within the
        session (0-indexed). Based on the scan ID but is more robust to small
        changes to the IDs within the session if for example there are
        two scans of the same type taken before and after a task.
    required_metadata : dict[str, ty.Any]
        Required metadata, which can be used to distinguish multiple items that match all
        other criteria. The provided dictionary contains metadata values that must match
        the stored required_metadata exactly.
    is_regex : bool
        Flags whether the name_path is a regular expression or not
    """

    quality_threshold: DataQuality = attrs.field(
        default=None, converter=optional(lambda q: DataQuality[str(q)])
    )
    order: int = attrs.field(
        default=None, converter=lambda x: int(x) if x is not None else None
    )
    required_metadata: dict[str, ty.Any] = attrs.field(default=None)
    is_regex: bool = attrs.field(
        default=False,
        converter=lambda x: x.lower() == "true" if isinstance(x, str) else x,
    )

    def criteria(self) -> list[ty.Callable]:
        criteria = []
        if self.path is not None:
            if self.is_regex:
                criteria.append(self.matches_path_regex)
            else:
                criteria.append(self.matches_path)
        if self.quality_threshold is not None:
            criteria.append(self.matches_quality)
        if self.required_metadata is not None:
            criteria.append(self.matches_metadata)
        criteria.append(self.matches_datatype)
        return criteria

    def format_criteria(self) -> str:
        msg = "\n\n  Criteria: "
        if self.path:
            msg += f"\n    path='{self.path}'"
            if self.is_regex:
                msg += " (regular-expression)" if self.is_regex else ""
        if self.quality_threshold:
            msg += f"\n    quality_threshold='{self.quality_threshold}'"
        if self.required_metadata:
            msg += f"\n    required_metadata={self.required_metadata}"
        msg += f"\n    datatype='{self.datatype.mime_like}'"
        if self.order:
            msg += f"\n    order={self.order}"
        return msg

    def matches_path_regex(self, entry: DataEntry) -> bool:
        "that matched the path pattern '{self.path}'"
        pattern = self.path
        if not pattern.endswith("$"):
            pattern += "$"
        return re.match(pattern, entry.id)

    def matches_quality(self, entry: DataEntry) -> bool:
        "with an acceptable quality '{self.quality_threshold}'"
        return entry.quality >= self.quality_threshold

    def matches_metadata(self, entry: DataEntry) -> bool:
        "with the required metadata '{self.required_metadata}'"
        return all(
            entry.item_metadata[k] == v for k, v in self.required_metadata.items()
        )

    def select_entry_from_matches(
        self, row: DataRow, matches: list[DataEntry]
    ) -> DataEntry:
        # Select a single item from the ones that match the criteria
        if self.order is not None:
            try:
                return matches[self.order - 1]
            except IndexError as e:
                raise ArcanaDataMatchError(
                    f"Not enough matching items in row {row.id} {row.frequency} in the "
                    f"'{self.name}' column to select one at index {self.order} "
                    "(starting from 1), found:" + self._format_matches(matches)
                ) from e
        else:
            return super().select_entry_from_matches(row, matches)


@attrs.define
class DataSink(DataColumn):
    """
    A specification for a file set within a analysis to be derived from a
    processing pipeline.

    Parameters
    ----------
    path : str
        The path to the relative location the corresponding data items will be
        stored within the rows of the data tree.
    datatype : type
        The file datatype or data type used to store the corresponding items
        in the store dataset.
    row_frequency : DataSpace
        The row_frequency of the file-set within the dataset tree, e.g. per
        'session', 'subject', 'timepoint', 'group', 'dataset'
    salience : Salience
        The salience of the specified file-set, i.e. whether it would be
        typically of interest for publication outputs or whether it is just
        a temporary file in a workflow, and stages in between
    pipeline_name : str
        The name of the workflow applied to the dataset to generates the data
        for the sink
    """

    path = attrs.field()
    salience: ColumnSalience = attrs.field(
        default=ColumnSalience.supplementary,
        converter=lambda s: ColumnSalience[str(s)] if s is not None else None,
    )
    pipeline_name: str = attrs.field(default=None)

    is_sink = True

    @path.default
    def path_default(self):
        return f"{self.dataset.name}/{self.name}"

    def derive(self, ids: list[str] = None):
        self.dataset.derive(self.name, ids=ids)

    def criteria(self):
        return [self.matches_path, self.matches_datatype]

    def format_criteria(self):
        return (
            "\n\n  Criteria: "
            f"\n    path='{self.path}' "
            f"\n    datatype='{self.datatype}' "
        )
