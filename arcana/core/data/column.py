from abc import abstractmethod, ABCMeta
import re
import typing as ty
import attrs
from operator import attrgetter
from attrs.converters import optional

# from arcana.core.data.row import DataRow
from arcana.core.utils.serialize import ClassResolver
from arcana.core.exceptions import ArcanaDataMatchError
from ..analysis.salience import DataQuality, ColumnSalience
from .space import DataSpace


ItemType = ty.TypeVar("ItemType")


@attrs.define
class DataColumn(ty.Generic[ItemType], metaclass=ABCMeta):

    name: str = attrs.field()
    path: str = attrs.field()
    datatype = attrs.field()
    row_frequency: DataSpace = attrs.field()
    dataset = attrs.field(
        default=None, metadata={"asdict": False}, eq=False, hash=False, repr=False
    )

    def __iter__(self):
        return (n[self.name] for n in self.dataset.rows(self.row_frequency))

    def __getitem__(self, id) -> ItemType:
        return self.dataset.row(id=id, row_frequency=self.row_frequency)[self.name]

    def __len__(self):
        return len(list(self.dataset.rows(self.row_frequency)))

    @property
    def ids(self):
        return [n.id for n in self.dataset.rows(self.row_frequency)]

    @abstractmethod
    def match(self, row):
        """Selects a single item from a data row that matches the
        criteria/path of the column.

        Parameters
        ----------
        row: DataRow
            the row to match the item from

        Returns
        -------
        DataType
            the data item that matches the criteria/path

        Raises
        ------
        ArcanaDataMatchError
            if none or multiple items match the criteria/path of the column
            within the row
        ArcanaFileFormatError
            if there are no files matching the format of the column in the row"""

    def assume_exists(self):
        # Update local cache of sink paths
        for item in self:
            item.get(assume_exists=True)


@attrs.define
class DataSource(DataColumn):
    """
    Specifies the criteria by which an item is selected from a data row to
    be a data source.

    Parameters
    ----------
    path : str
        A regex name_path to match the file_group names with. Must match
        one and only one file_group per <row_frequency>. If None, the name
        is used instead.
    datatype : type
        File format that data will be
    row_frequency : DataSpace
        The row_frequency of the file-group within the dataset tree, e.g. per
        'session', 'subject', 'timepoint', 'group', 'dataset'
    quality_threshold : DataQuality
        The acceptable quality (or above) that should be considered. Data items
        will be considered missing
    order : int | None
        To be used to distinguish multiple file_groups that match the
        name_path in the same session. The order of the file_group within the
        session (0-indexed). Based on the scan ID but is more robust to small
        changes to the IDs within the session if for example there are
        two scans of the same type taken before and after a task.
    header_vals : Dict[str, str]
        To be used to distinguish multiple items that match the
        the other criteria. The provided dictionary contains
        header values that must match the stored header_vals exactly.
    is_regex : bool
        Flags whether the name_path is a regular expression or not
    """

    quality_threshold: DataQuality = attrs.field(
        default=None, converter=optional(lambda q: DataQuality[str(q)])
    )
    order: int = attrs.field(
        default=None, converter=lambda x: int(x) if x is not None else None
    )
    header_vals: ty.Dict[str, ty.Any] = attrs.field(default=None)
    is_regex: bool = attrs.field(
        default=False,
        converter=lambda x: x.lower() == "true" if isinstance(x, str) else x,
    )

    is_sink = False

    def match(self, row):
        criteria = [
            (match_path, self.path if not self.is_regex else None),
            (match_path_regex, self.path if self.is_regex else None),
            (match_quality, self.quality_threshold),
            (match_header_vals, self.header_vals),
        ]
        # Get all items that match the data type of the source
        matches = row.resolved(self.datatype)
        if not matches:
            msg = (
                f"Did not find any items matching data datatype "
                f"{ClassResolver.tostr(self.datatype)} in '{row.id}' "
                f"{self.row_frequency} for the "
                f"'{self.name}' column, found unresolved items:"
            )
            for item in sorted(row.unresolved, key=attrgetter("path")):
                msg += (
                    f"\n    {item.path}: paths="
                    + ",".join(p.name for p in item.file_paths)
                    + ((", uris=" + ",".join(item.uris.keys())) if item.uris else "")
                )
            msg += self._format_criteria()
            raise ArcanaDataMatchError(msg)
        # Apply all filters to find items that match criteria
        for func, arg in criteria:
            if arg is not None:
                filtered = [m for m in matches if func(m, arg)]
                if not filtered:
                    raise ArcanaDataMatchError(
                        "Did not find any items "
                        + func.__doc__.format(arg)
                        + self._error_msg(row, matches)
                    )
                matches = filtered
        # Select a single item from the ones that match the criteria
        if self.order is not None:
            try:
                match = matches[self.order - 1]
            except IndexError as e:
                raise ArcanaDataMatchError(
                    "Not enough matching items to select one at index "
                    f"{self.order} (starting from 1), found:"
                    + self._format_matches(matches)
                ) from e
        elif len(matches) > 1:
            raise ArcanaDataMatchError(
                "Found multiple matches " + self._error_msg(row, matches)
            )
        else:
            match = matches[0]
        return match

    def _error_msg(self, row, matches):
        return (
            f" attempting to select {ClassResolver.tostr(self.datatype)} item for "
            f"the '{row.id}' {row.frequency} in the '{self.name}' column, found:"
            + self._format_matches(matches)
            + self._format_criteria()
        )

    def _format_criteria(self):
        return (
            f"\n\n    criteria: {self.path}', is_regex={self.is_regex}, "
            f"datatype={ClassResolver.tostr(self.datatype)}, "
            f"quality_threshold='{self.quality_threshold}', "
            f"header_vals={self.header_vals}, order={self.order}"
        )

    def _format_matches(self, matches):
        out_str = ""
        for match in sorted(matches, key=attrgetter("path")):
            out_str += "\n    "
            if match.order:
                out_str += match.order + ": "
            out_str += match.path
            out_str += f" ({match.quality})"
        return out_str


def match_path(item, path):
    "at the path '{}'"
    return item.path == path


def match_path_regex(item, pattern):
    "that matched the path pattern '{}'"
    if not pattern.endswith("$"):
        pattern += "$"
    return re.match(pattern, item.path)


def match_quality(item, threshold):
    "with an acceptable quality '{}'"
    return item.quality >= threshold


def match_header_vals(item, header_vals):
    "with the header values '{}'"
    return all(item.header(k) == v for k, v in header_vals.items())


@attrs.define
class DataSink(DataColumn):
    """
    A specification for a file group within a analysis to be derived from a
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
        The row_frequency of the file-group within the dataset tree, e.g. per
        'session', 'subject', 'timepoint', 'group', 'dataset'
    salience : Salience
        The salience of the specified file-group, i.e. whether it would be
        typically of interest for publication outputs or whether it is just
        a temporary file in a workflow, and stages in between
    pipeline_name : str
        The name of the workflow applied to the dataset to generates the data
        for the sink
    """

    salience: ColumnSalience = attrs.field(
        default=ColumnSalience.supplementary,
        converter=lambda s: ColumnSalience[str(s)] if s is not None else None,
    )
    pipeline_name: str = attrs.field(default=None)

    is_sink = True

    def match(self, row):
        matches = [i for i in row.resolved(self.datatype) if i.path == self.path]
        if not matches:
            # Return a placeholder data item th.datatypebe set
            return self.datatype(path=self.path, row=row, exists=False)
        elif len(matches) > 1:
            raise ArcanaDataMatchError(
                "Found multiple matches " + self._error_msg(row, matches)
            )
        return matches[0]

    def derive(self, ids=None):
        self.dataset.derive(self.name, ids=ids)
