import re
import typing as ty
import attr
from attr.converters import optional
# from arcana.core.data.node import DataNode
from arcana.exceptions import (
    ArcanaMultipleMatchesInputError, ArcanaFileFormatError,
    ArcanaInputMissingMatchError)
from .enum import DataQuality, DataSalience
from .spaces import DataSpace


@attr.s
class DataSource():
    """
    Specifies the criteria by which an item is selected from a data node to
    be a data source.

    Parameters
    ----------
    path : str
        A regex name_path to match the file_group names with. Must match
        one and only one file_group per <frequency>. If None, the name
        is used instead.
    datatype : FileFormat or type
        File format that data will be 
    frequency : DataSpace
        The frequency of the file-group within the dataset tree, e.g. per
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
    path: str = attr.ib()
    datatype = attr.ib()
    frequency: DataSpace = attr.ib()
    quality_threshold: DataQuality = attr.ib(
        default=None, converter=optional(lambda q: DataQuality[str(q)]))
    order: int = attr.ib(default=None)
    header_vals: ty.Dict[str, ty.Any] = attr.ib(default=None)
    is_regex: bool = attr.ib(default=False)

    def match(self, node):
        criteria = [
            (match_path, self.path if not self.is_regex else None),
            (match_path_regex, self.path if self.is_regex else None),
            (match_quality, self.quality_threshold),
            (match_header_vals, self.header_vals)]
        # Get all items that match the data format of the source
        matches = node.resolved(self.datatype)
        if not matches:
            raise ArcanaInputMissingMatchError(
                f"Did not find any items matching data format "
                f"{self.datatype} in {node}, found unresolved items:\n"
                + '\n'.join(str(i.path) for i in node.unresolved))
        # Apply all filters to find items that match criteria
        for func, arg in criteria:
            if arg is not None:
                filtered = [m for m in matches if func(m, arg)]
                if not filtered:
                    raise ArcanaInputMissingMatchError(
                        "Did not find any items " + func.__doc__.format(arg)
                        + self._error_msg(node, matches))
                matches = filtered
        # Select a single item from the ones that match the criteria
        if self.order is not None:
            try:
                match = matches[self.order]
            except IndexError:
                raise ArcanaInputMissingMatchError(
                    "Not enough matching items to select one at index "
                    f"{self.order}, found "
                    + ", ".join(str(m) for m in matches))
        elif len(matches) > 1:
            raise ArcanaMultipleMatchesInputError(
                "Found multiple matches " + self._error_msg(node, matches))
        else:
            match = matches[0]
        return match

    def _error_msg(self, node, matches):
        return (
            f" attempting to select an item from {node} matching {self}, "
            "found:\n" + "\n    ".join(str(m) for m in matches))

def match_path(item, path):
    "at the path {}"
    return item.path == path

def match_path_regex(item, pattern):
    "with a path that matched the pattern {}"
    if not pattern.endswith('$'):
        pattern += '$'
    return re.match(pattern, item.path)

def match_quality(item, threshold):
    "with an acceptable quality {}"
    return item.quality >= threshold

def match_header_vals(item, header_vals):
    "with the header values {}"
    return all(item.header(k) == v for k, v in header_vals.items())


@attr.s
class DataSink():
    """
    A specification for a file group within a analysis to be derived from a
    processing pipeline.

    Parameters
    ----------
    path : str
        The path to the relative location the corresponding data items will be
        stored within the nodes of the data tree.
    format : FileFormat or type
        The file format or data type used to store the corresponding items
        in the store dataset.
    frequency : DataSpace
        The frequency of the file-group within the dataset tree, e.g. per
        'session', 'subject', 'timepoint', 'group', 'dataset'
    salience : Salience
        The salience of the specified file-group, i.e. whether it would be
        typically of interest for publication outputs or whether it is just
        a temporary file in a workflow, and stages in between
    workflow : str
        The nane of the workflow applied to the dataset to generates the data
        for the sink
    """

    path: str = attr.ib()
    datatype = attr.ib()
    frequency: DataSpace = attr.ib()
    salience: DataSalience = attr.ib(default=DataSalience.supplementary)
    pipeline: str = attr.ib(default=None)

    def match(self, node):
        matches = [i for i in node.resolved(self.datatype)
                   if i.path == self.path]
        if not matches:
            # Return a placeholder data item that can be set
            return self.datatype(path=self.path, data_node=node,
                                    exists=False)
        elif len(matches) > 1:
            raise ArcanaMultipleMatchesInputError(
                "Found multiple matches " + self._error_msg(node, matches))
        return matches[0]
