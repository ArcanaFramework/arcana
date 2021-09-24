import re
import attr
from pydra import Workflow
from arcana2.exceptions import (
    ArcanaMultipleMatchesInputError, ArcanaFileFormatError,
    ArcanaInputMissingMatchError)
from .enum import DataDimension, DataQuality, DataSalience


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
    data_format : FileFormat or type
        File format that data will be 
    frequency : DataDimension
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
    metadata : Dict[str, str]
        To be used to distinguish multiple items that match the
        the other criteria. The provided dictionary contains
        header values that must match the stored header_vals exactly.   
    is_regex : bool
        Flags whether the name_path is a regular expression or not
    """
    path = attr.ib(type=str)
    data_format = attr.ib()
    frequency = attr.ib(type=DataDimension)
    quality_threshold = attr.ib(type=DataQuality, default=DataQuality.usable)
    order = attr.ib(type=int, default=None)
    metadata = attr.ib(default=None)
    is_regex = attr.ib(type=bool, default=False)

    def match(self, node):
        criteria = [
            (match_path, self.path if not self.is_regex else None),
            (match_path_regex, self.path if self.is_regex else None),
            (match_quality, self.quality_threshold),
            (match_metadata, self.metadata)]
        # Get all items that match the data format of the source
        matches = node.resolved(self.data_format)
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
    return re.match(pattern, item.path)

def match_quality(item, threshold):
    "with an acceptable quality {}"
    return item.quality >= threshold

def match_metadata(item, metadata):
    "with the header values {}"
    return all(item.metadata(k) == v for k, v in metadata.items())


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
        in the repository dataset.
    frequency : DataDimension
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
    data_format = attr.ib()
    frequency: DataDimension = attr.ib()
    salience: DataSalience = attr.ib(default=DataSalience.supplementary)
    pipeline: str = attr.ib(default=None)

    def match(self, node):
        matches = [i for i in node.resolved(self.data_format)
                   if i.path == self.path]
        if not matches:
            raise ArcanaInputMissingMatchError(
                f"Did not find any items matching path {self.path} of format "
                f"{self.data_format}")
        elif len(matches) > 1:
            raise ArcanaMultipleMatchesInputError(
                "Found multiple matches " + self._error_msg(node, matches))
        return matches[0]
