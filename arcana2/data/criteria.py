from builtins import object
from past.builtins import basestring
import re
from copy import copy
from enum import Enum
import attr
from itertools import chain
from arcana2.exceptions import (
    ArcanaMultipleMatchesInputError, ArcanaFileFormatError,
    ArcanaInputMissingMatchError, ArcanaNotBoundToAnalysisError)
from .base import FileGroupMixin, FieldMixin
from .item import FileGroup, Field
from .enum import DataFrequency, DataQuality


@attr.s
class DataCriteria():
    """
    Criteria by which an item is selected from a data node

    Parameters
    ----------
    path : str
        A regex name_path to match the file_group names with. Must match
        one and only one file_group per <frequency>. If None, the name
        is used instead.
    dtype : FileFormat or type
        File format that data will be 
    frequency : DataFrequency
        The frequency of the file-group within the dataset tree, e.g. per
        'session', 'subject', 'timepoint', 'group', 'dataset'        
    is_regex : bool
        Flags whether the name_path is a regular expression or not
    index : int | None
        To be used to distinguish multiple file_groups that match the
        name_path in the same session. The order of the file_group within the
        session. Based on the scan ID but is more robust to small
        changes to the IDs within the session if for example there are
        two scans of the same type taken before and after a task.
    metadata : Dict[str, str]
        To be used to distinguish multiple file_groups that match the
        name_path in the same node. The provided dictionary contains
        header values that must match the stored header_vals exactly.
    quality_threshold : str | list[str] | None
        An acceptable quality label, or list thereof, to accept, i.e. if a
        file_group's quality label is not in the list it will be ignored. If a
        scan wasn't labelled the value of its qualtiy will be None.
    """

    path = attr.ib(type=str)
    dtype = attr.ib()
    frequency = attr.ib(type=DataFrequency)
    is_regex = attr.ib(type=bool, default=False)
    index = attr.ib(type=int, default=None)
    metadata = attr.ib(default=None)
    quality_threshold = attr.ib(type=DataQuality, default=DataQuality.usable)

    def match(self, node):
        criteria = [
            (match_path, self.path if not self.is_regex else None),
            (match_path_regex, self.path if self.is_regex else None),
            (match_format, self.format),
            (match_quality, self.quality_threshold),
            (match_metadata, self.metadata)]
        matches = list(node.unresolved)
        for func, arg in criteria:
            if arg is not None:
                filtered = [m for m in matches if func(m)]
                if not filtered:
                    raise ArcanaInputMissingMatchError(
                        "Did not find any items " + func.__doc__.format(arg)
                        + self._error_msg(node, matches))
                matches = filtered
        if self.index is not None:
            try:
                match = matches[self.index]
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
            f"attempting to select an item from {node} matching {self}, "
            "found:\n" + "\n    ".join(str(m) for m in matches))
        

def match_path(item, path):
    "at the path {}"
    return item == path

def match_format(item, format):
    "that can be resolved to the requested format '{}'"
    try:
        item.resolve(format)
    except ArcanaFileFormatError:
        return False
    else:
        return True

def match_path_regex(item, pattern):
    "with a path that matched the pattern {}"
    return re.match(pattern, item.path)

def match_quality(item, threshold):
    "with an acceptable quality {}"
    return item.quality >= threshold

def match_metadata(item, metadata):
    "with the header values {}"
    return all(item.metadata(k) == v for k, v in metadata.items())
