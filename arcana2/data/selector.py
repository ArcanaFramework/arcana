from builtins import object
from past.builtins import basestring
import re
from copy import copy
from itertools import chain
from arcana2.exceptions import (
    ArcanaMultipleMatchesInputError, ArcanaFileFormatError,
    ArcanaInputMissingMatchError, ArcanaNotBoundToAnalysisError)
from .base import FileGroupMixin, FieldMixin
from .item import FileGroup, Field


class DataSelector():
    """
    Base class for FileGroup and Field Selector classes
    """

    def __init__(self, is_regex, order, frequency, skip_missing):
        self.is_regex = is_regex
        self.order = order
        self.skip_missing = skip_missing
        self.frequency = frequency

    def __eq__(self, other):
        return (self.is_regex == other.is_regex
                and self.frequency == other.frequency
                and self.order == other.order
                and self.skip_missing == other.skip_missing)

    def __hash__(self):
        return (hash(self.is_regex)
                ^ hash(self.frequency)
                ^ hash(self.order)
                ^ hash(self.skip_missing))

    def initkwargs(self):
        dct = {}
        dct['order'] = self.order
        dct['is_regex'] = self.is_regex
        dct['frequency'] = self.frequency
        dct['skip_missing'] = self.skip_missing
        return dct

    # def match(self, dataset, **kwargs):
    #     matches = []
    #     errors = []
    #     for data_node in dataset[self.frequency]:
    #         try:
    #             try:
    #                 matches.append(self.match_node(data_node, **kwargs))
    #             except ArcanaInputMissingMatchError as e:
    #                 if self.skip_missing:
    #                     # Insert a non-existant item placeholder in-place of
    #                     # the the missing item
    #                     matches.append(self.item_cls(
    #                         self.name_path,
    #                         data_node=data_node,
    #                         exists=False,
    #                         **self._specific_kwargs))
    #                 else:
    #                     raise e
    #         except ArcanaSelectionError as e:
    #             errors.append(e)
    #     # Collate potentially multiple errors into a single error message
    #     if errors:
    #         if all(isinstance(e, ArcanaInputMissingMatchError)
    #                for e in errors):
    #             ErrorClass = ArcanaInputMissingMatchError
    #         else:
    #             ErrorClass = ArcanaSelectionError
    #         raise ErrorClass('\n'.join(str(e) for e in errors))
    #     return matches

    def match(self, node, **kwargs):
        # Get names matching name_path
        matches = self._filtered_matches(node, **kwargs)
        # Select the file_group from the matches
        if not matches:
            raise ArcanaInputMissingMatchError(
                "No matches found for {} in {} for analysis {}."
                .format(self, node))
        elif self.order is not None:
            try:
                match = matches[self.order]
            except IndexError:
                raise ArcanaInputMissingMatchError(
                    "Did not find {} named data matching name_path {}, found "
                    " {} in {}".format(self.order, self.name_path,
                                       len(matches), node))
        elif len(matches) == 1:
            match = matches[0]
        else:
            raise ArcanaMultipleMatchesInputError(
                "Found multiple matches for {} in {}:\n    {}"
                .format(self, node,
                        '\n    '.join(str(m) for m in matches)))
        return match


class FileGroupSelector(DataSelector, FileGroupMixin):
    """
    A name_path that describes a single file_group (typically acquired
    rather than generated but not necessarily) within each session.

    Parameters
    ----------
    frequency : DataFrequency
        The frequency of the file-group within the dataset tree, e.g. per
        'session', 'subject', 'timepoint', 'group', 'dataset'
    name_path : str
        A regex name_path to match the file_group names with. Must match
        one and only one file_group per <frequency>. If None, the name
        is used instead.
    format : FileFormat
        File format that data will be 
    is_regex : bool
        Flags whether the name_path is a regular expression or not
    order : int | None
        To be used to distinguish multiple file_groups that match the
        name_path in the same session. The order of the file_group within the
        session. Based on the scan ID but is more robust to small
        changes to the IDs within the session if for example there are
        two scans of the same type taken before and after a task.
    header_vals : Dict[str, str]
        To be used to distinguish multiple file_groups that match the
        name_path in the same node. The provided dictionary contains
        header values that must match the stored header_vals exactly.
    acceptable_quality : str | list[str] | None
        An acceptable quality label, or list thereof, to accept, i.e. if a
        file_group's quality label is not in the list it will be ignored. If a
        scan wasn't labelled the value of its qualtiy will be None.
    skip_missing : bool
        Whether to skip over missing matches or raise an error
    """

    is_spec = False
    item_cls = FileGroup
    dtype = str  # For duck-typing with FieldSelectors. Returns the local path

    def __init__(self, frequency, format, name_path=None,
                 order=None, header_vals=None, is_regex=False,
                 acceptable_quality=None, skip_missing=False):
        FileGroupMixin.__init__(self, name_path, format)
        DataSelector.__init__(self, is_regex, order, frequency, skip_missing)
        self.header_vals = header_vals
        if isinstance(acceptable_quality, basestring):
            acceptable_quality = (acceptable_quality,)
        elif acceptable_quality is not None:
            acceptable_quality = tuple(acceptable_quality)
        self.acceptable_quality = acceptable_quality

    def __eq__(self, other):
        return (FileGroupMixin.__eq__(self, other) and
                DataSelector.__eq__(self, other) and
                self.header_vals == other.header_vals and
                self._acceptable_quality == other._acceptable_quality)

    def __hash__(self):
        return (FileGroupMixin.__hash__(self) ^
                DataSelector.__hash__(self) ^
                hash(self.header_vals) ^
                hash(self._acceptable_quality))

    def initkwargs(self):
        dct = FileGroupMixin.initkwargs(self)
        dct.update(DataSelector.initkwargs(self))
        dct['header_vals'] = self.header_vals
        dct['acceptable_quality'] = self.acceptable_quality
        return dct

    def __repr__(self):
        return ("{}(name_path='{}', format={}, frequency={}, name_path={}, "
                "is_regex={}, order={}, header_vals={}, acceptable_quality={})"
                .format(self.__class__.__name__, self.name_path, self.format,
                        self.frequency, self.name_path, self.is_regex,
                        self.order, self.header_vals,
                        self.acceptable_quality))

    def _filtered_matches(self, node, **kwargs):  # noqa pylint: disable=unused-argument
        # Start off with all file groups in the node
        matches = list(node.file_groups)
        # Filter by name
        if self.name_path is not None:
            if self.is_regex:
                name_path_re = re.compile(self.name_path)
                matches = [f for f in matches
                           if name_path_re.match(f.name_path)]
            else:
                matches = [f for f in matches if f.name_path == self.name_path]
        if not matches:
            raise ArcanaInputMissingMatchError(
                "Did not find any matches for {} in {}. Found:\n    {}"
                .format(self, node,
                        '\n    '.join(str(f) for f in node.file_groups)))
        # Filter by available formats
        filtered = []
        for unresolved_file_group in node.file_groups:
            try:
                file_group = unresolved_file_group.resolve(self.format)
            except ArcanaFileFormatError:
                pass
            else:
                filtered.append(file_group)
        if not filtered:
            raise ArcanaInputMissingMatchError(
                "Did not find file_groups names matching name_path {} "
                "in the requested format {} in {}. Found:\n    {}"
                .format(
                    self.name_path, self.format, node,
                    '\n    '.join(str(m) for m in matches)))
        matches = filtered
        # Filter by quality
        if self.acceptable_quality is not None:
            filtered = [f for f in matches
                        if f.quality in self.acceptable_quality]
            if not filtered:
                raise ArcanaInputMissingMatchError(
                    "Did not find file_groups names matching name_path {} "
                    "with an acceptable quality {} in {}. Found:\n    {}"
                    .format(
                        self.name_path, self.acceptable_quality, node,
                        '\n    '.join(str(m) for m in matches)))
            matches = filtered
        # Selector matches by matching header values
        if self.header_vals is not None:
            filtered = []
            for file_group in matches:
                if all(file_group.header_value(k) == v
                       for k, v in self.header_vals.items()):
                    filtered.append(file_group)
            if not filtered:
                raise ArcanaInputMissingMatchError(
                    "Did not find file_groups names matching name_path {}"
                    "that matched the header values {} in {}. Found:\n    {}"
                    .format(self.name_path, self.header_vals,
                            '\n    '.join(str(m) for m in matches), node))
            matches = filtered
        return matches

    def cache(self):
        """
        Forces the cache of the input file_group. Can be useful for before
        running a workflow that will use many concurrent jobs/processes to
        source data from remote dataset, to force the download to be done
        linearly and avoid DOSing the host
        """
        for item in self.column:
            if item.exists:
                item.get()

    @property
    def _specific_kwargs(self):
        return {'format': self.format}


class FieldSelector(DataSelector, FieldMixin):
    """
    A name_path that matches a single field (typically acquired rather than
    generated but not necessarily) in each session.

    Parameters
    ----------
    frequency : DataFrequency
        The frequency of the field within the dataset tree, e.g. per
        'session', 'subject', 'timepoint', 'group', 'dataset'
    name_path : str
        A regex name_path to match the field names with. Must match
        one and only one file_group per <frequency>. If None, the name
        is used instead.
    dtype : type | None
        The datatype of the value. Can be one of (float, int, str). If None
        then the dtype is taken from the FieldSpec that it is bound to
    is_regex : bool
        Flags whether the name_path is a regular expression or not
    order : int | None
        To be used to distinguish multiple file_groups that match the
        name_path in the same session. The order of the file_group within the
        session. Based on the scan ID but is more robust to small
        changes to the IDs within the session if for example there are
        two scans of the same type taken before and after a task.
    namespace : str
        The name of the analysis that generated the derived field to match.
        Is used to determine the location of the fields in the
        dataset as the derived file_groups and fields are grouped by
        the name of the analysis that generated them.
    skip_missing : bool
        Whether to skip over missing matches or raise an error
    """

    is_spec = False
    item_cls = Field

    def __init__(self, frequency, name_path, dtype=None, order=None, is_regex=False,
                 skip_missing=False, array=False):
        FieldMixin.__init__(self, name_path, dtype, array)
        DataSelector.__init__(self, is_regex, order, frequency, skip_missing)

    def __eq__(self, other):
        return (FieldMixin.__eq__(self, other) and
                DataSelector.__eq__(self, other))

    @property
    def dtype(self):
        if self._dtype is None:
            try:
                dtype = self.analysis.data_spec(self.name_path).dtype
            except ArcanaNotBoundToAnalysisError:
                dtype = None
        else:
            dtype = self._dtype
        return dtype

    def __hash__(self):
        return (FieldMixin.__hash__(self) ^ DataSelector.__hash__(self))

    def initkwargs(self):
        dct = FieldMixin.initkwargs(self)
        dct.update(DataSelector.initkwargs(self))
        return dct

    def _filtered_matches(self, node, **kwargs):
        if self.is_regex:
            name_path_re = re.compile(self.name_path)
            matches = [f for f in node.fields
                       if name_path_re.match(f.name_path)]
        else:
            matches = [f for f in node.fields
                       if f.name_path == self.name_path]
        if not matches:
            raise ArcanaInputMissingMatchError(
                "Did not find any matches for {} in {}. Found:\n    {}"
                .format(self, node,
                        '\n    '.join(f.name_path for f in node.fields)))
        return matches

    def __repr__(self):
        return ("{}(name_path='{}', dtype={}, frequency={}, "
                "is_regex={}, order={})"
                .format(self.__class__.__name__, self.name_path, self._dtype,
                        self.frequency, self.is_regex, self.order))

    @property
    def _specific_kwargs(self):
        return {'dtype': self.dtype}
