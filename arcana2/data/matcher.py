from builtins import object
from past.builtins import basestring
import re
from copy import copy
from itertools import chain
from arcana2.exceptions import (
    ArcanaUsageError, ArcanaInputError,
    ArcanaInputMissingMatchError, ArcanaNotBoundToAnalysisError)
from .base import FileGroupMixin, FieldMixin
from .item import FileGroup, Field
from .column import FileGroupColumn, FieldColumn
from .tree import TreeLevel


class DataMatcher():
    """
    Base class for FileGroup and Field Matcher classes
    """

    def __init__(self, pattern, is_regex, order):
        self.pattern = pattern
        self.is_regex = is_regex
        self.order = order

    def __eq__(self, other):
        return (self.pattern == other.pattern
                and self.is_regex == other.is_regex
                and self.order == other.order)

    def __hash__(self):
        return (hash(self.pattern)
                ^ hash(self.is_regex)
                ^ hash(self.order))

    def initkwargs(self):
        dct = {}
        dct['pattern'] = self.pattern
        dct['order'] = self.order
        dct['is_regex'] = self.is_regex
        return dct

    def _match(self, tree, item_cls, **kwargs):
        matches = []
        errors = []
        for node in self.nodes(tree):
            try:
                try:
                    matches.append(self.match_node(node, **kwargs))
                except ArcanaInputMissingMatchError as e:
                    if self._fallback is not None:
                        matches.append(self._fallback.column.item(
                            subject_id=node.subject_id,
                            visit_id=node.visit_id))
                    elif self.skip_missing:
                        # Insert a non-existant item placeholder in-place of
                        # the the missing item
                        matches.append(item_cls(
                            self.name,
                            tree_level=self.tree_level,
                            subject_id=node.subject_id,
                            visit_id=node.visit_id,
                            dataset=self.analysis.dataset,
                            namespace=self.namespace,
                            exists=False,
                            **self._specific_kwargs))
                    else:
                        raise e
            except ArcanaInputError as e:
                errors.append(e)
        # Collate potentially multiple errors into a single error message
        if errors:
            if all(isinstance(e, ArcanaInputMissingMatchError)
                   for e in errors):
                ErrorClass = ArcanaInputMissingMatchError
            else:
                ErrorClass = ArcanaInputError
            raise ErrorClass('\n'.join(str(e) for e in errors))
        return matches

    def match_node(self, node, **kwargs):
        # Get names matching pattern
        matches = self._filtered_matches(node, **kwargs)
        # Matcher matches by analysis name
        ns_matches = [d for d in matches if d.namespace == self.namespace]
        # Select the file_group from the matches
        if not ns_matches:
            raise ArcanaInputMissingMatchError(
                "No matches found for {} in {} for analysis {}. Found:\n    {}"
                .format(
                    self, node, self.namespace,
                    '\n    '.join(str(m) for m in matches)))
        elif self.order is not None:
            try:
                match = ns_matches[self.order]
            except IndexError:
                raise ArcanaInputMissingMatchError(
                    "Did not find {} named data matching pattern {}, found "
                    " {} in {}".format(self.order, self.pattern,
                                       len(matches), node))
        elif len(ns_matches) == 1:
            match = ns_matches[0]
        else:
            raise ArcanaInputError(
                "Found multiple matches for {} in {}:\n    {}"
                .format(self, node,
                        '\n    '.join(str(m) for m in ns_matches)))
        return match


class FileGroupMatcher(DataMatcher, FileGroupMixin):
    """
    A pattern that describes a single file_group (typically acquired
    rather than generated but not necessarily) within each session.

    Parameters
    ----------
    pattern : str
        A regex pattern to match the file_group names with. Must match
        one and only one file_group per <tree_level>. If None, the name
        is used instead.
    format : FileFormat
        File formats that data will be accepted in        
    is_regex : bool
        Flags whether the pattern is a regular expression or not
    tree_level : TreeLevel
        The level within the dataset tree that the data items sit, i.e. 
        per 'session', 'subject', 'visit', 'group_visit', 'group' or 'dataset'
    scan_id : int | None
        To be used to distinguish multiple scan that match the
        pattern in the same session. The scan ID of the file_group within the
        session.
    order : int | None
        To be used to distinguish multiple file_groups that match the
        pattern in the same session. The order of the file_group within the
        session. Based on the scan ID but is more robust to small
        changes to the IDs within the session if for example there are
        two scans of the same type taken before and after a task.
    dicom_tags : Dict[str, str]
        To be used to distinguish multiple file_groups that match the
        pattern in the same session. The provided DICOM values dicom
        header values must match exactly.
    namespace : str
        The name of the analysis that generated the derived file_group to match.
        Is used to determine the location of the file_groups in the
        dataset as the derived file_groups and fields are grouped by
        the name of the analysis that generated them.
    acceptable_quality : str | list[str] | None
        An acceptable quality label, or list thereof, to accept, i.e. if a
        file_group's quality label is not in the list it will be ignored. If a
        scan wasn't labelled the value of its qualtiy will be None.
    """

    is_spec = False
    ColumnClass = FileGroupColumn

    def __init__(self, pattern=None, format=None,
                 tree_level=TreeLevel.session, scan_id=None,
                 order=None, dicom_tags=None, is_regex=False,
                 namespace=None, acceptable_quality=None):
        FileGroupMixin.__init__(self, None, tree_level, namespace)
        DataMatcher.__init__(self, pattern, is_regex, order)
        self.dicom_tags = dicom_tags
        if order is not None and scan_id is not None:
            raise ArcanaUsageError(
                "Cannot provide both 'order' and 'scan_id' to a file_group"
                "match")
        self.format = format
        self.scan_id = str(scan_id) if scan_id is not None else scan_id
        if isinstance(acceptable_quality, basestring):
            acceptable_quality = (acceptable_quality,)
        elif acceptable_quality is not None:
            acceptable_quality = tuple(acceptable_quality)
        self.acceptable_quality = acceptable_quality

    def __eq__(self, other):
        return (FileGroupMixin.__eq__(self, other) and
                DataMatcher.__eq__(self, other) and
                self.dicom_tags == other.dicom_tags and
                self.scan_id == other.scan_id and
                self._acceptable_quality == other._acceptable_quality)

    def __hash__(self):
        return (FileGroupMixin.__hash__(self) ^
                DataMatcher.__hash__(self) ^
                hash(self.dicom_tags) ^
                hash(self.scan_id) ^
                hash(self._acceptable_quality))

    def initkwargs(self):
        dct = FileGroupMixin.initkwargs(self)
        dct.update(DataMatcher.initkwargs(self))
        dct['dicom_tags'] = self.dicom_tags
        dct['scan_id'] = self.scan_id
        dct['acceptable_quality'] = self.acceptable_quality
        return dct

    def __repr__(self):
        return ("{}(name='{}', format={}, tree_level={}, pattern={}, "
                "is_regex={}, order={}, scan_id={}, dicom_tags={}, "
                "namespace={}, acceptable_quality={})"
                .format(self.__class__.__name__, self.name, self._format,
                        self.tree_level, self.pattern, self.is_regex,
                        self.order, self.scan_id, self.dicom_tags,
                        self.namespace, self._acceptable_quality))

    def match(self, tree, **kwargs):
        # Run the match against the tree
        return FileGroupColumn(self.name,
                               self._match(tree, FileGroup, **kwargs),
                               tree_level=self.tree_level)

    def _filtered_matches(self, node, **kwargs):  # noqa pylint: disable=unused-argument
        if self.pattern is not None:
            if self.is_regex:
                pattern_re = re.compile(self.pattern)
                matches = [f for f in node.file_groups
                           if pattern_re.match(f.basename)]
            else:
                matches = [f for f in node.file_groups
                           if f.basename == self.pattern]
        else:
            matches = list(node.file_groups)
        if not matches:
            raise ArcanaInputMissingMatchError(
                "Did not find any matches for {} in {}. Found:\n    {}"
                .format(self, node,
                        '\n    '.join(str(f) for f in node.file_groups)))
        if self.acceptable_quality is not None:
            filtered = [f for f in matches
                        if f.quality in self.acceptable_quality]
            if not filtered:
                raise ArcanaInputMissingMatchError(
                    "Did not find file_groups names matching pattern {} "
                    "with an acceptable quality {} in {}. Found:\n    {}"
                    .format(
                        self.pattern, self.acceptable_quality, node,
                        '\n    '.join(str(m) for m in matches)))
            matches = filtered
        if self.scan_id is not None:
            filtered = [d for d in matches if d.scan_id == self.scan_id]
            if not filtered:
                raise ArcanaInputMissingMatchError(
                    "Did not find file_groups names matching pattern {} "
                    "with an scan_id of {} in {}. Found:\n    {} ".format(
                        self.pattern, self.scan_id,
                        '\n    '.join(str(m) for m in matches), node))
            matches = filtered
        if self.format is not None:
            format_matches = [m for m in matches if self.format.matches(m)]
            if not format_matches:
                for f in matches:
                    self.format.matches(f)
                raise ArcanaInputMissingMatchError(
                    "Did not find any file_groups that match the file format "
                    "specified by {} in {}. Found:\n    {}"
                    .format(self, node,
                            '\n    '.join(str(f) for f in matches)))
            matches = format_matches
        # Matcher matches by dicom tags
        if self.dicom_tags is not None:
            if self.valid_formats is None or len(self.valid_formats) != 1:
                raise ArcanaUsageError(
                    "Can only match header tags if exactly one valid format "
                    "is specified ({})".format(self.valid_formats))
            format = self.valid_formats[0]
            filtered = []
            for file_group in matches:
                keys, ref_values = zip(*self.dicom_tags.items())
                values = tuple(format.dicom_values(file_group, keys))
                if ref_values == values:
                    filtered.append(file_group)
            if not filtered:
                raise ArcanaInputMissingMatchError(
                    "Did not find file_groups names matching pattern {}"
                    "that matched DICOM tags {} in {}. Found:\n    {}"
                    .format(self.pattern, self.dicom_tags,
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


class FieldMatcher(DataMatcher, FieldMixin):
    """
    A pattern that matches a single field (typically acquired rather than
    generated but not necessarily) in each session.

    Parameters
    ----------
    pattern : str
        A regex pattern to match the field names with. Must match
        one and only one file_group per <tree_level>. If None, the name
        is used instead.
    dtype : type | None
        The datatype of the value. Can be one of (float, int, str). If None
        then the dtype is taken from the FieldSpec that it is bound to
    is_regex : bool
        Flags whether the pattern is a regular expression or not
    tree_level : TreeLevel
        The level within the dataset tree that the data items sit, i.e. 
        per 'session', 'subject', 'visit', 'group_visit', 'group' or 'dataset'
    order : int | None
        To be used to distinguish multiple file_groups that match the
        pattern in the same session. The order of the file_group within the
        session. Based on the scan ID but is more robust to small
        changes to the IDs within the session if for example there are
        two scans of the same type taken before and after a task.
    namespace : str
        The name of the analysis that generated the derived field to match.
        Is used to determine the location of the fields in the
        dataset as the derived file_groups and fields are grouped by
        the name of the analysis that generated them.
    """

    is_spec = False
    ColumnClass = FieldColumn

    def __init__(self, pattern, dtype=None, tree_level=TreeLevel.session,
                 order=None, is_regex=False, namespace=None):
        FieldMixin.__init__(self, dtype, tree_level, namespace)
        DataMatcher.__init__(self, pattern, is_regex, order)

    def __eq__(self, other):
        return (FieldMixin.__eq__(self, other) and
                DataMatcher.__eq__(self, other))

    def match(self, tree, **kwargs):
        # Run the match against the tree
        return FieldColumn(self.name,
                          self._match(tree, Field, **kwargs),
                          tree_level=self.tree_level,
                          dtype=self.dtype)

    @property
    def dtype(self):
        if self._dtype is None:
            try:
                dtype = self.analysis.data_spec(self.name).dtype
            except ArcanaNotBoundToAnalysisError:
                dtype = None
        else:
            dtype = self._dtype
        return dtype

    def __hash__(self):
        return (FieldMixin.__hash__(self) ^ DataMatcher.__hash__(self))

    def initkwargs(self):
        dct = FieldMixin.initkwargs(self)
        dct.update(DataMatcher.initkwargs(self))
        return dct

    def _filtered_matches(self, node, **kwargs):
        if self.is_regex:
            pattern_re = re.compile(self.pattern)
            matches = [f for f in node.fields
                       if pattern_re.match(f.name)]
        else:
            matches = [f for f in node.fields
                       if f.name == self.pattern]
        if self.namespace is not None:
            matches = [f for f in matches
                       if f.namespace == self.namespace]
        if not matches:
            raise ArcanaInputMissingMatchError(
                "Did not find any matches for {} in {}. Found:\n    {}"
                .format(self, node,
                        '\n    '.join(f.name for f in node.fields)))
        return matches

    def __repr__(self):
        return ("{}(name='{}', dtype={}, tree_level={}, pattern={}, "
                "is_regex={}, order={}, namespace={})"
                .format(self.__class__.__name__, self.name, self._dtype,
                        self.tree_level, self.pattern, self.is_regex,
                        self.order, self.namespace))

    @property
    def _specific_kwargs(self):
        return {'dtype': self.dtype}
