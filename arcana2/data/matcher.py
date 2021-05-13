from builtins import object
from past.builtins import basestring
import re
from copy import copy
from itertools import chain
from arcana2.exceptions import (
    ArcanaUsageError, ArcanaInputError,
    ArcanaInputMissingMatchError, ArcanaNotBoundToAnalysisError)
from .base import BaseFileGroup, BaseField
from .item import FileGroup, Field
from .column import FileGroupColumn, FieldColumn


class BaseMatcherMixin(object):
    """
    Base class for FileGroup and Field Matcher classes
    """

    def __init__(self, pattern, is_regex, order, from_analysis,
                 skip_missing=False, drop_if_missing=False,
                 fallback_to_default=False):
        self.pattern = pattern
        self.is_regex = is_regex
        self.order = order
        self.from_analysis = from_analysis
        self.skip_missing = skip_missing
        self.drop_if_missing = drop_if_missing
        self.fallback_to_default = fallback_to_default
        if skip_missing and fallback_to_default:
            raise ArcanaUsageError(
                "Cannot provide both mutually exclusive 'skip_missing' and "
                "'fallback_to_default' flags to {}".format(self))
        # Set when fallback_to_default is True and there are missing matches
        self._derivable = False
        self._fallback = None

    def __eq__(self, other):
        return (self.from_analysis == other.from_analysis
                and self.pattern == other.pattern
                and self.is_regex == other.is_regex
                and self.order == other.order
                and self._dataset == other._dataset
                and self.skip_missing == other.skip_missing
                and self.drop_if_missing == other.drop_if_missing
                and self.fallback_to_default == other.fallback_to_default)

    def __hash__(self):
        return (hash(self.from_analysis)
                ^ hash(self.pattern)
                ^ hash(self.is_regex)
                ^ hash(self.order)
                ^ hash(self._dataset)
                ^ hash(self.skip_missing)
                ^ hash(self.drop_if_missing)
                ^ hash(self.fallback_to_default))

    def initkwargs(self):
        dct = {}
        dct['from_analysis'] = self.from_analysis
        dct['pattern'] = self.pattern
        dct['order'] = self.order
        dct['is_regex'] = self.is_regex
        dct['analysis_'] = self._analysis
        dct['column'] = self._column
        dct['skip_missing'] = self.skip_missing
        dct['drop_if_missing'] = self.drop_if_missing
        dct['fallback_to_default'] = self.fallback_to_default
        return dct

    def nodes(self, tree):
        # Run the match against the tree
        if self.frequency == 'per_session':
            nodes = chain(*(s.sessions for s in tree.subjects))
        elif self.frequency == 'per_subject':
            nodes = tree.subjects
        elif self.frequency == 'per_visit':
            nodes = tree.visits
        elif self.frequency == 'per_dataset':
            nodes = [tree]
        else:
            assert False, "Unrecognised frequency '{}'".format(self.frequency)
        return nodes

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
                            frequency=self.frequency,
                            subject_id=node.subject_id,
                            visit_id=node.visit_id,
                            dataset=self.analysis.dataset,
                            from_analysis=self.from_analysis,
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
        analysis_matches = [d for d in matches
                            if d.from_analysis == self.from_analysis]
        # Select the file_group from the matches
        if not analysis_matches:
            raise ArcanaInputMissingMatchError(
                "No matches found for {} in {} for analysis {}. Found:\n    {}"
                .format(
                    self, node, self.from_analysis,
                    '\n    '.join(str(m) for m in matches)))
        elif self.order is not None:
            try:
                match = analysis_matches[self.order]
            except IndexError:
                raise ArcanaInputMissingMatchError(
                    "Did not find {} named data matching pattern {}, found "
                    " {} in {}".format(self.order, self.pattern,
                                       len(matches), node))
        elif len(analysis_matches) == 1:
            match = analysis_matches[0]
        else:
            raise ArcanaInputError(
                "Found multiple matches for {} in {}:\n    {}"
                .format(self, node,
                        '\n    '.join(str(m) for m in analysis_matches)))
        return match


class FileGroupMatcher(BaseMatcherMixin, BaseFileGroup):
    """
    A pattern that describes a single file_group (typically acquired
    rather than generated but not necessarily) within each session.

    Parameters
    ----------
    valid_formats : list[FileFormat] | FileFormat
        File formats that data will be accepted in
    pattern : str
        A regex pattern to match the file_group names with. Must match
        one and only one file_group per <frequency>. If None, the name
        is used instead.
    is_regex : bool
        Flags whether the pattern is a regular expression or not
    frequency : str
        One of 'per_session', 'per_subject', 'per_visit' and 'per_dataset',
        specifying whether the file_group is present for each session, subject,
        visit or project.
    id : int | None
        To be used to distinguish multiple file_groups that match the
        pattern in the same session. The ID of the file_group within the
        session.
    order : int | None
        To be used to distinguish multiple file_groups that match the
        pattern in the same session. The order of the file_group within the
        session. Based on the scan ID but is more robust to small
        changes to the IDs within the session if for example there are
        two scans of the same type taken before and after a task.
    dicom_tags : dct(str | str)
        To be used to distinguish multiple file_groups that match the
        pattern in the same session. The provided DICOM values dicom
        header values must match exactly.
    from_analysis : str
        The name of the analysis that generated the derived file_group to match.
        Is used to determine the location of the file_groups in the
        dataset as the derived file_groups and fields are grouped by
        the name of the analysis that generated them.
    skip_missing : bool
        If there is no file_group matching the selector for a node then pipelines
        that use it as an input, including downstream pipelines, will not be
        run for that node
    drop_if_missing : bool
        If there are missing file_groups then drop the selector from the analysis
        input. Useful in the case where you want to provide selectors for the
        a list of inputs which may or may not be acquired for a range of
        studies
    fallback_to_default : bool
        If there is no file_group matching the selection for a node
        and corresponding data spec has a default or is a derived spec
        then fallback to the default or generate the derivative.
    acceptable_quality : str | list[str] | None
        An acceptable quality label, or list thereof, to accept, i.e. if a
        file_group's quality label is not in the list it will be ignored. If a
        scan wasn't labelled the value of its qualtiy will be None.
    """

    is_spec = False
    ColumnClass = FileGroupColumn

    def __init__(self, pattern=None, valid_formats=None,
                 frequency='per_session', id=None,
                 order=None, dicom_tags=None, is_regex=False,
                 from_analysis=None, skip_missing=False, drop_if_missing=False,
                 fallback_to_default=False, dataset=None,
                 acceptable_quality=None,
                 analysis_=None, column=None):
        BaseFileGroup.__init__(self, None, frequency)
        BaseMatcherMixin.__init__(self, pattern, is_regex, order,
                                from_analysis, skip_missing, drop_if_missing,
                                fallback_to_default, dataset, analysis_,
                                column)
        self.dicom_tags = dicom_tags
        if order is not None and id is not None:
            raise ArcanaUsageError(
                "Cannot provide both 'order' and 'id' to a file_group"
                "match")
        if valid_formats is not None:
            try:
                valid_formats = tuple(valid_formats)
            except TypeError:
                valid_formats = (valid_formats,)
        self.valid_formats = valid_formats
        self.id = str(id) if id is not None else id
        if isinstance(acceptable_quality, basestring):
            acceptable_quality = (acceptable_quality,)
        elif acceptable_quality is not None:
            acceptable_quality = tuple(acceptable_quality)
        self.acceptable_quality = acceptable_quality

    def __eq__(self, other):
        return (BaseFileGroup.__eq__(self, other) and
                BaseMatcherMixin.__eq__(self, other) and
                self.dicom_tags == other.dicom_tags and
                self.id == other.id and
                self._acceptable_quality == other._acceptable_quality)

    def __hash__(self):
        return (BaseFileGroup.__hash__(self) ^
                BaseMatcherMixin.__hash__(self) ^
                hash(self.dicom_tags) ^
                hash(self.id) ^
                hash(self._acceptable_quality))

    def initkwargs(self):
        dct = BaseFileGroup.initkwargs(self)
        dct.update(BaseMatcherMixin.initkwargs(self))
        dct['dicom_tags'] = self.dicom_tags
        dct['id'] = self.id
        dct['acceptable_quality'] = self.acceptable_quality
        return dct

    def __repr__(self):
        return ("{}(name='{}', format={}, frequency={}, pattern={}, "
                "is_regex={}, order={}, id={}, dicom_tags={}, "
                "from_analysis={}, acceptable_quality={})"
                .format(self.__class__.__name__, self.name, self._format,
                        self.frequency, self.pattern, self.is_regex,
                        self.order, self.id, self.dicom_tags,
                        self.from_analysis, self._acceptable_quality))

    def match(self, tree, valid_formats=None, **kwargs):
        if self.valid_formats is not None:
            valid_formats = self.valid_formats
        else:
            if valid_formats is None:
                raise ArcanaUsageError(
                    "'valid_formats' need to be provided to the 'match' "
                    "method if the FileGroupMatcher ({}) doesn't specify a "
                    "format".format(self))
        # Run the match against the tree
        return FileGroupColumn(self.name,
                            self._match(
                                tree, FileGroup,
                                valid_formats=valid_formats,
                                **kwargs),
                            frequency=self.frequency,
                            candidate_formats=valid_formats)

    def _filtered_matches(self, node, valid_formats=None, **kwargs):  # noqa pylint: disable=unused-argument
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
        if self.id is not None:
            filtered = [d for d in matches if d.id == self.id]
            if not filtered:
                raise ArcanaInputMissingMatchError(
                    "Did not find file_groups names matching pattern {} "
                    "with an id of {} in {}. Found:\n    {} ".format(
                        self.pattern, self.id,
                        '\n    '.join(str(m) for m in matches), node))
            matches = filtered
        if valid_formats is not None:
            format_matches = [
                m for m in matches if any(f.matches(m) for f in valid_formats)]
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


class FieldMatcher(BaseMatcherMixin, BaseField):
    """
    A pattern that matches a single field (typically acquired rather than
    generated but not necessarily) in each session.

    Parameters
    ----------
    pattern : str
        A regex pattern to match the field names with. Must match
        one and only one file_group per <frequency>. If None, the name
        is used instead.
    dtype : type | None
        The datatype of the value. Can be one of (float, int, str). If None
        then the dtype is taken from the FieldSpec that it is bound to
    is_regex : bool
        Flags whether the pattern is a regular expression or not
    frequency : str
        One of 'per_session', 'per_subject', 'per_visit' and 'per_dataset',
        specifying whether the file_group is present for each session, subject,
        visit or project.
    order : int | None
        To be used to distinguish multiple file_groups that match the
        pattern in the same session. The order of the file_group within the
        session. Based on the scan ID but is more robust to small
        changes to the IDs within the session if for example there are
        two scans of the same type taken before and after a task.
    from_analysis : str
        The name of the analysis that generated the derived field to match.
        Is used to determine the location of the fields in the
        dataset as the derived file_groups and fields are grouped by
        the name of the analysis that generated them.
    skip_missing : bool
        If there is no field matching the selector for a node then pipelines
        that use it as an input, including downstream pipelines, will not be
        run for that node
    drop_if_missing : bool
        If there are missing file_groups then drop the selector from the analysis
        input. Useful in the case where you want to provide selectors for the
        a list of inputs which may or may not be acquired for a range of
        studies
    fallback_to_default : bool
        If the there is no file_group/field matching the selection for a node
        and corresponding data spec has a default or is a derived spec,
        then fallback to the default or generate the derivative.
    dataset : Repository | None
        The dataset to draw the matches from, if not the main dataset
        that is used to store the products of the current analysis.
    """

    is_spec = False
    ColumnClass = FieldColumn

    def __init__(self, pattern, dtype=None, frequency='per_session',
                 order=None, is_regex=False, from_analysis=None,
                 skip_missing=False, drop_if_missing=False,
                 fallback_to_default=False, dataset=None, analysis_=None,
                 column=None):
        BaseField.__init__(self, dtype, frequency)
        BaseMatcherMixin.__init__(self, pattern, is_regex, order,
                                from_analysis, skip_missing, drop_if_missing,
                                fallback_to_default, dataset, analysis_,
                                column)

    def __eq__(self, other):
        return (BaseField.__eq__(self, other) and
                BaseMatcherMixin.__eq__(self, other))

    def match(self, tree, **kwargs):
        # Run the match against the tree
        return FieldColumn(self.name,
                          self._match(tree, Field, **kwargs),
                          frequency=self.frequency,
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
        return (BaseField.__hash__(self) ^ BaseMatcherMixin.__hash__(self))

    def initkwargs(self):
        dct = BaseField.initkwargs(self)
        dct.update(BaseMatcherMixin.initkwargs(self))
        return dct

    def _filtered_matches(self, node, **kwargs):
        if self.is_regex:
            pattern_re = re.compile(self.pattern)
            matches = [f for f in node.fields
                       if pattern_re.match(f.name)]
        else:
            matches = [f for f in node.fields
                       if f.name == self.pattern]
        if self.from_analysis is not None:
            matches = [f for f in matches
                       if f.from_analysis == self.from_analysis]
        if not matches:
            raise ArcanaInputMissingMatchError(
                "Did not find any matches for {} in {}. Found:\n    {}"
                .format(self, node,
                        '\n    '.join(f.name for f in node.fields)))
        return matches

    def __repr__(self):
        return ("{}(name='{}', dtype={}, frequency={}, pattern={}, "
                "is_regex={}, order={}, from_analysis={})"
                .format(self.__class__.__name__, self.name, self._dtype,
                        self.frequency, self.pattern, self.is_regex,
                        self.order, self.from_analysis))

    @property
    def _specific_kwargs(self):
        return {'dtype': self.dtype}
