from copy import copy
from arcana2.exceptions import (
    ArcanaError, ArcanaUsageError, ArcanaIndexError)
from .base import FileGroupMixin, FieldMixin
from .item import FileGroup, Field
from collections import OrderedDict
from operator import itemgetter
from itertools import chain


DICOM_SERIES_NUMBER_TAG = ('0020', '0011')


class DataColumn(object):
    """
    Base class for a "column" of file_groups and field items
    """

    def __init__(self, column, tree_level):
        self._tree_level = tree_level
        if tree_level == 'per_dataset':
            # If wrapped in an iterable
            if not isinstance(column, self.RowClass):
                if len(column) > 1:
                    raise ArcanaUsageError(
                        "More than one {} passed to {}"
                        .format(self.CONTAINED_CLASS.__name__,
                                type(self).__name__))
                column = list(column)
            self._column = column
        elif tree_level == 'per_session':
            self._column = OrderedDict()
            for subj_id in sorted(set(c.subject_id for c in column)):
                self._column[subj_id] = OrderedDict(
                    sorted(((c.visit_id, c) for c in column
                            if c.subject_id == subj_id),
                           key=itemgetter(0)))
        elif tree_level == 'per_subject':
            self._column = OrderedDict(
                sorted(((c.subject_id, c) for c in column),
                       key=itemgetter(0)))
        elif tree_level == 'per_visit':
            self._column = OrderedDict(
                sorted(((c.visit_id, c) for c in column),
                       key=itemgetter(0)))
        else:
            assert False
        for datum in self:
            if not isinstance(datum, self.RowClass):
                raise ArcanaUsageError(
                    "Invalid class {} in {}".format(datum, self))

    def __iter__(self):
        if self._tree_level == 'per_dataset':
            return iter(self._column)
        elif self._tree_level == 'per_session':
            return chain(*(c.values()
                           for c in self._column.values()))
        else:
            return iter(self._column.values())

    def __len__(self):
        if self.tree_level == 'per_session':
            ln = sum(len(d) for d in self._column.values())
        else:
            ln = len(self._column)
        return ln

    def _common_attr(self, column, attr_name, ignore_none=True):
        attr_set = set(getattr(c, attr_name) for c in column)
        if ignore_none:
            attr_set -= set([None])
        if len(attr_set) > 1:
            raise ArcanaUsageError(
                "Heterogeneous attributes for '{}' within {}".format(
                    attr_name, self))
        try:
            return next(iter(attr_set))
        except StopIteration:
            return None

    def item(self, subject_id=None, visit_id=None):
        """
        Returns a particular file_group|field in the column corresponding to
        the given subject and visit_ids. subject_id and visit_id must be
        provided for relevant frequencies. Note that subject_id/visit_id can
        also be provided for non-relevant frequencies, they will just be
        ignored.

        Parameter
        ---------
        subject_id : str
            The subject id of the item to return
        visit_id : str
            The visit id of the item to return
        """

        if self.tree_level == 'per_session':
            if subject_id is None or visit_id is None:
                raise ArcanaError(
                    "The 'subject_id' ({}) and 'visit_id' ({}) must be "
                    "provided to get an item from {}".format(
                        subject_id, visit_id, self))
            try:
                subj_dct = self._column[subject_id]
            except KeyError:
                raise ArcanaIndexError(
                    subject_id,
                    "{} not a subject ID in '{}' column ({})"
                    .format(subject_id, self.name,
                            ', '.join(self._column.keys())))
            try:
                file_group = subj_dct[visit_id]
            except KeyError:
                raise ArcanaIndexError(
                    visit_id,
                    "{} not a visit ID in subject {} of '{}' "
                    "column ({})"
                    .format(visit_id, subject_id, self.name,
                            ', '.join(subj_dct.keys())))
        elif self.tree_level == 'per_subject':
            if subject_id is None:
                raise ArcanaError(
                    "The 'subject_id' arg must be provided to get "
                    "the match from {}"
                    .format(self))
            try:
                file_group = self._column[subject_id]
            except KeyError:
                raise ArcanaIndexError(
                    subject_id,
                    "{} not a subject ID in '{}' column ({})"
                    .format(subject_id, self.name,
                            ', '.join(self._column.keys())))
        elif self.tree_level == 'per_visit':
            if visit_id is None:
                raise ArcanaError(
                    "The 'visit_id' arg must be provided to get "
                    "the match from {}"
                    .format(self))
            try:
                file_group = self._column[visit_id]
            except KeyError:
                raise ArcanaIndexError(
                    visit_id,
                    "{} not a visit ID in '{}' column ({})"
                    .format(visit_id, self.name,
                            ', '.join(self._column.keys())))
        elif self.tree_level == 'per_dataset':
            try:
                file_group = self._column[0]
            except IndexError:
                raise ArcanaIndexError(
                    0, ("'{}' Column is empty so doesn't have a "
                        + "per_dataset node").format(self.name))
        return file_group

    @property
    def slice(self):
        "Used for duck typing Column objects with Spec and Match "
        "in source and sink initiation"
        return self

    def bind(self, analysis, **kwargs):
        """
        Used for duck typing Column objects with Spec and Match
        in source and sink initiation. Checks IDs match sessions in analysis.
        """
        if self.tree_level == 'per_subject':
            tree_subject_ids = list(analysis.dataset.tree.subject_ids)
            subject_ids = list(self._column.keys())
            if tree_subject_ids != subject_ids:
                raise ArcanaUsageError(
                    "Subject IDs in column provided to '{}' ('{}') "
                    "do not match Analysis tree ('{}')".format(
                        self.name, "', '".join(subject_ids),
                        "', '".join(tree_subject_ids)))
        elif self.tree_level == 'per_visit':
            tree_visit_ids = list(analysis.dataset.tree.visit_ids)
            visit_ids = list(self._column.keys())
            if tree_visit_ids != visit_ids:
                raise ArcanaUsageError(
                    "Subject IDs in column provided to '{}' ('{}') "
                    "do not match Analysis tree ('{}')".format(
                        self.name, "', '".join(visit_ids),
                        "', '".join(tree_visit_ids)))
        elif self.tree_level == 'per_session':
            for subject in analysis.dataset.tree.subjects:
                if subject.id not in self._column:
                    raise ArcanaUsageError(
                        "Analysis subject ID '{}' was not found in colleciton "
                        "provided to '{}' (found '{}')".format(
                            subject.id, self.name,
                            "', '".join(self._column.keys())))
                for session in subject.sessions:
                    if session.visit_id not in self._column[subject.id]:
                        raise ArcanaUsageError(
                            ("Analysis visit ID '{}' for subject '{}' was not "
                             + "found in colleciton provided to '{}' "
                             + "(found '{}')").format(
                                 subject.id, self.name,
                                 "', '".join(
                                     self._column[subject.id].keys())))

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        self._name = name


class FileGroupColumn(DataColumn, FileGroupMixin):
    """
    A "column" of file_groups within a dataset

    Parameters
    ----------
    name : str
        Name of the column
    items : List[FileGroup]
        An iterable of file_groups
    tree_level : TreeLevel
        The level within the dataset tree that the data items sit, i.e. 
        per 'session', 'subject', 'visit', 'group_visit', 'group' or 'dataset'
    format : FileFormat | None
        The file format of the column (will be determined from file_groups
        if not provided).
    """

    RowClass = FileGroup

    def __init__(self, name, items, format=None, tree_level=None,
                 candidate_formats=None):
        if format is None and candidate_formats is None:
            formats = set(d.format for d in column)
            if len(formats) > 1:
                raise ArcanaUsageError(
                    "Either 'format' or candidate_formats needs to be supplied"
                    " during the initialisation of a FileGroupColumn ('{}') with "
                    "heterogeneous formats".format(name))
            format = next(iter(formats))
        column = list(column)
        if not column:
            if format is None:
                format = candidate_formats[0]
            if tree_level is None:
                raise ArcanaUsageError(
                    "Need to provide explicit tree_level for empty "
                    "FileGroupColumn")
        else:
            implicit_tree_level = self._common_attr(column,
                                                   'tree_level')
            if tree_level is None:
                tree_level = implicit_tree_level
            elif tree_level != implicit_tree_level:
                raise ArcanaUsageError(
                    "Implicit tree_level '{}' does not match explicit "
                    "tree_level '{}' for '{}' FileGroupColumn"
                    .format(implicit_tree_level, tree_level, name))
            formatted_column = []
            for file_group in column:
                file_group = copy(file_group)
                if file_group.exists and file_group.format is None:
                    file_group.format = (file_group.detect_format(candidate_formats)
                                      if format is None else format)
                formatted_column.append(file_group)
            column = formatted_column
            format = self._common_attr(column, 'format')
        FileGroupMixin.__init__(self, name, format, tree_level=tree_level)
        DataColumn.__init__(self, column, tree_level)

    def path(self, subject_id=None, visit_id=None):
        return self.item(
            subject_id=subject_id, visit_id=visit_id).path


class FieldColumn(DataColumn, FieldMixin):
    """
    A "column" of fields within a dataset

    Parameters
    ----------
    name : str
        Name of the column
    column : List[Field]
        An iterable of Fields
    """

    RowClass = Field

    def __init__(self, name, column, tree_level=None, dtype=None,
                 array=None):
        column = list(column)
        if column:
            implicit_tree_level = self._common_attr(column,
                                                   'tree_level')
            if tree_level is None:
                tree_level = implicit_tree_level
            elif tree_level != implicit_tree_level:
                raise ArcanaUsageError(
                    "Implicit tree_level '{}' does not match explicit "
                    "tree_level '{}' for '{}' FieldColumn"
                    .format(implicit_tree_level, tree_level, name))
            implicit_dtype = self._common_attr(column, 'dtype')
            if dtype is None:
                dtype = implicit_dtype
            elif dtype != implicit_dtype:
                raise ArcanaUsageError(
                    "Implicit dtype '{}' does not match explicit "
                    "dtype '{}' for '{}' FieldColumn"
                    .format(implicit_dtype, dtype, name))
            implicit_array = self._common_attr(column, 'array')
            if array is None:
                array = implicit_array
            elif array != implicit_array:
                raise ArcanaUsageError(
                    "Implicit array '{}' does not match explicit "
                    "array '{}' for '{}' FieldColumn"
                    .format(implicit_array, array, name))
        if tree_level is None:
            raise ArcanaUsageError(
                "Need to provide explicit tree_level for empty "
                "FieldColumn")
        if dtype is None:
            raise ArcanaUsageError(
                "Need to provide explicit dtype for empty "
                "FieldColumn")
        FieldMixin.__init__(self, name, dtype=dtype, tree_level=tree_level,
                           array=array)
        DataColumn.__init__(self, column, tree_level)

    def value(self, subject_id=None, visit_id=None):
        return self.item(subject_id=subject_id, visit_id=visit_id).value
