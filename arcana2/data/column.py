from copy import copy
from arcana2.exceptions import (
    ArcanaError, ArcanaUsageError, ArcanaIndexError)
from .base import FileGroupMixin, FieldMixin
from .item import FileGroup, Field
from collections import OrderedDict
from operator import itemgetter
from itertools import chain


DICOM_SERIES_NUMBER_TAG = ('0020', '0011')


class DataColumn():
    """
    Base class for a "column" of file_groups and field items
    """

    def __init__(self, items, frequency):
        self._frequency = frequency
        if frequency == 'per_dataset':
            # If wrapped in an iterable
            if not isinstance(items, self.RowClass):
                if len(items) > 1:
                    raise ArcanaUsageError(
                        "More than one {} passed to {}"
                        .format(self.CONTAINED_CLASS.__name__,
                                type(self).__name__))
                items = list(items)
            self._items = items
        elif frequency == 'per_session':
            self._items = OrderedDict()
            for subj_id in sorted(set(c.subject_id for c in items)):
                self._items[subj_id] = OrderedDict(
                    sorted(((c.visit_id, c) for c in items
                            if c.subject_id == subj_id),
                           key=itemgetter(0)))
        elif frequency == 'per_subject':
            self._items = OrderedDict(
                sorted(((c.subject_id, c) for c in items),
                       key=itemgetter(0)))
        elif frequency == 'per_visit':
            self._items = OrderedDict(
                sorted(((c.visit_id, c) for c in items),
                       key=itemgetter(0)))
        else:
            assert False
        for datum in self:
            if not isinstance(datum, self.RowClass):
                raise ArcanaUsageError(
                    "Invalid class {} in {}".format(datum, self))

    def __iter__(self):
        if self._frequency == 'per_dataset':
            return iter(self._items)
        elif self._frequency == 'per_session':
            return chain(*(c.values()
                           for c in self._items.values()))
        else:
            return iter(self._items.values())

    def __len__(self):
        if self.frequency == 'per_session':
            ln = sum(len(d) for d in self._items.values())
        else:
            ln = len(self._items)
        return ln

    def _common_attr(self, items, attr_name, ignore_none=True):
        attr_set = set(getattr(c, attr_name) for c in items)
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
        Returns a particular file_group|field in the columns corresponding to
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

        if self.frequency == 'per_session':
            if subject_id is None or visit_id is None:
                raise ArcanaError(
                    "The 'subject_id' ({}) and 'visit_id' ({}) must be "
                    "provided to get an item from {}".format(
                        subject_id, visit_id, self))
            try:
                subj_dct = self._items[subject_id]
            except KeyError:
                raise ArcanaIndexError(
                    subject_id,
                    "{} not a subject ID in '{}' items ({})"
                    .format(subject_id, self.path,
                            ', '.join(self._items.keys())))
            try:
                file_group = subj_dct[visit_id]
            except KeyError:
                raise ArcanaIndexError(
                    visit_id,
                    "{} not a visit ID in subject {} of '{}' "
                    "items ({})"
                    .format(visit_id, subject_id, self.path,
                            ', '.join(subj_dct.keys())))
        elif self.frequency == 'per_subject':
            if subject_id is None:
                raise ArcanaError(
                    "The 'subject_id' arg must be provided to get "
                    "the match from {}"
                    .format(self))
            try:
                file_group = self._items[subject_id]
            except KeyError:
                raise ArcanaIndexError(
                    subject_id,
                    "{} not a subject ID in '{}' items ({})"
                    .format(subject_id, self.path,
                            ', '.join(self._items.keys())))
        elif self.frequency == 'per_visit':
            if visit_id is None:
                raise ArcanaError(
                    "The 'visit_id' arg must be provided to get "
                    "the match from {}"
                    .format(self))
            try:
                file_group = self._items[visit_id]
            except KeyError:
                raise ArcanaIndexError(
                    visit_id,
                    "{} not a visit ID in '{}' items ({})"
                    .format(visit_id, self.path,
                            ', '.join(self._items.keys())))
        elif self.frequency == 'per_dataset':
            try:
                file_group = self._items[0]
            except IndexError:
                raise ArcanaIndexError(
                    0, ("'{}' Column is empty so doesn't have a "
                        + "per_dataset node").format(self.path))
        return file_group

    # @property
    # def slice(self):
    #     "Used for duck typing Column objects with Spec and Match "
    #     "in source and sink initiation"
    #     return self

    # def bind(self, analysis, **kwargs):
    #     """
    #     Used for duck typing Column objects with Spec and Match
    #     in source and sink initiation. Checks IDs match sessions in analysis.
    #     """
    #     if self.frequency == 'per_subject':
    #         tree_subject_ids = list(analysis.dataset.tree.subject_ids)
    #         subject_ids = list(self._column.keys())
    #         if tree_subject_ids != subject_ids:
    #             raise ArcanaUsageError(
    #                 "Subject IDs in column provided to '{}' ('{}') "
    #                 "do not match Analysis tree ('{}')".format(
    #                     self.name, "', '".join(subject_ids),
    #                     "', '".join(tree_subject_ids)))
    #     elif self.frequency == 'per_visit':
    #         tree_visit_ids = list(analysis.dataset.tree.visit_ids)
    #         visit_ids = list(self._column.keys())
    #         if tree_visit_ids != visit_ids:
    #             raise ArcanaUsageError(
    #                 "Subject IDs in column provided to '{}' ('{}') "
    #                 "do not match Analysis tree ('{}')".format(
    #                     self.name, "', '".join(visit_ids),
    #                     "', '".join(tree_visit_ids)))
    #     elif self.frequency == 'per_session':
    #         for subject in analysis.dataset.tree.subjects:
    #             if subject.id not in self._column:
    #                 raise ArcanaUsageError(
    #                     "Analysis subject ID '{}' was not found in colleciton "
    #                     "provided to '{}' (found '{}')".format(
    #                         subject.id, self.name,
    #                         "', '".join(self._column.keys())))
    #             for session in subject.sessions:
    #                 if session.visit_id not in self._column[subject.id]:
    #                     raise ArcanaUsageError(
    #                         ("Analysis visit ID '{}' for subject '{}' was not "
    #                          + "found in colleciton provided to '{}' "
    #                          + "(found '{}')").format(
    #                              subject.id, self.name,
    #                              "', '".join(
    #                                  self._column[subject.id].keys())))


class FileGroupColumn(DataColumn, FileGroupMixin):
    """
    A "column" of file_groups within a dataset

    Parameters
    ----------
    items : List[FileGroup]
        An iterable of file_groups
    frequency : DataFreq
        The frequency that the items occur in the dataset, i.e. 
        per 'session', 'subject', 'visit', 'group_visit', 'group' or 'dataset'
    format : FileFormat | None
        The file format of the column (will be determined from items
        if not provided).
    """

    RowClass = FileGroup

    def __init__(self, items, format=None, frequency=None,
                 candidate_formats=None):
        if format is None and candidate_formats is None:
            formats = set(d.format for d in items)
            if len(formats) > 1:
                raise ArcanaUsageError(
                    "Either 'format' or candidate_formats needs to be supplied"
                    " during the initialisation of a FileGroupColumn with "
                    "heterogeneous formats")
            format = next(iter(formats))
        items = list(items)
        if not items:
            if format is None:
                format = candidate_formats[0]
            if frequency is None:
                raise ArcanaUsageError(
                    "Need to provide explicit frequency for empty "
                    "FileGroupColumn")
        else:
            implicit_frequency = self._common_attr(items,
                                                   'frequency')
            if frequency is None:
                frequency = implicit_frequency
            elif frequency != implicit_frequency:
                raise ArcanaUsageError(
                    "Implicit frequency '{}' does not match explicit "
                    "frequency '{}' for FileGroupColumn"
                    .format(implicit_frequency, frequency))
            formatted_items = []
            for file_group in items:
                file_group = copy(file_group)
                if file_group.exists and file_group.format is None:
                    file_group.format = (
                        file_group.detect_format(candidate_formats)
                        if format is None else format)
                formatted_items.append(file_group)
            items = formatted_items
            path = self._common_attr(items, 'path')
            format = self._common_attr(items, 'format')
        FileGroupMixin.__init__(self, path, format, frequency=frequency)
        DataColumn.__init__(self, items, frequency)

    def path(self, subject_id=None, visit_id=None):
        return self.item(
            subject_id=subject_id, visit_id=visit_id).path


class FieldColumn(DataColumn, FieldMixin):
    """
    A "column" of fields within a dataset

    Parameters
    ----------
    items : List[Field]
        An iterable of Fields
    frequency : DataFreq
        The frequency that the items occur in the dataset, i.e. 
        per 'session', 'subject', 'visit', 'group_visit', 'group' or 'dataset'
    dtype : type
        The data type ofthe column (will be determined from items
        if not provided).
    """

    RowClass = Field

    def __init__(self, items, frequency=None, dtype=None, array=None):
        items = list(items)
        if items:
            implicit_frequency = self._common_attr(items,
                                                   'frequency')
            path = self._common_attr(items, 'path')
            if frequency is None:
                frequency = implicit_frequency
            elif frequency != implicit_frequency:
                raise ArcanaUsageError(
                    "Implicit frequency '{}' does not match explicit "
                    "frequency '{}' for FieldColumn (of common path: {})"
                    .format(implicit_frequency, frequency, path))
            implicit_dtype = self._common_attr(items, 'dtype')
            if dtype is None:
                dtype = implicit_dtype
            elif dtype != implicit_dtype:
                raise ArcanaUsageError(
                    "Implicit dtype '{}' does not match explicit "
                    "dtype '{}' for '{}' FieldColumn (of common path: {})"
                    .format(implicit_dtype, dtype, path))
            implicit_array = self._common_attr(items, 'array')
            if array is None:
                array = implicit_array
            elif array != implicit_array:
                raise ArcanaUsageError(
                    "Implicit array '{}' does not match explicit "
                    "array '{}' for '{}' FieldColumn (of common path: {})"
                    .format(implicit_array, array, path))
        if frequency is None:
            raise ArcanaUsageError(
                "Need to provide explicit frequency for empty "
                "FieldColumn")
        if dtype is None:
            raise ArcanaUsageError(
                "Need to provide explicit dtype for empty "
                "FieldColumn")
        FieldMixin.__init__(self, path, dtype=dtype, frequency=frequency,
                           array=array)
        DataColumn.__init__(self, items, frequency)

    def value(self, subject_id=None, visit_id=None):
        return self.item(subject_id=subject_id, visit_id=visit_id).value
