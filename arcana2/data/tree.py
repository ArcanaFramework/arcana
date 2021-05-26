from builtins import zip
from builtins import object
import weakref
from enum import Enum
from itertools import chain, groupby
from collections import defaultdict
from operator import attrgetter, itemgetter
from collections import OrderedDict
import logging
# from .base import FileGroupMixin, FieldMixin
from arcana2.utils import split_extension
from arcana2.exceptions import (
    ArcanaNameError, ArcanaRepositoryError, ArcanaUsageError)

id_getter = attrgetter('id')

logger = logging.getLogger('arcana')


class DataFreq(Enum):
    """
    The frequency at which a data item is stored within a data tree. For
    typical neuroimaging analysis these levels are hardcoded to one of six
    values.
    """

    visit = 0b001  # for each visit (e.g. longitudinal timepoint)
    subject = 0b010  # for each subject
    group = 0b100  #for each subject group
    session = 0b011  # for each session (i.e. a single visit of a subject)
    group_visit = 0b101  # for each combination of subject group and visit
    dataset = 0b000  # singular within the dataset

    # The value for each enum is a binary string that specifies the depth of
    # the tree by its length (i.e. 3), and which level needs to be iterated
    # over for each frequency. Note that patterns 0b110 and 0b111 are not
    # required because it is assumed that each subject can only belong to one
    # group

    def __init__(self, value):
        if value in (0b010, 0b011):
            value += 0b100
        super().__init__(value)


    def __str__(self):
        return self.name

    @property
    def bases(self):
        """Returns the bases the frequency is composed of, e.g.
           session -> subject + visit
           group_visit -> group + visit
        """
        n = self.value
        bases = []
        while n:
            m = n & (n - 1)
            bases.append(type(self)(m ^ n))
            n = m
        return bases

    def is_base(self):
        return len(self.bases) == 1


class DataNode():

    def __init__(self, id=None, base_ids=None):
        self.file_groups = OrderedDict()
        self.fields = OrderedDict()
        self.records = OrderedDict()
        self.id = id
        self.base_ids = base_ids

        # self, file_groups, fields, records):
        # if file_groups is None:
        #     file_groups = []
        # if fields is None:
        #     fields = []
        # if records is None:
        #     records = []
        # # Save file_groups and fields in ordered dictionary by name and
        # # name of analysis that generated them (if applicable)
        # self._file_groups = OrderedDict()
        # for file_group in sorted(file_groups):
        #     try:
        #         dct = self._file_groups[file_group.path]
        #     except KeyError:
        #         dct = self._file_groups[file_group.path] = OrderedDict()
        #     if file_group.resource_name is not None:
        #         format_key = file_group.resource_name
        #     else:
        #         format_key = split_extension(file_group.local_path)[1]
        #     if format_key in dct:
        #         raise ArcanaRepositoryError(
        #             "Attempting to add duplicate file_groups to tree ({} and "
        #             "{})".format(file_group, dct[format_key]))
        #     dct[format_key] = file_group
        # self._fields = OrderedDict((f.pathj, f) for f in sorted(fields))
        # self._records = OrderedDict(
        #     (r.pipeline_name, r)
        #     for r in sorted(records, key=lambda r: (r.subject_id, r.visit_id)))
        # self._missing_records = []
        # self._duplicate_records = []
        # self._tree = None
        # # Match up provenance records with items in the node
        # for item in chain(self.file_groups, self.fields):
        #     if not item.derived:
        #         continue  # Skip acquired items
        #     records = [r for r in self.records
        #                if (item.namespace == r.namespace
        #                    and item.name in r.outputs)]
        #     if not records:
        #         self._missing_records.append(item.name)
        #     elif len(records) > 1:
        #         item.record = sorted(records, key=attrgetter('datetime'))[-1]
        #         self._duplicate_records.append(item.name)
        #     else:
        #         item.record = records[0]

    def __eq__(self, other):
        if not (isinstance(other, type(self))
                or isinstance(self, type(other))):
            return False
        return (tuple(self.file_groups) == tuple(other.file_groups)
                and tuple(self.fields) == tuple(other.fields)
                and tuple(self.records) == tuple(other.records))

    def __hash__(self):
        return (hash(tuple(self.file_groups)) ^ hash(tuple(self.fields))
                ^ hash(tuple(self.fields)))

    @property
    def file_groups(self):
        return chain(*(d.values() for d in self._file_groups.values()))

    @property
    def fields(self):
        return self._fields.values()

    @property
    def records(self):
        return self._records.values()

    def file_group(self, path, file_format=None):
        """
        Gets the file_group with the ID 'id' produced by the Analysis named
        'analysis' if provided. If a spec is passed instead of a str to the
        name argument, then the analysis will be set from the spec iff it is
        derived

        Parameters
        ----------
        path : str
            The path to the file_group within the tree node, e.g. anat/T1w
        file_format : FileFormat | Sequence[FileFormat] | None
            A file format, or sequence of file formats, which are used to
            resolve the format of the file-group

        Returns
        -------
        FileGroup | UnresolvedFormatFileGroup
            The file-group corresponding to the given path. If a, or
            multiple, candidate file formats are provided then the format of
            the file-group is resolved and a FileGroup object is returned.
            Otherwise, an UnresolvedFormatFileGroup is returned instead.
        """
        # if id.is_file_group:
        #     if namespace is None and id.derived:
        #         namespace = id.analysis.name
        #     id = id.name
        try:
            file_group = self._file_groups[path]
        except KeyError:
            raise ArcanaNameError(
                path,
                (f"{self} doesn't have a file_group at the path {path} "
                 "(available '{}')".format("', '".join(self._file_groups))))
        else:
            if file_format is not None:
                file_group = file_group.resolve_format(file_format)
        return file_group

    def field(self, path):
        """
        Gets the field named 'name' produced by the Analysis named 'analysis'
        if provided. If a spec is passed instead of a str to the name argument,
        then the analysis will be set from the spec iff it is derived

        Parameters
        ----------
        path : str
            The path of the field within the node
        """
        # if isinstance(name, FieldMixin):
        #     if namespace is None and name.derived:
        #         namespace = name.analysis.name
        #     name = name.name
        try:
            return self._fields[path]
        except KeyError:
            raise ArcanaNameError(
                path, ("{} doesn't have a field named '{}' "
                       "(available '{}')").format(
                           self, path, "', '".join(self._fields)))

    def record(self, path):
        """
        Returns the provenance record for a given pipeline

        Parameters
        ----------
        path : str
            The name of the pipeline that generated the record

        Returns
        -------
        record : arcana2.provenance.Record
            The provenance record generated by the specified pipeline
        """
        try:
            return self._records[path]
        except KeyError:
            raise ArcanaNameError(
                path,
                ("{} doesn't have a provenance record for '{}' "
                 "(found {})".format(self, path, '; '.join(self._records))))

    @property
    def data(self):
        return chain(self.file_groups, self.fields)

    def __ne__(self, other):
        return not (self == other)

    def find_mismatch(self, other, indent=''):
        """
        Highlights where two nodes differ in a human-readable form

        Parameters
        ----------
        other : TreeNode
            The node to compare
        indent : str
            The white-space with which to indent output string

        Returns
        -------
        mismatch : str
            The human-readable mismatch string
        """
        if self != other:
            mismatch = "\n{}{}".format(indent, type(self).__name__)
        else:
            mismatch = ''
        sub_indent = indent + '  '
        if len(list(self.file_groups)) != len(list(other.file_groups)):
            mismatch += ('\n{indent}mismatching summary file_group lengths '
                         '(self={} vs other={}): '
                         '\n{indent}  self={}\n{indent}  other={}'
                         .format(len(list(self.file_groups)),
                                 len(list(other.file_groups)),
                                 list(self.file_groups),
                                 list(other.file_groups),
                                 indent=sub_indent))
        else:
            for s, o in zip(self.file_groups, other.file_groups):
                mismatch += s.find_mismatch(o, indent=sub_indent)
        if len(list(self.fields)) != len(list(other.fields)):
            mismatch += ('\n{indent}mismatching summary field lengths '
                         '(self={} vs other={}): '
                         '\n{indent}  self={}\n{indent}  other={}'
                         .format(len(list(self.fields)),
                                 len(list(other.fields)),
                                 list(self.fields),
                                 list(other.fields),
                                 indent=sub_indent))
        else:
            for s, o in zip(self.fields, other.fields):
                mismatch += s.find_mismatch(o, indent=sub_indent)
        return mismatch

    @property
    def tree(self):
        if self._tree is None:
            raise ArcanaUsageError("{} has not been added to a tree"
                                   .format(self))
        return self._tree()  # tree will be a weakref if present

    @tree.setter
    def tree(self, tree):
        if self._tree is not None:
            raise ArcanaUsageError("{} already has a tree {}"
                                   .format(self, self._tree()))
        self._tree = weakref.ref(tree)

    def __getstate__(self):
        if self._tree is not None:
            dct = self.__dict__.copy()
            dct['_tree'] = dct['_tree']()
        else:
            dct = self.__dict__
        return dct

    def __setstate__(self, state):
        self.__dict__ = state.copy()
        if self._tree is not None:
            self._tree = weakref.ref(self._tree)


class DataTree():

    def __init__(self, frequencies):
        self.frequencies = frequencies
        self.nodes = defaultdict(dict)

    def add_node(self, ids, file_groups, fields, records):
        self.nodes
