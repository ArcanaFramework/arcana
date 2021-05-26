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

    # Name = (iteration-pattern, description)
    visit = (0b001, 'for each visit across the whole cohort '
             '(e.g. longitudinal timepoint)')    
    subject = (0b010, 'for each subject')
    group = (0b100, 'for each subject group')
    session = (0b011, 'for each session (i.e. a single visit of a subject)')
    group_visit = (
        0b101, 'for each combination of subject group and visit, '
        'e.g. a timepoint for a specific group in a longitudinal study')
    dataset = (0b000, "singular within the dataset")

    # The 'iteration-pattern' is a binary string that specifies the depth of
    # the tree by its length (i.e. 3), and which level needs to be iterated
    # over for each frequency. Note that patterns 0b110 and 0b111 are not
    # required because it is assumed that each subject can only belong to one
    # group

    @property
    def desc(self):
        return self.value[1]

    @property
    def iteration_pattern(self):
        return self.value[0]

    def __str__(self):
        return self.name


class TreeNode(object):

    def __init__(self, file_groups, fields, records):
        if file_groups is None:
            file_groups = []
        if fields is None:
            fields = []
        if records is None:
            records = []
        # Save file_groups and fields in ordered dictionary by name and
        # name of analysis that generated them (if applicable)
        self._file_groups = OrderedDict()
        for file_group in sorted(file_groups):
            id_key = (file_group.id, file_group.namespace)
            try:
                dct = self._file_groups[id_key]
            except KeyError:
                dct = self._file_groups[id_key] = OrderedDict()
            if file_group.format_name is not None:
                format_key = file_group.format_name
            else:
                format_key = split_extension(file_group.path)[1]
            if format_key in dct:
                raise ArcanaRepositoryError(
                    "Attempting to add duplicate file_groups to tree ({} and {})"
                    .format(file_group, dct[format_key]))
            dct[format_key] = file_group
        self._fields = OrderedDict(((f.name, f.namespace), f)
                                   for f in sorted(fields))
        self._records = OrderedDict(
            ((r.pipeline_name, r.namespace), r)
            for r in sorted(records, key=lambda r: (r.subject_id, r.visit_id,
                                                    r.namespace)))
        self._missing_records = []
        self._duplicate_records = []
        self._tree = None
        # Match up provenance records with items in the node
        for item in chain(self.file_groups, self.fields):
            if not item.derived:
                continue  # Skip acquired items
            records = [r for r in self.records
                       if (item.namespace == r.namespace
                           and item.name in r.outputs)]
            if not records:
                self._missing_records.append(item.name)
            elif len(records) > 1:
                item.record = sorted(records, key=attrgetter('datetime'))[-1]
                self._duplicate_records.append(item.name)
            else:
                item.record = records[0]

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

    @property
    def subject_id(self):
        "To be overridden by subclasses where appropriate"
        return None

    @property
    def visit_id(self):
        "To be overridden by subclasses where appropriate"
        return None

    def file_group(self, id, format=None):
        """
        Gets the file_group with the ID 'id' produced by the Analysis named
        'analysis' if provided. If a spec is passed instead of a str to the
        name argument, then the analysis will be set from the spec iff it is
        derived

        Parameters
        ----------
        id : str | FileGroupSpec
            The name of the file_group or a spec matching the given name
            format : FileFormat | str | None
            Either the format of the file_group to return or the name of the
            format. If None and only a single file_group is found for the given
            name and analysis then that is returned otherwise an exception is
            raised
        """
        # if id.is_file_group:
        #     if namespace is None and id.derived:
        #         namespace = id.analysis.name
        #     id = id.name
        try:
            format_dct = self._file_groups[(id, namespace)]
        except KeyError:
            available = [
                ('{}(format={})'.format(f.id, f.resource_name)
                 if f.resource_name is not None else f.id)
                for f in self.file_groups if f.namespace == namespace]
            other_analyses = [
                (f.namespace if f.namespace is not None else '<root>')
                for f in self.file_groups if f.id == id]
            if other_analyses:
                msg = (". NB: matching file_group(s) found for '{}' analysis(es) "
                       "('{}')".format(id, "', '".join(other_analyses)))
            else:
                msg = ''
            raise ArcanaNameError(
                id,
                ("{} doesn't have a file_group named '{}'{} "
                 "(available '{}'){}"
                 .format(self, id,
                         (" from analysis '{}'".format(namespace)
                          if namespace is not None else ''),
                         "', '".join(available), msg)))
        else:
            if format is None:
                all_formats = list(format_dct.values())
                if len(all_formats) > 1:
                    raise ArcanaNameError(
                        id,
                        "Multiple file_groups found for '{}'{} in {} with formats"
                        " {}. Need to specify a format"
                        .format(id, ("in '{}'".format(namespace)
                                     if namespace is not None else ''),
                                self, "', '".join(format_dct.keys())))
                file_group = all_formats[0]
            else:
                try:
                    if isinstance(format, str):
                        file_group = format_dct[format]
                    else:
                        try:
                            file_group = format_dct[format.ext]
                        except KeyError:
                            file_group = None
                            for rname, rfile_group in format_dct.items():
                                if rname in format.resource_names(
                                        self.tree.dataset.repository.type):  # noqa pylint: disable=no-member
                                    file_group = rfile_group
                                    break
                            if file_group is None:
                                raise
                except KeyError:
                    raise ArcanaNameError(
                        format,
                        ("{} doesn't have a file_group named '{}'{} with "
                         "format '{}' (available '{}')"
                         .format(self, id,
                                 (" from analysis '{}'".format(namespace)
                                  if namespace is not None else ''),
                                 format, "', '".join(format_dct.keys()))))

        return file_group

    def field(self, name, namespace=None):
        """
        Gets the field named 'name' produced by the Analysis named 'analysis'
        if provided. If a spec is passed instead of a str to the name argument,
        then the analysis will be set from the spec iff it is derived

        Parameters
        ----------
        name : str | FieldMixin
            The name of the field or a spec matching the given name
        analysis : str | None
            Name of the analysis that produced the field if derived. If None
            and a spec is passed instaed of string to the name argument then
            the analysis name will be taken from the spec instead.
        """
        # if isinstance(name, FieldMixin):
        #     if namespace is None and name.derived:
        #         namespace = name.analysis.name
        #     name = name.name
        try:
            return self._fields[(name, namespace)]
        except KeyError:
            available = [d.name for d in self.fields
                         if d.namespace == namespace]
            other_analyses = [(d.namespace if d.namespace is not None
                               else '<root>')
                              for d in self.fields if d.name == name]
            if other_analyses:
                msg = (". NB: matching field(s) found for '{}' analysis(ies) "
                       "('{}')".format(name, "', '".join(other_analyses)))
            else:
                msg = ''
            raise ArcanaNameError(
                name, ("{} doesn't have a field named '{}'{} "
                       + "(available '{}')").format(
                           self, name,
                           (" from analysis '{}'".format(namespace)
                            if namespace is not None else ''),
                           "', '".join(available), msg))

    def record(self, pipeline_name, namespace):
        """
        Returns the provenance record for a given pipeline

        Parameters
        ----------
        pipeline_name : str
            The name of the pipeline that generated the record
        namespace : str
            The name of the analysis that the pipeline was generated from

        Returns
        -------
        record : arcana2.provenance.Record
            The provenance record generated by the specified pipeline
        """
        try:
            return self._records[(pipeline_name, namespace)]
        except KeyError:
            found = []
            for sname, pnames in groupby(sorted(self._records,
                                                key=itemgetter(1)),
                                         key=itemgetter(1)):
                found.append(
                    "'{}' for '{}'".format("', '".join(p for p, _ in pnames),
                                           sname))
            raise ArcanaNameError(
                (pipeline_name, namespace),
                ("{} doesn't have a provenance record for pipeline '{}' "
                 "for '{}' analysis (found {})".format(
                     self, pipeline_name, namespace,
                     '; '.join(found))))

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


class DataTree(TreeNode):
    """
    Represents a project tree as stored in a dataset

    Parameters
    ----------
    subjects : List[Subject]
        List of subjects
    visits : List[Visits]
        List of visits in the project across subjects
        (i.e. timepoint 1, 2, 3)
    dataset : Dataset
        The dataset that the tree represents
    file_groups : List[FileGroup]
        The file_groups at the top level of the data tree
    fields : List[Field]
        The fields at the top level of the data tree
    fill_subjects : list[int] | None
        Create empty sessions for any subjects that are missing
        from the provided list. Typically only used if all
        the inputs to the analysis are coming from different datasets
        to the one that the derived products are stored in
    fill_visits : list[int] | None
        Create empty sessions for any visits that are missing
        from the provided list. Typically only used if all
        the inputs to the analysis are coming from different datasets
        to the one that the derived products are stored in
    """

    level = DataFreq.dataset

    def __init__(self, subjects, visits, dataset, file_groups=None,
                 fields=None, records=None, fill_subjects=None,
                 fill_visits=None, **kwargs):  # noqa: E501 @UnusedVariable
        TreeNode.__init__(self, file_groups, fields, records)
        self._subjects = OrderedDict(sorted(
            ((s.id, s) for s in subjects), key=itemgetter(0)))
        self._visits = OrderedDict(sorted(
            ((v.id, v) for v in visits), key=itemgetter(0)))
        if fill_subjects is not None or fill_visits is not None:
            self._fill_empty_sessions(fill_subjects, fill_visits)
        for subject in self.subjects:
            subject.tree = self
        for visit in self.visits:
            visit.tree = self
        for session in self.sessions:
            session.tree = self
        self._dataset = dataset
        # Collate missing and duplicates provenance records for single warnings
        missing_records = defaultdict(lambda: defaultdict(list))
        duplicate_records = defaultdict(lambda: defaultdict(list))
        for node in self.nodes():
            for missing in node._missing_records:
                missing_records[missing][node.visit_id].append(node.subject_id)
            for duplicate in node._duplicate_records:
                duplicate_records[duplicate][node.visit_id].append(
                    node.subject_id)
        for name, ids in missing_records.items():
            logger.warning(
                "No provenance records found for {} derivative in "
                "the following nodes: {}. Will assume they are a "
                "\"protected\" (manually created) derivatives"
                .format(name, '; '.join("visit='{}', subjects={}".format(k, v)
                                        for k, v in ids.items())))
        for name, ids in duplicate_records.items():
            logger.warning(
                "Duplicate provenance records found for {} in the following "
                "nodes: {}. Will select the latest record in each case"
                .format(name, '; '.join("visit='{}', subjects={}".format(k, v)
                                        for k, v in ids.items())))

    def __eq__(self, other):
        return (super(DataTree, self).__eq__(other)
                and self._subjects == other._subjects
                and self._visits == other._visits)

    def __hash__(self):
        return (TreeNode.__hash__(self)
                ^ hash(tuple(self.subjects))
                ^ hash(tuple(self._visits)))

    @property
    def dataset(self):
        return self._dataset

    @property
    def subjects(self):
        return self._subjects.values()

    @property
    def visits(self):
        return self._visits.values()

    @property
    def sessions(self):
        return chain(*(s.sessions for s in self.subjects))

    @property
    def tree(self):
        return self

    @property
    def subject_ids(self):
        return self._subjects.keys()

    @property
    def visit_ids(self):
        return self._visits.keys()

    @property
    def session_ids(self):
        return ((s.subject_id, s.visit_id) for s in self.sessions)

    @property
    def complete_subjects(self):
        max_num_sessions = max(len(s) for s in self.subjects)
        return (s for s in self.subjects if len(s) == max_num_sessions)

    @property
    def complete_visits(self):
        max_num_sessions = max(len(v) for v in self.visits)
        return (v for v in self.visits if len(v) == max_num_sessions)

    @property
    def incomplete_subjects(self):
        max_num_sessions = max(len(s) for s in self.subjects)
        return (s for s in self.subjects if len(s) != max_num_sessions)

    @property
    def incomplete_visits(self):
        max_num_sessions = max(len(v) for v in self.visits)
        return (v for v in self.visits if len(v) != max_num_sessions)

    def subject(self, id):
        try:
            return self._subjects[str(id)]
        except KeyError:
            raise ArcanaNameError(
                id, ("{} doesn't have a subject named '{}' ('{}')"
                     .format(self, id, "', '".join(self._subjects))))

    def visit(self, id):
        try:
            return self._visits[str(id)]
        except KeyError:
            raise ArcanaNameError(
                id, ("{} doesn't have a visit named '{}' ('{}')"
                     .format(self, id, "', '".join(self._visits))))

    def session(self, subject_id, visit_id):
        return self.subject(subject_id).session(visit_id)

    def __iter__(self):
        return self.nodes()

    def nodes(self, level=None):
        """
        Returns an iterator over all nodes in the tree for the specified
        level. If no level is specified then all nodes are returned

        Parameters
        ----------
        level : DataFreq | None
            The frequency that the items occur in the dataset, i.e. 
            per 'session', 'subject', 'visit', 'group_visit', 'group' or
            'dataset'

        Returns
        -------
        nodes : iterable[TreeNode]
        """
        if level is None:
            nodes = chain(*(self._nodes(f)
                            for f in ('per_dataset', 'per_subject',
                                      'per_visit', 'per_session')))
        else:
            nodes = self._nodes(level=level)
        return nodes

    def _nodes(self, frequency):
        if frequency == 'per_session':
            nodes = chain(*(s.sessions for s in self.subjects))
        elif frequency == 'per_subject':
            nodes = self.subjects
        elif frequency == 'per_visit':
            nodes = self.visits
        elif frequency == 'per_dataset':
            nodes = [self]
        else:
            assert False
        return nodes

    def find_mismatch(self, other, indent=''):
        """
        Used in debugging unittests
        """
        mismatch = super(DataTree, self).find_mismatch(other, indent)
        sub_indent = indent + '  '
        if len(list(self.subjects)) != len(list(other.subjects)):
            mismatch += ('\n{indent}mismatching subject lengths '
                         '(self={} vs other={}): '
                         '\n{indent}  self={}\n{indent}  other={}'
                         .format(len(list(self.subjects)),
                                 len(list(other.subjects)),
                                 list(self.subjects),
                                 list(other.subjects),
                                 indent=sub_indent))
        else:
            for s, o in zip(self.subjects, other.subjects):
                mismatch += s.find_mismatch(o, indent=sub_indent)
        if len(list(self.visits)) != len(list(other.visits)):
            mismatch += ('\n{indent}mismatching visit lengths '
                         '(self={} vs other={}): '
                         '\n{indent}  self={}\n{indent}  other={}'
                         .format(len(list(self.visits)),
                                 len(list(other.visits)),
                                 list(self.visits),
                                 list(other.visits),
                                 indent=sub_indent))
        else:
            for s, o in zip(self.visits, other.visits):
                mismatch += s.find_mismatch(o, indent=sub_indent)
        return mismatch

    def __repr__(self):
        return ("DataTree(num_subjects={}, num_visits={}, "
                "num_file_groups={}, num_fields={})".format(
                    len(list(self.subjects)),
                    len(list(self.visits)),
                    len(list(self.file_groups)), len(list(self.fields))))

    def _fill_empty_sessions(self, fill_subjects, fill_visits):
        """
        Fill in tree with additional empty subjects and/or visits to
        allow the analysis to pull its inputs from external datasets
        """
        if fill_subjects is None:
            fill_subjects = [s.id for s in self.subjects]
        if fill_visits is None:
            fill_visits = [v.id for v in self.complete_visits]
        for subject_id in fill_subjects:
            try:
                subject = self.subject(subject_id)
            except ArcanaNameError:
                subject = self._subjects[subject_id] = Subject(
                    subject_id, [], [], [])
            for visit_id in fill_visits:
                try:
                    subject.session(visit_id)
                except ArcanaNameError:
                    session = Session(subject_id, visit_id, [], [])
                    subject._sessions[visit_id] = session
                    try:
                        visit = self.visit(visit_id)
                    except ArcanaNameError:
                        visit = self._visits[visit_id] = Visit(
                            visit_id, [], [], [])
                    visit._sessions[subject_id] = session

    @classmethod
    def construct(cls, dataset, file_groups=(), fields=(), records=(),
                  file_formats=(), **kwargs):
        """
        Return the hierarchical tree of the file_groups and fields stored in a
        dataset

        Parameters
        ----------
        dataset : Dataset
            The dataset that the tree represents
        file_groups : list[FileGroup]
            List of all file_groups in the tree
        fields : list[Field]
            List of all fields in the tree
        records : list[Record]
            List of all records in the tree

        Returns
        -------
        tree : arcana2.repository.DataTree
            A hierarchical tree of subject, session and file_group
            information for the dataset
        """
        # Sort the data by subject and visit ID
        file_groups_dict = defaultdict(list)
        for fset in file_groups:
            if file_formats:
                fset.set_format(file_formats)
            file_groups_dict[(fset.subject_id, fset.visit_id)].append(fset)
        fields_dict = defaultdict(list)
        for field in fields:
            fields_dict[(field.subject_id, field.visit_id)].append(field)
        records_dict = defaultdict(list)
        for record in records:
            records_dict[(record.subject_id, record.visit_id)].append(record)
        # Create all sessions
        subj_sessions = defaultdict(list)
        visit_sessions = defaultdict(list)
        for sess_id in set(chain(file_groups_dict, fields_dict,
                                 records_dict)):
            if None in sess_id:
                continue  # Save summaries for later
            subj_id, visit_id = sess_id
            session = Session(
                subject_id=subj_id, visit_id=visit_id,
                file_groups=file_groups_dict[sess_id],
                fields=fields_dict[sess_id],
                records=records_dict[sess_id])
            subj_sessions[subj_id].append(session)
            visit_sessions[visit_id].append(session)
        subjects = []
        for subj_id in subj_sessions:
            subjects.append(Subject(
                subj_id,
                sorted(subj_sessions[subj_id]),
                file_groups_dict[(subj_id, None)],
                fields_dict[(subj_id, None)],
                records_dict[(subj_id, None)]))
        visits = []
        for visit_id in visit_sessions:
            visits.append(Visit(
                visit_id,
                sorted(visit_sessions[visit_id]),
                file_groups_dict[(None, visit_id)],
                fields_dict[(None, visit_id)],
                records_dict[(None, visit_id)]))
        return DataTree(sorted(subjects),
                    sorted(visits),
                    dataset,
                    file_groups_dict[(None, None)],
                    fields_dict[(None, None)],
                    records_dict[(None, None)],
                    **kwargs)


class Subject(TreeNode):
    """
    Represents a subject as stored in a dataset

    Parameters
    ----------
    subject_id : str
        The ID of the subject
    sessions : List[Session]
        The sessions in the subject
    file_groups : List[FileGroup]
        The file_groups that belong to the subject, i.e. of 'per_subject'
        frequency
    fields : List[Field]
        The fields that belong to the subject, i.e. of 'per_subject'
        frequency
    """

    frequency = 'per_subject'

    def __init__(self, subject_id, sessions, file_groups=None,
                 fields=None, records=None):
        TreeNode.__init__(self, file_groups, fields, records)
        self._id = subject_id
        self._sessions = OrderedDict(sorted(
            ((s.visit_id, s) for s in sessions), key=itemgetter(0)))
        for session in self.sessions:
            session.subject = self
        self._tree = None

    @property
    def id(self):
        return self._id

    @property
    def subject_id(self):
        return self.id

    def __lt__(self, other):
        return self._id < other._id

    def __eq__(self, other):
        return (TreeNode.__eq__(self, other)and
                self._id == other._id and
                self._sessions == other._sessions)

    def __hash__(self):
        return (TreeNode.__hash__(self) ^
                hash(self._id) ^
                hash(tuple(self.sessions)))

    def __len__(self):
        return len(self._sessions)

    def __iter__(self):
        return self.sessions

    @property
    def sessions(self):
        return self._sessions.values()

    def nodes(self, frequency=None):
        """
        Returns all sessions in the subject. If a frequency is passed then
        it will return all nodes of that frequency related to the current node.
        If there is no relationshop between the current node and the frequency
        then all nodes in the tree for that frequency will be returned

        Parameters
        ----------
        frequency : DataFreq | None
            The frequency that the items occur in the dataset, i.e. 
            per 'session', 'subject', 'visit', 'group_visit', 'group' or
            'dataset'

        Returns
        -------
        nodes : iterable[TreeNode]
            All nodes related to the subject for the specified frequency, or
            all nodes in the tree if there is no relation with that frequency
            (e.g. per_visit)
        """
        if frequency in (None, 'per_session'):
            return self.sessions
        elif frequency == 'per_visit':
            return self.tree.nodes(frequency)
        elif frequency == 'per_subject':
            return [self]
        elif frequency == 'per_dataset':
            return [self.tree]

    @property
    def visit_ids(self):
        return self._sessions.values()

    def session(self, visit_id):
        try:
            return self._sessions[str(visit_id)]
        except KeyError:
            raise ArcanaNameError(
                visit_id, ("{} doesn't have a session named '{}' ('{}')"
                           .format(self, visit_id,
                                   "', '".join(self._sessions))))

    def find_mismatch(self, other, indent=''):
        mismatch = TreeNode.find_mismatch(self, other, indent)
        sub_indent = indent + '  '
        if self.id != other.id:
            mismatch += ('\n{}id: self={} v other={}'
                         .format(sub_indent, self.id, other.id))
        if len(list(self.sessions)) != len(list(other.sessions)):
            mismatch += ('\n{indent}mismatching session lengths '
                         '(self={} vs other={}): '
                         '\n{indent}  self={}\n{indent}  other={}'
                         .format(len(list(self.sessions)),
                                 len(list(other.sessions)),
                                 list(self.sessions),
                                 list(other.sessions),
                                 indent=sub_indent))
        else:
            for s, o in zip(self.sessions, other.sessions):
                mismatch += s.find_mismatch(o, indent=sub_indent)
        return mismatch

    def __ne__(self, other):
        return not (self == other)

    def __repr__(self):
        return ("Subject(id={}, num_sessions={})"
                .format(self._id, len(self._sessions)))


class Visit(TreeNode):
    """
    Represents a slice of visits across subjects (e.g. time-point 1)
    as stored in a dataset

    Parameters
    ----------
    visit_id : str
        The ID of the visit
    sessions : List[Session]
        The sessions in the visit
    file_groups : List[FileGroup]
        The file_groups that belong to the visit, i.e. of 'per_visit'
        frequency
    fields : List[Field]
        The fields that belong to the visit, i.e. of 'per_visit'
        frequency
    """

    frequency = 'per_visit'

    def __init__(self, visit_id, sessions, file_groups=None, fields=None,
                 records=None):
        TreeNode.__init__(self, file_groups, fields, records)
        self._id = visit_id
        self._sessions = OrderedDict(sorted(
            ((s.subject_id, s) for s in sessions), key=itemgetter(0)))
        for session in sessions:
            session.visit = self

    @property
    def id(self):
        return self._id

    @property
    def visit_id(self):
        return self.id

    def __eq__(self, other):
        return (TreeNode.__eq__(self, other) and
                self._id == other._id and
                self._sessions == other._sessions)

    def __hash__(self):
        return (TreeNode.__hash__(self) ^
                hash(self._id) ^
                hash(tuple(self.sessions)))

    def __lt__(self, other):
        return self._id < other._id

    def __len__(self):
        return len(self._sessions)

    def __iter__(self):
        return self.sessions

    @property
    def sessions(self):
        return self._sessions.values()

    def nodes(self, frequency=None):
        """
        Returns all sessions in the visit. If a frequency is passed then
        it will return all nodes of that frequency related to the current node.
        If there is no relationshop between the current node and the frequency
        then all nodes in the tree for that frequency will be returned

        Parameters
        ----------
        frequency : DataFreq | None
            The frequency that the items occur in the dataset, i.e. 
            per 'session', 'subject', 'visit', 'group_visit', 'group' or
            'dataset'

        Returns
        -------
        nodes : iterable[TreeNode]
            All nodes related to the visit for the specified frequency, or
            all nodes in the tree if there is no relation with that frequency
            (e.g. per_subject)
        """
        if frequency in (None, 'per_session'):
            return self.sessions
        elif frequency == 'per_subject':
            return self.tree.nodes(frequency)
        elif frequency == 'per_visit':
            return [self]
        elif frequency == 'per_dataset':
            return [self.tree]

    def session(self, subject_id):
        try:
            return self._sessions[str(subject_id)]
        except KeyError:
            raise ArcanaNameError(
                subject_id, ("{} doesn't have a session named '{}' ('{}')"
                             .format(self, subject_id,
                                     "', '".join(self._sessions))))

    def find_mismatch(self, other, indent=''):
        mismatch = TreeNode.find_mismatch(self, other, indent)
        sub_indent = indent + '  '
        if self.id != other.id:
            mismatch += ('\n{}id: self={} v other={}'
                         .format(sub_indent, self.id, other.id))
        if len(list(self.sessions)) != len(list(other.sessions)):
            mismatch += ('\n{indent}mismatching session lengths '
                         '(self={} vs other={}): '
                         '\n{indent}  self={}\n{indent}  other={}'
                         .format(len(list(self.sessions)),
                                 len(list(other.sessions)),
                                 list(self.sessions),
                                 list(other.sessions),
                                 indent=sub_indent))
        else:
            for s, o in zip(self.sessions, other.sessions):
                mismatch += s.find_mismatch(o, indent=sub_indent)
        return mismatch

    def __ne__(self, other):
        return not (self == other)

    def __repr__(self):
        return "Visit(id={}, num_sessions={})".format(self._id,
                                                      len(self._sessions))


class Session(TreeNode):
    """
    Represents a session stored in a dataset

    Parameters
    ----------
    subject_id : str
        The subject ID of the session
    visit_id : str
        The visit ID of the session
    file_groups : list(FileGroup)
        The file_groups found in the session
    derived : dict[str, Session]
        Sessions storing derived scans are stored for separate analyses
    """

    frequency = 'per_session'

    def __init__(self, subject_id, visit_id, file_groups=None, fields=None,
                 records=None):
        TreeNode.__init__(self, file_groups, fields, records)
        self._subject_id = subject_id
        self._visit_id = visit_id
        self._subject = None
        self._visit = None

    @property
    def visit_id(self):
        return self._visit_id

    @property
    def subject_id(self):
        return self._subject_id

    def __eq__(self, other):
        return (TreeNode.__eq__(self, other) and
                self.subject_id == other.subject_id and
                self.visit_id == other.visit_id)

    def __hash__(self):
        return (TreeNode.__hash__(self) ^
                hash(self.subject_id) ^
                hash(self.visit_id))

    def __lt__(self, other):
        if self.subject_id < other.subject_id:
            return True
        else:
            return self.visit_id < other.visit_id

    @property
    def subject(self):
        return self._subject

    @subject.setter
    def subject(self, subject):
        self._subject = subject

    @property
    def visit(self):
        return self._visit

    @visit.setter
    def visit(self, visit):
        self._visit = visit

    def nodes(self, frequency=None):
        """
        Returns all nodes of the specified frequency that are related to
        the given Session

        Parameters
        ----------
        frequency : DataFreq | None
            The level of the nodes to return within the tree

        Returns
        -------
        nodes : iterable[TreeNode]
            All nodes related to the Session for the specified frequency
        """
        if frequency is None:
            []
        elif frequency == 'per_session':
            return [self]
        elif frequency in ('per_visit', 'per_subject'):
            return [self.tree.nodes(self.frequency)]
        elif frequency == 'per_dataset':
            return [self.tree]

    def find_mismatch(self, other, indent=''):
        mismatch = TreeNode.find_mismatch(self, other, indent)
        sub_indent = indent + '  '
        if self.subject_id != other.subject_id:
            mismatch += ('\n{}subject_id: self={} v other={}'
                         .format(sub_indent, self.subject_id,
                                 other.subject_id))
        if self.visit_id != other.visit_id:
            mismatch += ('\n{}visit_id: self={} v other={}'
                         .format(sub_indent, self.visit_id,
                                 other.visit_id))
        return mismatch

    def __ne__(self, other):
        return not (self == other)

    def __repr__(self):
        return ("Session(subject_id='{}', visit_id='{}', num_file_groups={}, "
                "num_fields={})".format(
                    self.subject_id, self.visit_id, len(self._file_groups),
                    len(self._fields)))
