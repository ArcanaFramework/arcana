from past.builtins import basestring
from builtins import object
from operator import attrgetter
from copy import copy, deepcopy
from arcana2.exceptions import (
    ArcanaError, ArcanaUsageError,
    ArcanaOutputNotProducedException,
    ArcanaMissingDataException, ArcanaNameError,
    ArcanaDesignError)
from .base import BaseFileGroup, BaseField, BaseData
from .item import FileGroup, Field
from .column import FileGroupColumn, FieldColumn


class BaseInputSpecMixin(object):

    category = 'input'
    derived = False
    # For duck-typing with *Matcher objects
    skip_missing = False
    drop_if_missing = False

    def __init__(self, name, desc=None, optional=False, default=None):
        if optional and default is not None:
            raise ArcanaUsageError(
                "'optional' doesn't make sense for specs ('{}') with default "
                "values"
                .format(name))
        self._desc = desc
        self._analysis = None
        self._optional = optional
        # Set the name of the default slice-like object so it matches
        # the name of the spec
        if default is not None:
            if default.frequency != self.frequency:
                raise ArcanaDesignError(
                    "Frequency of default slice-like object passed to "
                    "'{}' spec ('{}'), does not match spec ('{}')".format(
                        name, default.frequency, self.frequency))
            default = deepcopy(default)
        self._default = default

    def __eq__(self, other):
        return (self.desc == other.desc
                and self.optional == other.optional
                and self.default == other.default)

    def __hash__(self):
        return (hash(self.desc) ^ hash(self.optional) ^ hash(self.default))

    def initkwargs(self):
        dct = {}
        dct['desc'] = self.desc
        dct['optional'] = self.optional
        dct['default'] = deepcopy(self.default)
        return dct

    def find_mismatch(self, other, indent=''):
        mismatch = ''
        sub_indent = indent + '  '
        if self.optional != other.optional:
            mismatch += ('\n{}optional: self={} v other={}'
                         .format(sub_indent, self.optional,
                                 other.optional))
        if self.default != other.default:
            mismatch += ('\n{}default: self={} v other={}'
                         .format(sub_indent, self.default,
                                 other.default))
        if self.desc != other.desc:
            mismatch += ('\n{}desc: self={} v other={}'
                         .format(sub_indent, self.desc, other.desc))
        return mismatch

    def bind(self, analysis, **kwargs):
        """
        Returns a copy of the InputSpec bound to the given analysis

        Parameters
        ----------
        analysis : Analysis
            A analysis to bind the file_group spec to (should happen in the
            analysis __init__)
        """
        if self.default is None:
            raise ArcanaError(
                ("Attempted to bind '{}' to {} but only acquired specs with "
                 + "a default value should be bound to studies").format(
                    self.name, analysis))
        if self._analysis is not None:
            # This avoids rebinding specs to sub-studies that have already
            # been bound to the multi-analysis
            bound = self
        else:
            bound = copy(self)
            bound._analysis = analysis
            bound._default = bound.default.bind(analysis)
        return bound

    @property
    def optional(self):
        return self._optional

    @property
    def analysis(self):
        if self._analysis is None:
            raise ArcanaError(
                "{} is not bound to a analysis".format(self))
        return self._analysis

    @property
    def default(self):
        # Ensure the default has the same name as the spec to it can be
        # accessed properly by the source node
        if self._default is not None:
            # FIXME: Should use setter but throwing an error for some reason
            self._default.name = self.name
        return self._default

    @property
    def desc(self):
        return self._desc

    @property
    def slice(self):
        if self._analysis is None:
            raise ArcanaUsageError(
                "{} needs to be bound to a analysis before accessing "
                "the corresponding slice".format(self))
        if self.default is None:
            raise ArcanaUsageError(
                "{} does not have default so cannot access its slice"
                .format(self))
        return self.default.column


class BaseSpecMixin(object):

    category = 'intermediate'
    derived = True
    # For duck-typing with *Input objects
    skip_missing = False
    drop_if_missing = False

    def __init__(self, name, pipeline_getter, desc=None,
                 pipeline_args=None, group=None):
        if pipeline_getter is not None:
            if not isinstance(pipeline_getter, basestring):
                raise ArcanaUsageError(
                    ("Pipeline name for {} '{}' is not a string "
                     + "'{}'").format(name, pipeline_getter))
        self._pipeline_getter = pipeline_getter
        self._desc = desc
        self._analysis = None
        self._column = None
        if isinstance(pipeline_args, dict):
            pipeline_args = tuple(sorted(pipeline_args.items()))
        elif pipeline_args is not None:
            pipeline_args = tuple(pipeline_args)
        else:
            pipeline_args = ()
        self._pipeline_args = pipeline_args
        self._group = group

    def __eq__(self, other):
        return (self.pipeline_getter == other.pipeline_getter
                and self.desc == other.desc
                and self.pipeline_args == other.pipeline_args
                and self.group == other.group)

    def __hash__(self):
        return (hash(self.pipeline_getter) ^ hash(self.desc)
                ^ hash(self.pipeline_args.items()) ^ hash(self.group))

    def initkwargs(self):
        dct = {}
        dct['pipeline_getter'] = self.pipeline_getter
        dct['desc'] = self.desc
        dct['pipeline_args'] = copy(self.pipeline_args)
        dct['group'] = self.group
        return dct

    def find_mismatch(self, other, indent=''):
        mismatch = ''
        sub_indent = indent + '  '
        if self.pipeline_getter != other.pipeline_getter:
            mismatch += ('\n{}pipeline_getter: self={} v other={}'
                         .format(sub_indent, self.pipeline_getter,
                                 other.pipeline_getter))
        if self.desc != other.desc:
            mismatch += ('\n{}desc: self={} v other={}'
                         .format(sub_indent, self.desc, other.desc))
        if self.group != other.group:
            mismatch += ('\n{}group: self={} v other={}'
                         .format(sub_indent, self.group, other.group))
        if self.pipeline_args != other.pipeline_args:
            mismatch += ('\n{}pipeline_args: self={} v other={}'
                         .format(sub_indent, self.pipeline_args,
                                 other.pipeline_args))
        return mismatch

    def bind(self, analysis, **kwargs):
        """
        Returns a copy of the Spec bound to the given analysis

        Parameters
        ----------
        analysis : Analysis
            A analysis to bind the file_group spec to (should happen in the
            analysis __init__)
        """
        if self._analysis is not None:
            # Avoid rebinding specs in sub-studies that have already
            # been bound to MultiAnalysis
            bound = self
        else:
            bound = copy(self)
            bound._analysis = analysis
            if not hasattr(analysis, self.pipeline_getter):
                raise ArcanaError(
                    "{} does not have a method named '{}' required to "
                    "derive {}".format(analysis, self.pipeline_getter,
                                       self))
            bound._bind_tree(analysis.dataset.tree)
        return bound

    @property
    def slice(self):
        if self._column is None:
            raise ArcanaUsageError(
                "{} needs to be bound to a analysis before accessing "
                "the corresponding slice".format(self))
        return self._column

    def nodes(self, tree):
        """
        Returns the relevant nodes for the spec's frequency
        """
        # Run the match against the tree
        if self.frequency == 'per_session':
            nodes = []
            for subject in tree.subjects:
                for sess in subject.sessions:
                    nodes.append(sess)
        elif self.frequency == 'per_subject':
            nodes = tree.subjects
        elif self.frequency == 'per_visit':
            nodes = tree.visits
        elif self.frequency == 'per_dataset':
            nodes = [tree]
        else:
            assert False, "Unrecognised frequency '{}'".format(
                self.frequency)
        return nodes

    @property
    def derivable(self):
        """
        Whether the spec (only valid for derived specs) can be derived
        given the inputs and switches provided to the analysis
        """
        try:
            # Just need to iterate all analysis inputs and catch relevant
            # exceptions
            list(self.pipeline.analysis_inputs)
        except (ArcanaOutputNotProducedException,
                ArcanaMissingDataException):
            return False
        return True

    @property
    def pipeline_getter(self):
        return self._pipeline_getter

    @property
    def group(self):
        return self._group

    @property
    def pipeline_args(self):
        return self._pipeline_args

    @property
    def pipeline_arg_names(self):
        return tuple(n for n, _ in self._pipeline_args)

    @property
    def pipeline(self):
        return self.analysis.pipeline(self.pipeline_getter, [self.name],
                                      pipeline_args=self.pipeline_args)

    @property
    def analysis(self):
        if self._analysis is None:
            raise ArcanaError(
                "{} is not bound to a analysis".format(self))
        return self._analysis

    @property
    def desc(self):
        return self._desc

    def _tree_node(self, subject_id=None, visit_id=None):
        if self.frequency == 'per_session':
            node = self.analysis.tree.subject(subject_id).session(visit_id)
        elif self.frequency == 'per_subject':
            node = self.analysis.tree.subject(subject_id)
        elif self.frequency == 'per_visit':
            node = self.analysis.tree.visit(visit_id)
        elif self.frequency == 'per_dataset':
            node = self.analysis.tree
        else:
            assert False
        return node


class InputFileGroupSpec(BaseFileGroup, BaseInputSpecMixin):
    """
    A specification for an "acquired" file_group (e.g from the scanner or
    standard atlas)

    Parameters
    ----------
    name : str
        The name of the file_group
    valid_formats : FileFormat | list[FileFormat]
        The acceptable file formats for input file_groups to match this spec
    frequency : str
        One of 'per_session', 'per_subject', 'per_visit' or 'per_dataset',
        specifying whether the file_group is present for each session, subject,
        visit or dataset.
    desc : str
        Description of what the field represents
    optional : bool
        Whether the specification is optional or not. Only valid for
        "acquired" file_group specs.
    default : FileGroupColumn | object
        The default value to be passed as an input to this spec if none are
        provided. Can either be an explicit FileGroupColumn or any object
        with a 'slice' property that will return a default slice.
        This object should also implement a 'bind(self, analysis)' method to
        allow the analysis to be bound to it.
    """

    is_spec = True
    ColumnClass = FileGroupColumn

    def __init__(self, name, valid_formats, frequency='per_session',
                 desc=None, optional=False, default=None):
        # Ensure allowed formats is a list
        try:
            valid_formats = tuple(valid_formats)
        except TypeError:
            valid_formats = (valid_formats,)
        else:
            if not valid_formats:
                raise ArcanaError(
                    "'{}' spec doesn't have any allowed formats".format(name))
        self._valid_formats = valid_formats
        BaseFileGroup.__init__(self, name, None, frequency)
        BaseInputSpecMixin.__init__(self, name, desc, optional=optional,
                                    default=default)

    @property
    def valid_formats(self):
        return self._valid_formats

    @property
    def format(self):
        try:
            return self.default.format
        except AttributeError:
            raise ArcanaUsageError(
                "File format is not defined for InputFileGroupSpec objects "
                "without a default")

    def __eq__(self, other):
        return (BaseFileGroup.__eq__(self, other)
                and BaseInputSpecMixin.__eq__(self, other)
                and self._valid_formats == other._valid_formats)

    def __hash__(self):
        return (BaseFileGroup.__hash__(self)
                ^ BaseInputSpecMixin.__hash__(self)
                and hash(self.valid_formats))

    def initkwargs(self):
        dct = BaseData.initkwargs(self)
        dct.update(BaseInputSpecMixin.initkwargs(self))
        dct['valid_formats'] = self._valid_formats
        return dct

    def __repr__(self):
        return ("{}(name='{}', valid_formats={}, frequency={}, default={}, "
                "optional={})"
                .format(type(self).__name__, self.name,
                        list(f.name for f in self.valid_formats),
                        self.frequency, self.default, self.optional))

    def find_mismatch(self, other, indent=''):
        sub_indent = indent + '  '
        mismatch = BaseFileGroup.find_mismatch(self, other, indent)
        mismatch += BaseInputSpecMixin.find_mismatch(self, other, indent)
        if self.valid_formats != other.valid_formats:
            mismatch += ('\n{}pipeline: self={} v other={}'
                         .format(sub_indent, list(self.valid_formats),
                                 list(other.valid_formats)))
        return mismatch


class FileGroupSpec(BaseFileGroup, BaseSpecMixin):
    """
    A specification for a file group within a analysis to be derived from a
    processing pipeline.

    Parameters
    ----------
    name : str
        The name of the file group
    format : FileFormat
        The file format used to store the file_group. Can be one of the
        recognised formats
    pipeline_getter : str
        Name of the method in the analysis that constructs a pipeline to derive
        the file_group
    frequency : str
        One of 'per_session', 'per_subject', 'per_visit' or 'per_dataset',
        specifying whether the file_group is present for each session, subject,
        visit or dataset.
    desc : str
        Description of what the field represents
    valid_formats : list[FileFormat]
        A list of valid file formats that can be supplied to the spec if
        overridden as an input. Typically not required, but useful for some
        specs that are typically provided as inputs (e.g. magnitude MRI)
        but can be derived from other inputs (e.g. coil-wise MRI images)
    pipeline_args : dct[str, *] | None
        Arguments to pass to the pipeline constructor method. Avoids having to
        create separate methods for each spec, where the only difference
        between the specs are interface parameterisations
    group : str | None
        A name for a group of file_group specs. Used improve human searching of
        available options
    """

    is_spec = True
    ColumnClass = FileGroupColumn

    def __init__(self, name, format, pipeline_getter, frequency='per_session',
                 desc=None, valid_formats=None, pipeline_args=None,
                 group=None):
        BaseFileGroup.__init__(self, name, format, frequency)
        BaseSpecMixin.__init__(self, name, pipeline_getter, desc,
                               pipeline_args, group)
        if valid_formats is not None:
            # Ensure allowed formats is a list
            try:
                valid_formats = sorted(valid_formats, key=attrgetter('name'))
            except TypeError:
                valid_formats = [valid_formats]
        self._valid_formats = valid_formats

    def __eq__(self, other):
        return (BaseFileGroup.__eq__(self, other)
                and BaseSpecMixin.__eq__(self, other)
                and self.valid_formats == other.valid_formats)

    def __hash__(self):
        return (BaseFileGroup.__hash__(self)
                ^ BaseSpecMixin.__hash__(self)
                ^ hash(self.valid_formats))

    def initkwargs(self):
        dct = BaseFileGroup.initkwargs(self)
        dct.update(BaseSpecMixin.initkwargs(self))
        dct['valid_formats'] = self._valid_formats
        return dct

    def __repr__(self):
        return ("FileGroupSpec(name='{}', format={}, pipeline_getter={}, "
                "frequency={})".format(
                    self.name, self.format, self.pipeline_getter,
                    self.frequency))

    def find_mismatch(self, other, indent=''):
        sub_indent = indent + '  '
        mismatch = BaseFileGroup.find_mismatch(self, other, indent)
        mismatch += BaseSpecMixin.find_mismatch(self, other, indent)
        if self.valid_formats != other.valid_formats:
            mismatch += ('\n{}pipeline: self={} v other={}'
                         .format(sub_indent, list(self.valid_formats),
                                 list(other.valid_formats)))
        return mismatch

    def _bind_node(self, node, **kwargs):
        try:
            file_group = node.file_group(self.name, from_analysis=self.analysis.name,
                                   format=self.format)
        except ArcanaNameError:
            # For file_groups that can be generated by the analysis
            file_group = FileGroup(self.name, format=self.format,
                              frequency=self.frequency, path=None,
                              subject_id=node.subject_id,
                              visit_id=node.visit_id,
                              dataset=self.analysis.dataset,
                              from_analysis=self.analysis.name,
                              exists=False,
                              **kwargs)
        return file_group

    def _bind_tree(self, tree, **kwargs):
        self._column = FileGroupColumn(
            self.name,
            (self._bind_node(n, **kwargs) for n in self.nodes(tree)),
            frequency=self.frequency,
            format=self.format)

    @property
    def valid_formats(self):
        if self._valid_formats is not None:
            valid_formats = self._valid_formats
        else:
            valid_formats = [self.format] + list(self.format.convertable_from)
        return valid_formats


class InputFieldSpec(BaseField, BaseInputSpecMixin):
    """
    A specification of a field to be supplied to the analysis

    Parameters
    ----------
    name : str
        The name of the file_group
    dtype : type
        The datatype of the value. Can be one of (float, int, str)
    frequency : str
        One of 'per_session', 'per_subject', 'per_visit' or 'per_dataset',
        specifying whether the file_group is present for each session, subject,
        visit or dataset.
    desc : str
        Description of what the field represents
    optional : bool
        Whether the specification is optional or not. Only valid for
        "acquired" file_group specs.
    default : FieldColumn | callable
        The default value to be passed as an input to this spec if none are
        provided. Can either be an explicit FieldColumn or any object
        with a 'slice' property that will return a default slice.
        This object should also implement a 'bind(self, analysis)' method to
        allow the analysis to be bound to it.
    """

    is_spec = True
    ColumnClass = FieldColumn

    def __init__(self, name, dtype, frequency='per_session', desc=None,
                 optional=False, default=None, array=False):
        BaseField.__init__(self, name, dtype, frequency, array=array)
        BaseInputSpecMixin.__init__(self, name, desc, optional=optional,
                                    default=default)

    def __eq__(self, other):
        return (BaseField.__eq__(self, other)
                and BaseInputSpecMixin.__eq__(self, other))

    def __hash__(self):
        return (BaseField.__hash__(self) ^ BaseInputSpecMixin.__hash__(self))

    def find_mismatch(self, other, indent=''):
        mismatch = BaseField.find_mismatch(self, other, indent)
        mismatch += BaseInputSpecMixin.find_mismatch(self, other, indent)
        return mismatch

    def __repr__(self):
        return ("{}(name='{}', dtype={}, frequency={}, array={})".format(
            self.__class__.__name__, self.name, self.dtype,
            self.frequency, self.array))

    def initkwargs(self):
        dct = BaseField.initkwargs(self)
        dct.update(BaseInputSpecMixin.initkwargs(self))
        return dct


class FieldSpec(BaseField, BaseSpecMixin):
    """
    An abstract base class representing the specification for a derived
    file_group.

    Parameters
    ----------
    name : str
        The name of the file_group
    dtype : type
        The datatype of the value. Can be one of (float, int, str)
    pipeline_getter : str
        Name of the method that constructs pipelines to derive the field
    frequency : str
        One of 'per_session', 'per_subject', 'per_visit' or 'per_dataset',
        specifying whether the file_group is present for each session, subject,
        visit or dataset.
    desc : str
        Description of what the field represents
    pipeline_args : dct[str, *] | None
        Arguments to pass to the pipeline constructor method. Avoids having to
        create separate methods for each spec, where the only difference
        between the specs are interface parameterisations
    group : str
        A name for a group of file_group specs. Used improve human searching of
        available options
    """

    is_spec = True
    ColumnClass = FieldColumn

    def __init__(self, name, dtype, pipeline_getter,
                 frequency='per_session', desc=None, array=False,
                 pipeline_args=None, group=None):
        BaseField.__init__(self, name, dtype, frequency, array=array)
        BaseSpecMixin.__init__(self, name, pipeline_getter, desc,
                               pipeline_args=pipeline_args, group=group)

    def __eq__(self, other):
        return (BaseField.__eq__(self, other)
                and BaseSpecMixin.__eq__(self, other))

    def __hash__(self):
        return (BaseField.__hash__(self) ^ BaseSpecMixin.__hash__(self))

    def find_mismatch(self, other, indent=''):
        mismatch = BaseField.find_mismatch(self, other, indent)
        mismatch += BaseSpecMixin.find_mismatch(self, other, indent)
        return mismatch

    def __repr__(self):
        return ("{}(name='{}', dtype={}, pipeline_getter={}, "
                "frequency={}, array={})".format(
                    self.__class__.__name__, self.name, self.dtype,
                    self.pipeline_getter, self.frequency, self.array))

    def initkwargs(self):
        dct = BaseField.initkwargs(self)
        dct.update(BaseSpecMixin.initkwargs(self))
        return dct

    def _bind_node(self, node, **kwargs):
        try:
            field = node.field(self.name, from_analysis=self.analysis.name)
        except ArcanaNameError:
            # For fields to be generated by the analysis
            field = Field(self.name, dtype=self.dtype,
                          frequency=self.frequency,
                          subject_id=node.subject_id,
                          visit_id=node.visit_id,
                          dataset=self.analysis.dataset,
                          from_analysis=self.analysis.name,
                          array=self.array,
                          exists=False, **kwargs)
        return field

    def _bind_tree(self, tree, **kwargs):
        self._column = FieldColumn(
            self.name,
            (self._bind_node(n, **kwargs) for n in self.nodes(tree)),
            frequency=self.frequency,
            dtype=self.dtype,
            array=self.array)


class OutputFileGroupSpec(FileGroupSpec):
    """
    A specification for a file_group within a analysis to be derived from a
    processing pipeline that is typically a publishable output (almost
    identical to FileGroupSpec)

    Parameters
    ----------
    name : str
        The name of the file_group
    format : FileFormat
        The file format used to store the file_group. Can be one of the
        recognised formats
    pipeline_getter : str
        Name of the method in the analysis that constructs a pipeline to derive
        the file_group
    frequency : str
        One of 'per_session', 'per_subject', 'per_visit' or 'per_dataset',
        specifying whether the file_group is present for each session, subject,
        visit or dataset.
    desc : str
        Description of what the field represents
    valid_formats : list[FileFormat]
        A list of valid file formats that can be supplied to the spec if
        overridden as an input. Typically not required, but useful for some
        specs that are typically provided as inputs (e.g. magnitude MRI)
        but can be derived from other inputs (e.g. coil-wise MRI images)
    pipeline_args : dct[str, *] | None
        Arguments to pass to the pipeline constructor method. Avoids having to
        create separate methods for each spec, where the only difference
        between the specs are interface parameterisations
    group : str | None
        A name for a group of file_group specs. Used improve human searching of
        available options
    """

    category = 'output'


class OutputFieldSpec(FieldSpec):
    """
    A specification for a field within a analysis to be derived from a
    processing pipeline that is typically a publishable output (almost
    identical to FieldSpec).

    Parameters
    ----------
    name : str
        The name of the file_group
    dtype : type
        The datatype of the value. Can be one of (float, int, str)
    pipeline_getter : str
        Name of the method that constructs pipelines to derive the field
    frequency : str
        One of 'per_session', 'per_subject', 'per_visit' or 'per_dataset',
        specifying whether the file_group is present for each session, subject,
        visit or dataset.
    desc : str
        Description of what the field represents
    pipeline_args : dct[str, *] | None
        Arguments to pass to the pipeline constructor method. Avoids having to
        create separate methods for each spec, where the only difference
        between the specs are interface parameterisations
    group : str
        A name for a group of file_group specs. Used improve human searching of
        available options
    """

    category = 'output'
