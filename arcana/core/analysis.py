import attrs
import typing as ty
import inspect
from collections import defaultdict
import operator as operator_module
from operator import attrgetter
from .data.space import DataSpace
from .enum import CheckSalience, ColumnSalience, ParameterSalience
import arcana.core.mark
from arcana.exceptions import ArcanaDesignError

# from arcana.exceptions import ArcanaFormatConversionError


@attrs.define(frozen=True)
class Operation:
    """Defines logical expressions used in specifying conditions when different versions
    of pipelines will run"""

    operator: str
    operands: ty.Tuple[str]

    def evaluate(self, analysis, dataset):
        operands = [o.evaluate(analysis, dataset) for o in self.operands]
        if self.operator == "value_of":
            assert len(operands) == 1
            val = getattr(analysis, operands[0])
        elif self.operator == "is_provided":
            assert len(operands) <= 2
            column_name = getattr(analysis, operands[0])
            if column_name is None:
                val = False
            else:
                column = dataset[column_name]
                if len(operands) == 2:
                    in_format = operands[1]
                    val = column.format is in_format or issubclass(
                        column.format, in_format
                    )
                else:
                    val = True
        else:
            val = getattr(operator_module, self.operator)(*operands)
        return val


@attrs.define(frozen=True)
class ColumnSpec:
    """Specifies a column that the analysis can add when it is applied to a dataset"""

    name: str
    type: type
    desc: str
    row_frequency: DataSpace
    salience: ColumnSalience
    defined_in: type
    modified: ty.Tuple[ty.Tuple[str, ty.Any]]


@attrs.define(frozen=True)
class Parameter:
    """Specifies a free parameter of an analysis"""

    name: str
    type: type
    desc: str
    salience: ParameterSalience
    choices: ty.Tuple[int] or ty.Tuple[float] or ty.Tuple[str]
    defined_in: type
    modified: ty.Tuple[ty.Tuple[str, ty.Any]]
    default: int or float or str or ty.Tuple[int] or ty.Tuple[float] or ty.Tuple[
        str
    ] = attrs.field()

    @default.validator
    def default_validator(self, _, default):
        if default is None and self.salience != ParameterSalience.required:
            raise ValueError(
                f"Default value for '{self.name}' parameter must be provided unless "
                f"parameter salience is set to'{ParameterSalience.required}'"
            )


@attrs.define(frozen=True)
class Switch:
    """Specifies a "switch" point at which the processing can bifurcate to handle two
    separate types of input streams"""

    name: str
    desc: str
    inputs: ty.Tuple[str]
    parameters: ty.Tuple[str]
    method: ty.Callable
    defined_in: type


@attrs.define(frozen=True)
class PipelineSpec:
    """Specifies a pipeline that is able to generate data for sink columns under
    certain conditions"""

    name: str
    desc: str
    parameters: ty.Tuple[str]
    inputs: ty.Tuple[str]
    outputs: ty.Tuple[str]
    condition: Operation or None
    switch: Switch or None
    method: ty.Callable
    defined_in: type


@attrs.define(frozen=True)
class Check:
    """Specifies a quality-control check that can be run on generated derivatives to
    assess the probability that they have failed"""

    name: str
    column: ColumnSpec
    desc: str
    inputs: ty.Tuple[str]
    parameters: ty.Tuple[str]
    salience: CheckSalience
    method: ty.Callable
    defined_in: type


@attrs.define(frozen=True)
class Subanalysis:
    """Specifies a "sub-analysis" component, when composing an analysis of several
    predefined analyses"""

    name: str
    analysis: type
    columns: ty.Tuple[str or ty.Tuple[str, str]]
    parameters: ty.Tuple[str or ty.Tuple[str, str]]
    defined_in: type


def unique_names(inst, attr, val):
    names = [v.name for v in val]
    if duplicates := [v for v in val if names.count(v.name) > 1]:
        raise ValueError(f"Duplicate names found in provided tuple: {duplicates}")


@attrs.define(frozen=True)
class AnalysisSpec:
    """Specifies all the components of the analysis class"""

    space: type
    column_specs: ty.Tuple[ColumnSpec] = attrs.field(validator=unique_names)
    pipeline_specs: ty.Tuple[PipelineSpec] = attrs.field(validator=unique_names)
    parameters: ty.Tuple[Parameter] = attrs.field(validator=unique_names)
    switches: ty.Tuple[Switch] = attrs.field(validator=unique_names)
    checks: ty.Tuple[Check] = attrs.field(validator=unique_names)
    subanalyses: ty.Tuple[Subanalysis] = attrs.field(validator=unique_names)

    @property
    def column_names(self):
        return (c.name for c in self.column_specs)

    @property
    def parameter_names(self):
        return (p.name for p in self.parameters)

    @property
    def pipeline_names(self):
        return (p.name for p in self.pipeline_specs)

    @property
    def switch_names(self):
        return (s.name for s in self.switches)

    @property
    def check_names(self):
        return (c.name for c in self.checks)

    @property
    def subanalysis_names(self):
        return (s.name for s in self.subanalyses)

    def column_spec(self, name):
        return next(c for c in self.column_specs if c.name == name)

    def parameter(self, name):
        return next(p for p in self.parameters if p.name == name)

    def pipeline_spec(self, name):
        return next(p for p in self.pipeline_specs if p.name == name)

    def switch(self, name):
        return next(s for s in self.switches if s.name == name)

    def check(self, name):
        return next(c for c in self.checks if c.name == name)

    def subanalysis(self, name):
        return next(s for s in self.subanalyses if s.name == name)

    def column_checks(self, column_name):
        "Return all checks for a given column"
        return (c for c in self.checks if c.column == column_name)

    def select_pipeline(self, column_name, analysis, dataset):
        candidates = [p for p in self.pipeline_specs if column_name in p.outputs]
        matching = [
            m
            for m in candidates
            if (
                m.condition is not None
                and m.condition.evaluate(self, analysis, dataset)
            )
        ]
        if len(matching) == 1:
            selected = matching[0]
        elif not matching:
            # Use default
            selected = [p for p in candidates if p.condition is None]
        else:
            raise ArcanaDesignError(
                "Multiple potential pipelines match criteria for the given analysis "
                f"configuration and provided dataset: {matching}"
            )
        return selected

    @column_specs.validator
    def column_specs_validator(self, _, column_specs):
        for column_spec in column_specs:
            sorted_by_cond = defaultdict(list)
            for pipe_spec in self.pipeline_specs:
                if pipe_spec.output.name == column_spec.name:
                    sorted_by_cond[(pipe_spec.condition, pipe_spec.switch)] = pipe_spec
            if (None, None) not in sorted_by_cond:
                raise ArcanaDesignError(
                    "Default pipeline not specified (i.e. with no condition or switch) "
                    "for '{column_spec.name}'"
                )
            if duplicated := [d for d in sorted_by_cond.values() if len(d) > 1]:
                raise ArcanaDesignError(
                    f"Multiple pipelines provide outputs for {column_spec.name} under "
                    "matching conditions - \n"
                    + "\n".join(
                        f"Condition: {dups.condition}, Switch: {dups.switch} - "
                        + ", ".join(str(p) for p in dups)
                        for dups in duplicated
                    )
                )
            if not sorted_by_cond:
                inputs_to = [
                    p for p in self.pipeline_specs if column_spec.name in p.inputs
                ]
                if not inputs_to:
                    raise ArcanaDesignError(
                        f"{column_spec} is neither an input nor output to any pipeline"
                    )
                if column_spec.salience.level <= ColumnSalience.publication:
                    raise ArcanaDesignError(
                        f"{column_spec} is not generated by any pipeline yet its salience "
                        f"is not specified as 'raw' or 'primary'"
                    )

    @pipeline_specs.validator
    def pipeline_specs_validator(self, _, pipeline_specs):
        for pipeline_spec in pipeline_specs:
            if missing_outputs := [
                o for o in pipeline_spec.outputs if o not in self.column_names
            ]:
                raise ArcanaDesignError(
                    f"{pipeline_spec} outputs to unknown columns: {missing_outputs}"
                )


def make_analysis_class(cls, space: DataSpace):
    """
    Construct an analysis class and validate all the components fit together
    """

    for name, inherited in list(cls.__dict__.items()):
        if isinstance(inherited, _Inherited):
            setattr(cls, name, inherited.resolve(name, cls))
            inherited.resolved_to = name

    # Ensure slot gets created for the __analysis_spec__ attr in the class generated by
    # attrs.define, if it doesn't already exist (which will be the case when subclassing
    # another analysis class)
    if not hasattr(cls, "__analysis_spec__"):
        cls.__analysis_spec__ = None

    # Create class using attrs package, will create attributes for all columns and
    # parameters
    analysis_cls = attrs.define(cls)

    # Initialise lists to hold all the different components of an analysis
    column_specs = []
    pipeline_specs = []
    parameters = []
    switches = []
    checks = []
    subanalyses = []

    # Loop through all attributes created by attrs.define and create specs for columns,
    # parameters and sub-analyses to be stored in the __analysis_spec__ attribute
    for attr in analysis_cls.__attrs_attrs__:
        try:
            attr_type = attr.metadata[arcana.core.mark.ATTR_TYPE]
        except KeyError:
            continue
        if attr.inherited:
            continue  # Inherited attributes will be combined at the end
        if attr_type == "column":
            row_freq = attr.metadata["row_frequency"]
            if row_freq is None:
                row_freq = max(space)  # "Leaf" frequency of the data tree
            column_specs.append(
                ColumnSpec(
                    name=attr.name,
                    type=attr.type,
                    desc=attr.metadata["desc"],
                    row_frequency=row_freq,
                    salience=attr.metadata["salience"],
                    defined_in=attr.metadata.get("defined_in", analysis_cls),
                    modified=attr.metadata.get("modified"),
                )
            )
        elif attr_type == "parameter":
            parameters.append(
                Parameter(
                    name=attr.name,
                    type=attr.type,
                    default=attr.default,
                    choices=attr.metadata["choices"],
                    desc=attr.metadata["desc"],
                    salience=attr.metadata["salience"],
                    defined_in=attr.metadata.get("defined_in", analysis_cls),
                    modified=attr.metadata.get("modified"),
                )
            )
        elif attr_type == "subanalysis":
            raise NotImplementedError
        else:
            raise ValueError(f"Unrecognised attrs type '{attr_type}'")

    # Loop through all attributes to pick up decorated methods for pipelines, checks
    # and switches
    for attr in analysis_cls.__dict__.values():
        try:
            attr_anots = attr.__annotations__
        except AttributeError:
            continue

        if arcana.core.mark.PIPELINE_ANNOT in attr_anots:
            anots = attr_anots[arcana.core.mark.PIPELINE_ANNOT]
            outputs = tuple(_attr_name(cls, o) for o in anots["outputs"])
            input_columns, used_parameters = get_args_automagically(
                column_specs=column_specs, parameters=parameters, method=attr
            )
            unresolved_condition = anots["condition"]
            if unresolved_condition is not None:
                try:
                    condition = _attr_name(cls, unresolved_condition)
                except AttributeError:
                    condition = unresolved_condition.resolve(
                        cls, column_specs=column_specs, parameters=parameters
                    )
            else:
                condition = None
            pipeline_specs.append(
                PipelineSpec(
                    name=attr.__name__,
                    desc=attr.__doc__,
                    inputs=input_columns,
                    outputs=outputs,
                    parameters=used_parameters,
                    condition=condition,
                    switch=anots["switch"],
                    method=attr,
                    defined_in=analysis_cls,
                )
            )
        elif arcana.core.mark.SWICTH_ANNOT in attr_anots:
            input_columns, used_parameters = get_args_automagically(
                column_specs=column_specs, parameters=parameters, method=attr
            )
            switches.append(
                Switch(
                    name=attr.__name__,
                    desc=__doc__,
                    inputs=input_columns,
                    parameters=used_parameters,
                    method=attr,
                    defined_in=analysis_cls,
                )
            )
        elif arcana.core.mark.CHECK_ANNOT in attr_anots:
            anots = attr_anots[arcana.core.mark.CHECK_ANNOT]
            column_name = _attr_name(cls, anots["column"])
            input_columns, used_parameters = get_args_automagically(
                column_specs=column_specs, parameters=parameters, method=attr
            )
            checks.append(
                Check(
                    name=attr.__name__,
                    column=column_name,
                    desc=attr.__doc__,
                    inputs=input_columns,
                    parameters=used_parameters,
                    salience=anots["salience"],
                    method=attr,
                    defined_in=analysis_cls,
                )
            )

    # Combine with specs from base classes
    for base in analysis_cls.__mro__[1:]:
        try:
            base_spec = base.__analysis_spec__
        except AttributeError:
            continue  # skip classes that aren't decorated analyses
        if base_spec.space is not space:
            raise ValueError(
                "Cannot redefine the space that an analysis operates on from "
                f"{base_spec.space} to {space}"
            )
        # Prepend column and parameter specs that were inherited from base
        column_specs.extend(
            b
            for b in base_spec.column_specs
            if b.name not in (x.name for x in column_specs)
        )
        pipeline_specs.extend(
            b
            for b in base_spec.pipeline_specs
            if b.name not in (x.name for x in pipeline_specs)
        )
        parameters.extend(
            b
            for b in base_spec.parameters
            if b.name not in (x.name for x in parameters)
        )
        switches.extend(
            b for b in base_spec.switches if b.name not in (x.name for x in switches)
        )
        checks.extend(
            b for b in base_spec.checks if b.name not in (x.name for x in checks)
        )
        subanalyses.extend(
            b
            for b in base_spec.subanalyses
            if b.name not in (x.name for x in subanalyses)
        )

    analysis_cls.__analysis_spec__ = AnalysisSpec(
        space=space,
        column_specs=tuple(sorted(column_specs, key=attrgetter("name"))),
        pipeline_specs=tuple(sorted(pipeline_specs, key=attrgetter("name"))),
        parameters=tuple(sorted(parameters, key=attrgetter("name"))),
        switches=tuple(sorted(switches, key=attrgetter("name"))),
        checks=tuple(sorted(checks, key=attrgetter("name"))),
        subanalyses=tuple(sorted(subanalyses, key=attrgetter("name"))),
    )

    analysis_cls.__annotations__["__analysis_spec__"] = AnalysisSpec

    return analysis_cls


def _attr_name(cls, counting_attr):
    """Get the name of a counting attribute by reading the original class dict"""
    if isinstance(counting_attr, _Inherited):
        assert counting_attr.resolved_to is not None
        return counting_attr.resolved_to
    try:
        return next(n for n, v in cls.__dict__.items() if v is counting_attr)
    except StopIteration:
        raise AttributeError(f"Attribute {counting_attr} not found in cls {cls}")


# def _attr(cls, counting_attr):
#     return cls.__dict__[_attr_name(cls, counting_attr)]


def get_args_automagically(column_specs, parameters, method, index_start=2):
    """Automagically determine inputs to pipeline or switched by matching
    a methods argument names with columns and parameters of the class

    Parameters
    ----------
    column_specs : list[ColumnSpec]
        the column specs to match the inputs against
    parameters : list[Parameter]
        the parameters to match the inputs against
    method : bound-method
        the method to automagically determine the inputs for
    index_start : int
        the argument index to start from (i.e. can be used to skip the workflow
        arg passed to pipelines classes)

    Returns
    -------
    list[str]
        the names of the input columns to automagically provide to the method
    list[str]
        the names of the parameters to automagically provide to the method
    """
    inputs = []
    used_parameters = []
    column_names = [c.name for c in column_specs]
    param_names = [p.name for p in parameters]
    signature = inspect.signature(method)
    for arg in list(signature.parameters)[
        index_start:
    ]:  # First arg is self and second is the workflow object to add to
        required_type = method.__annotations__.get(arg)
        if arg in column_names:
            column_spec = next(c for c in column_specs if c.name == arg)
            if required_type is not None and required_type is not column_spec.type:
                # Check to see whether conversion is possible
                required_type.find_converter(column_spec.type)
            inputs.append(arg)
        elif arg in param_names:
            used_parameters.append(arg)
        else:
            raise ValueError(f"Unrecognised argument '{arg}'")
    return tuple(inputs), tuple(used_parameters)


@attrs.define
class _Inherited:

    base_class: type
    to_modify: ty.Dict[str, ty.Any]
    resolved_to: str = None

    def resolve(self, name, klass):
        """Resolve to columns and parameters in the specified class

        Parameters
        ----------
        name : str
            the name of the attribute in the class to resolve the inherited attribute to
        klass : type
            the initial class to be transformed into an analysis class
        """
        # Validate inheritance
        if not issubclass(klass, self.base_class):
            raise ValueError(
                f"Trying to inherit '{name}' from class that is not a base "
                f"{self.base_class}"
            )
        try:
            inherited_from = next(
                a for a in self.base_class.__attrs_attrs__ if a.name == name
            )
        except StopIteration:
            raise ValueError(
                f"No attribute named {name} in base class {self.base_class}"
            )
        if inherited_from.inherited:
            raise ValueError(
                "Must inherit from a column that is explicitly defined in "
                f"the base class {self.base_class} (not {name})"
            )

        # Copy type annotation across to new class
        klass.__annotations__[name] = self.base_class.__annotations__[name]

        attr_type = inherited_from.metadata[arcana.core.mark.ATTR_TYPE]
        kwargs = dict(inherited_from.metadata)
        kwargs.pop(arcana.core.mark.ATTR_TYPE)
        if attr_type == "parameter":
            kwargs["default"] = inherited_from.default
        if unrecognised := [k for k in self.to_modify if k not in kwargs]:
            raise TypeError(
                f"Unrecognised keyword args {unrecognised} for {attr_type} attr"
            )
        kwargs.update(self.to_modify)
        resolved = getattr(arcana.core.mark, attr_type)(**kwargs)
        resolved.metadata["defined_in"] = self.base_class
        resolved.metadata["modified"] = tuple(self.to_modify.items())
        return resolved


@attrs.define
class _MappedColumn:

    subanalysis: str
    name: str


@attrs.define
class _UnresolvedOp:
    """An operation within a conditional expression that hasn't been resolved"""

    operator: str
    operands: ty.Tuple[str]

    def resolve(self, klass, column_specs, parameters):
        """Resolves counting attribute operands to the names of attributes in the class

        Parameters
        ----------
        klass : type
            the class being wrapped by attrs.define
        column_specs : list[ColumnSpec]
            the column specs defined in the class
        parameters : list[Parameter]
            the parameters defined in the class

        Return
        ------
        _Op
            the operator with counting-attributes by attrs resolved to attribute names
        """
        resolved = []
        parameter_names = [p.name for p in parameters]
        column_names = [c.name for c in column_specs]
        for operand in self.operands:
            if isinstance(operand, _UnresolvedOp):
                operand = operand.resolve(klass, column_specs, parameters)
            else:
                try:
                    operand = _attr_name(klass, operand)
                except AttributeError:
                    pass
            resolved.append(operand)
        if self.operator == "value_of":
            assert len(resolved) == 1
            if resolved[0] not in parameter_names:
                raise ValueError(
                    f"'value_of' can only be used on parameter attributes not '{operand}'"
                )
        elif self.operator == "is_provided":
            assert len(resolved) <= 2
            if resolved[0] not in column_names:
                raise ValueError(
                    f"'is_provided' can only be used on column specs not '{operand}'"
                )
        return Operation(self.operator, tuple(resolved))

    def __eq__(self, o):
        return _UnresolvedOp("eq", (self, o))

    def __ne__(self, o):
        return _UnresolvedOp("ne", (self, o))

    def __lt__(self, o):
        return _UnresolvedOp("lt", (self, o))

    def __le__(self, o):
        return _UnresolvedOp("le", (self, o))

    def __gt__(self, o):
        return _UnresolvedOp("gt", (self, o))

    def __ge__(self, o):
        return _UnresolvedOp("ge", (self, o))

    def __and__(self, o):
        return _UnresolvedOp("and_", (self, o))

    def __or__(self, o):
        return _UnresolvedOp("or_", (self, o))

    def __invert__(self):
        return _UnresolvedOp("invert_", (self,))


def menu(cls):
    """Defines a menu method on the analysis class"""
    raise NotImplementedError
