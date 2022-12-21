from __future__ import annotations
import typing as ty
import itertools
from copy import copy
from collections import defaultdict
import operator as operator_module
import attrs
from attrs.converters import default_if_none
from ..data.space import DataSpace
from .salience import CheckSalience, ColumnSalience, ParameterSalience
from ..utils.misc import ARCANA_SPEC
from arcana.core.exceptions import ArcanaDesignError


@attrs.define
class BaseAttr:

    name: str = None  # Default to None, as will be set later after class is initialised
    type: type = None
    desc: str = None
    inherited: bool = False
    defined_in: ty.Tuple[type] = ()
    modified: ty.Tuple[ty.Tuple[str, ty.Any]] = ()
    mapped_from: ty.Tuple[
        str, str or ty.Tuple
    ] or None = None  # sub-analysis name, column name
    metadata: dict = attrs.field(
        factory=dict, converter=default_if_none(default=attrs.Factory(dict))
    )


@attrs.define(frozen=True)
class ColumnSpec(BaseAttr):
    """Specifies a column that the analysis can add when it is applied to a dataset"""

    row_frequency: DataSpace = None
    salience: ColumnSalience = ColumnSalience.default()

    def select_pipeline_builders(self, analysis, dataset):
        candidates = [
            p for p in analysis.__spec__.pipeline_builders if self.name in p.outputs
        ]
        selected = [
            m
            for m in candidates
            if (
                m.condition is not None
                and m.condition.evaluate(self, analysis, dataset)
            )
        ]
        # Check for defaults
        if not selected:
            selected = [p for p in candidates if p.condition is None]
        # Select pipeline builders from subanalysis if present
        if not selected and self.mapped_from is not None:
            subanalysis = getattr(analysis, self.mapped_from[0])
            sub_column_spec = subanalysis.__spec__.column(self.mapped_from[1])
            selected = sub_column_spec.select_pipeline_builders(subanalysis, dataset)

        if not selected:
            raise ArcanaDesignError(
                "Could not find any potential pipeline builders with conditions that "
                "match the current analysis parameterisation and provided dataset. "
                f"All candidates are: {candidates}"
            )
        # Check to see whether there are pipelines with the same switch
        all_switches = [p.switch for p in selected]
        if with_duplicate_switches := [
            p for p in selected if all_switches.count(p.switch)
        ]:
            raise ArcanaDesignError(
                "Multiple potential pipelines match criteria for the given analysis "
                f"configuration and provided dataset: {with_duplicate_switches}"
            )
        return selected

    def to_attrs_field(self):
        return attrs.field(
            default=None,
            metadata={ARCANA_SPEC: self},
        )


@attrs.define(frozen=True)
class Parameter(BaseAttr):
    """Specifies a free parameter of an analysis"""

    default: int or float or str or ty.Tuple[int] or ty.Tuple[float] or ty.Tuple[
        str
    ] = attrs.field(default=None)
    salience: ParameterSalience = None
    choices: ty.Union[
        ty.Tuple[int], ty.Tuple[float], ty.Tuple[str], None
    ] = attrs.field(default=None)
    lower_bound: ty.Union[int, float, None] = None
    upper_bound: ty.Union[int, float, None] = None

    @default.validator
    def default_validator(self, _, default):
        if default is None:
            if self.salience != ParameterSalience.required:
                raise ValueError(
                    f"Default value for '{self.name}' parameter must be provided unless "
                    f"parameter salience is set to'{ParameterSalience.required}'"
                )
        else:
            if self.lower_bound is not None and default < self.lower_bound:
                raise ValueError(
                    f"Default provided to '{self.name}' ({default}) is lower than "
                    f"lower bound ({self.lower_bound})"
                )
            if self.upper_bound is not None and default > self.upper_bound:
                raise ValueError(
                    f"Default provided to '{self.name}' ({default}) is higher than "
                    f"upper bound ({self.upper_bound})"
                )

    @choices.validator
    def choices_validator(self, _, choices):
        if choices is not None:
            if self.upper_bound is not None or self.lower_bound is not None:
                raise ArcanaDesignError(
                    f"Cannot specify lower ({self.lower_bound}) or upper "
                    f"({self.upper_bound}) bound in conjunction with 'choices' arg "
                    f"({choices})"
                )

    def to_attrs_field(self):
        return attrs.field(
            default=self.default,
            validator=_parameter_validator,
            metadata={ARCANA_SPEC: self},
        )


@attrs.define(frozen=True)
class SubanalysisSpec(BaseAttr):
    """Specifies a "sub-analysis" component, when composing an analysis of several
    predefined analyses"""

    mappings: ty.Tuple[
        str, str
    ] = ()  # to name in subanalysis, from name in analysis class

    def mapping(self, name):
        try:
            mapped_name = next(m[1] for m in self.mappings if name == m[0])
        except StopIteration:
            raise KeyError(f"No mapping from '{name}' in sub-analysis: {self.mappings}")
        return mapped_name

    def to_attrs_field(self):
        return attrs.field(
            metadata={ARCANA_SPEC: self},
            factory=dict,
        )


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
                    val = column.datatype is in_format or issubclass(
                        column.datatype, in_format
                    )
                else:
                    val = True
        else:
            val = getattr(operator_module, self.operator)(*operands)
        return val


@attrs.define(frozen=True)
class BaseMethod:

    name: str
    desc: str
    inputs: ty.Tuple[str]
    parameters: ty.Tuple[str]
    method: ty.Callable
    defined_in: ty.Tuple[type]


@attrs.define(frozen=True)
class Switch(BaseMethod):
    """Specifies a "switch" point at which the processing can bifurcate to handle two
    separate types of input streams"""

    pass


@attrs.define(frozen=True)
class PipelineConstructor(BaseMethod):
    """Specifies a method that is used to add nodes in the construction of a pipeline
    that is able to generate data for sink columns under certain conditions"""

    outputs: ty.Tuple[str]
    condition: ty.Union[Operation, None] = None
    switch: ty.Union[Switch, None] = None


@attrs.define(frozen=True)
class Check(BaseMethod):
    """Specifies a quality-control check that can be run on generated derivatives to
    assess the probability that they have failed"""

    column: ColumnSpec
    salience: CheckSalience = CheckSalience.default()


def unique_names(inst, attr, val):
    names = [v.name for v in val]
    if duplicates := [v for v in val if names.count(v.name) > 1]:
        raise ValueError(f"Duplicate names found in provided tuple: {duplicates}")


@attrs.define(frozen=True)
class AnalysisSpec:
    """Specifies all the components of the analysis class"""

    space: type
    column_specs: ty.Tuple[ColumnSpec] = attrs.field(validator=unique_names)
    pipeline_builders: ty.Tuple[PipelineConstructor] = attrs.field(
        validator=unique_names
    )
    parameters: ty.Tuple[Parameter] = attrs.field(validator=unique_names)
    switches: ty.Tuple[Switch] = attrs.field(validator=unique_names)
    checks: ty.Tuple[Check] = attrs.field(validator=unique_names)
    subanalysis_specs: ty.Tuple[SubanalysisSpec] = attrs.field(validator=unique_names)

    @property
    def column_names(self):
        return (c.name for c in self.column_specs)

    @property
    def parameter_names(self):
        return (p.name for p in self.parameters)

    @property
    def pipeline_names(self):
        return (p.name for p in self.pipeline_builders)

    @property
    def switch_names(self):
        return (s.name for s in self.switches)

    @property
    def check_names(self):
        return (c.name for c in self.checks)

    @property
    def subanalysis_names(self):
        return (s.name for s in self.subanalysis_specs)

    def column_spec(self, name):
        try:
            return next(c for c in self.column_specs if c.name == name)
        except StopIteration:
            raise KeyError(f"No column spec named '{name}' in {self}")

    def parameter(self, name):
        try:
            return next(p for p in self.parameters if p.name == name)
        except StopIteration:
            raise KeyError(f"No parameter named '{name}' in {self}")

    def subanalysis_spec(self, name):
        try:
            return next(s for s in self.subanalysis_specs if s.name == name)
        except StopIteration:
            raise KeyError(f"No subanalysis spec named '{name}' in {self}")

    def pipeline_builder(self, name):
        try:
            return next(p for p in self.pipeline_builders if p.name == name)
        except StopIteration:
            raise KeyError(f"No pipeline builder named '{name}' in {self}")

    def switch(self, name):
        try:
            return next(s for s in self.switches if s.name == name)
        except StopIteration:
            raise KeyError(f"No switches named '{name}' in {self}")

    def check(self, name):
        try:
            return next(c for c in self.checks if c.name == name)
        except StopIteration:
            raise KeyError(f"No checks named '{name}' in {self}")

    def member(self, name):
        try:
            return next(m for m in self.members() if m.name == name)
        except StopIteration:
            raise KeyError(f"No member named '{name}' in {self}")

    def members(self):
        return itertools.chain(
            self.column_specs, self.parameters, self.subanalysis_specs
        )

    def column_checks(self, column_name):
        "Return all checks for a given column"
        return (c for c in self.checks if c.column == column_name)

    @column_specs.validator
    def column_specs_validator(self, _, column_specs):
        for column_spec in column_specs:
            sorted_by_cond = defaultdict(list)
            for pipe_spec in self.pipeline_builders:
                if column_spec.name in pipe_spec.outputs:
                    sorted_by_cond[(pipe_spec.condition, pipe_spec.switch)].append(
                        pipe_spec
                    )
            if duplicated := [
                (c, d) for (c, d) in sorted_by_cond.items() if len(d) > 1
            ]:
                raise ArcanaDesignError(
                    f"Multiple pipelines provide outputs for '{column_spec.name}' under "
                    "matching conditions - \n"
                    + "\n".join(
                        f"Condition: {cond[0]}, Switch: {cond[1]} - "
                        + ", ".join(str(p) for p in dups)
                        for cond, dups in duplicated
                    )
                )
            if not sorted_by_cond and not column_spec.mapped_from:
                inputs_to = [
                    p for p in self.pipeline_builders if column_spec.name in p.inputs
                ]
                if not inputs_to:
                    raise ArcanaDesignError(
                        f"'{column_spec.name}' is neither an input nor output to any pipeline"
                    )
                if column_spec.salience.level <= ColumnSalience.publication.level:
                    raise ArcanaDesignError(
                        f"'{column_spec.name}' is not generated by any pipeline yet its salience "
                        f"is not specified as 'raw' or 'primary'"
                    )

    @pipeline_builders.validator
    def pipeline_builders_validator(self, _, pipeline_builders):
        for pipeline_builder in pipeline_builders:
            if missing_outputs := [
                o for o in pipeline_builder.outputs if o not in self.column_names
            ]:
                raise ArcanaDesignError(
                    f"'{pipeline_builder.name}' pipeline outputs to unknown columns: {missing_outputs}"
                )


@attrs.define
class _Inherited:

    to_overwrite: ty.Dict[str, ty.Any]
    resolved_to: str = None
    name: str = None

    def resolve(self, name, klass):
        """Resolve to columns and parameters in the specified class

        Parameters
        ----------
        name : str
            the name of the attribute in the class to resolve the inherited attribute to
        klass : type
            the initial class to be transformed into an analysis class
        """
        self.name = name
        defining_class = None
        for base in klass.__mro__[1:-1]:  # skip current class and base "object" class
            if name in base.__dict__:
                defining_class = base
        if not defining_class:
            raise AttributeError(
                f"Supers of {klass} have no attribute named '{name}' to inherit"
            )
        attr_to_inherit = getattr(attrs.fields(defining_class), name).metadata[
            ARCANA_SPEC
        ]
        kwargs = copy(self.to_overwrite)
        if self.to_overwrite:
            kwargs["modified"] = attr_to_inherit.modified + (
                tuple(self.to_overwrite.items()),
            )
        kwargs["inherited"] = True
        resolved = attrs.evolve(attr_to_inherit, **kwargs)
        # Return the resolved attribute and its type annotation
        return resolved, resolved.type


@attrs.define
class _MappedFrom:

    subanalysis_name: str
    attr_name: str
    to_overwrite: ty.Dict[str, ty.Any]
    resolved_to: str = None
    name: str = None

    def resolve(self, name, klass):
        """Resolve to a column temporary attribute to be transformed into a attribute
        of the analysis class

        Parameters
        ----------
        name : str
            the name of the attribute in the class to resolve the inherited attribute to
        klass : type
            the initial class to be transformed into an analysis class
        """
        self.name = name
        analysis_class = klass.__annotations__[self.subanalysis_name]
        analysis_spec = analysis_class.__spec__
        # Get the Attribute in the subanalysis class
        try:
            attr_spec = analysis_spec.member(self.attr_name)
        except KeyError:
            raise ArcanaDesignError(
                f"No member attribute named '{self.attr_name}' in subanalysis "
                f"'{self.subanalysis_name}' ({analysis_class}): "
                + str([a.name for a in analysis_class.__attrs_attrs__])
            )
        kwargs = copy(self.to_overwrite)
        kwargs["mapped_from"] = (self.subanalysis_name, self.attr_name)
        kwargs["modified"] = attr_spec.modified + (tuple(self.to_overwrite.items()),)
        resolved = attrs.evolve(attr_spec, **kwargs)
        return resolved, attr_spec.type


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
        for operand in self.operands:
            if isinstance(operand, _UnresolvedOp):
                operand = operand.resolve(klass, column_specs, parameters)
            else:
                try:
                    operand = operand.resolved_to
                except AttributeError:
                    pass
            resolved.append(operand)
        if self.operator == "value_of":
            assert len(resolved) == 1
            if resolved[0] not in parameters:
                raise ValueError(
                    f"'value_of' can only be used on parameter attributes not '{operand}'"
                )
        elif self.operator == "is_provided":
            assert len(resolved) <= 2
            if resolved[0] not in column_specs:
                raise ValueError(
                    f"'is_provided' can only be used on column specs not '{operand}'"
                )
        return Operation(
            self.operator, tuple(a.name if hasattr(a, "name") else a for a in resolved)
        )

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


def _parameter_validator(self, attr, val):
    spec = attr.metadata[ARCANA_SPEC]
    if spec.salience is ParameterSalience.required and val is None:
        raise ValueError(
            f"A value needs to be provided to required parameter '{attr.name}' in {self}"
        )
    if spec.choices is not None and val not in spec.choices:
        val_str = f"'{val}'" if isinstance(val, str) else val
        raise ValueError(
            f"{val_str} is not a valid value for '{attr.name}' parameter in {self}, "
            f"valid choices are {spec.choices}"
        )
    if not (
        (spec.lower_bound is None or val >= spec.lower_bound)
        and (spec.upper_bound is None or val <= spec.upper_bound)
    ):
        raise ValueError(
            f"Value of '{attr.name}' ({val}) is not within the specified bounds: "
            f"{spec.lower_bound} - {spec.upper_bound} in {self}"
        )


# def _column_validator(self, attr, val):
#     if val not in self._dataset.columns():
#         raise ValueError(
#             f"Unrecognised column name '{val}' provided as mapping for {attr.name} "
#             "column spec")
