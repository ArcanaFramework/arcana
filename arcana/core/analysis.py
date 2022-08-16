import attrs
import typing as ty
import inspect
import operator as operator_module
from .data.space import DataSpace
from .enum import ColumnSalience, ParameterSalience
import arcana.core.mark

# from arcana.exceptions import ArcanaFormatConversionError


@attrs.define
class ColumnSpec:

    name: str
    type: type
    desc: str
    row_frequency: DataSpace
    salience: ColumnSalience
    inherited: bool


@attrs.define
class Parameter:

    name: str
    type: type
    desc: str
    default: int or float or str or ty.List[int] or ty.List[float] or ty.List[str]
    choices: ty.List[int] or ty.List[float] or ty.List[str]
    salience: ParameterSalience
    inherited: bool


@attrs.define
class PipelineSpec:

    name: str
    desc: str
    parameters: ty.Tuple[Parameter]
    inputs: ty.Tuple[ColumnSpec]
    outputs: ty.Tuple[ColumnSpec]
    method: ty.Callable


@attrs.define
class CheckSpec:

    name: str
    column: ColumnSpec
    desc: str
    inputs: ty.Tuple[str]
    parameters: ty.Tuple[ColumnSpec]
    method: ty.Callable


@attrs.define(frozen=True)
class Switch:

    name: str
    desc: str
    inputs: ty.Tuple[str]
    parameters: ty.Tuple[ColumnSpec]
    method: ty.Callable


@attrs.define
class Subanalysis:

    name: str
    analysis: type


@attrs.define
class Inherited:

    base_class: type
    to_overwrite: ty.Dict[str, ty.Any]
    resolved_to: str = None


def menu(cls):
    """Defines a menu method on the analysis class"""
    raise NotImplementedError


def make_analysis_class(cls, space: DataSpace):
    """
    Construct an analysis class and validate all the components fit together
    """

    # Add generated attributes of the class
    cls.__space__ = space
    cls.__column_specs__ = {}
    cls.__parameters__ = {}
    cls.__pipelines__ = {}
    cls.__switches__ = {}
    cls.__checks__ = {}

    # Resolve inherited attributes
    for name, inherited in list(cls.__dict__.items()):
        if isinstance(inherited, Inherited):
            # Validate inheritance
            if not issubclass(cls, inherited.base_class):
                raise ValueError(
                    f"Trying to inherit '{name}' from class that is not a base "
                    f"{inherited.base_class}"
                )
            try:
                inherit_from = next(
                    a for a in inherited.base_class.__attrs_attrs__ if a.name == name
                )
            except StopIteration:
                raise ValueError(
                    f"No attribute named {name} in base class {inherited.base_class}"
                )
            if inherit_from.inherited:
                raise ValueError(
                    "Must inheritedify inherit from a column that is explicitly defined in "
                    f"the base class {inherited.base_class} (not {name})"
                )

            # Copy type annotation across to new class
            cls.__annotations__[name] = inherited.base_class.__annotations__[name]

            attr_type = inherit_from.metadata[arcana.core.mark.ATTR_TYPE]
            kwargs = dict(inherit_from.metadata)
            kwargs.pop(arcana.core.mark.ATTR_TYPE)
            if attr_type == "parameter":
                kwargs["default"] = inherit_from.default
            if unrecognised := [k for k in inherited.to_overwrite if k not in kwargs]:
                raise TypeError(
                    f"Unrecognised keyword args {unrecognised} for {attr_type} attr"
                )
            kwargs.update(inherited.to_overwrite)
            counting_attr = getattr(arcana.core.mark, attr_type)(**kwargs)
            counting_attr.metadata["inherited"] = True
            setattr(cls, name, counting_attr)
            inherited.resolved_to = name

    attrs_cls = attrs.define(cls)

    column_specs = attrs_cls.__column_specs__
    param_specs = attrs_cls.__parameters__
    switch_specs = attrs_cls.__switches__

    for attr in attrs_cls.__attrs_attrs__:
        try:
            attr_type = attr.metadata[arcana.core.mark.ATTR_TYPE]
        except KeyError:
            continue
        if attr_type == "column":
            row_freq = attr.metadata["row_frequency"]
            if row_freq is None:
                row_freq = max(space)  # "Leaf" frequency of the data tree
            column_specs[attr.name] = ColumnSpec(
                name=attr.name,
                type=attr.type,
                desc=attr.metadata["desc"],
                row_frequency=row_freq,
                salience=attr.metadata["salience"],
                inherited=attr.inherited or attr.metadata.get("inherited"),
            )
        elif attr_type == "parameter":
            param_specs[attr.name] = Parameter(
                name=attr.name,
                type=attr.type,
                default=attr.default,
                choices=attr.metadata["choices"],
                desc=attr.metadata["desc"],
                salience=attr.metadata["salience"],
                inherited=attr.inherited or attr.metadata.get("inherited"),
            )
        else:
            raise ValueError(f"Unrecognised attrs type '{attr_type}'")

    pipelines = []

    for attr in attrs_cls.__dict__.values():
        try:
            attr_anots = attr.__annotations__
        except AttributeError:
            continue

        if arcana.core.mark.PIPELINE_ANNOT in attr_anots:
            anots = attr_anots[arcana.core.mark.PIPELINE_ANNOT]
            outputs = [column_specs[_attr_name(cls, o)] for o in anots["outputs"]]
            inputs, parameters = get_args_automagically(analysis=attrs_cls, method=attr)
            pipeline = PipelineSpec(
                name=attr.__name__,
                desc=attr.__doc__,
                inputs=inputs,
                outputs=outputs,
                parameters=parameters,
                method=attr,
            )
            pipelines.append(pipeline)
            for output in outputs:
                unresolved_condition = anots["condition"]
                if unresolved_condition is not None:
                    try:
                        condition = _attr_name(cls, unresolved_condition)
                    except AttributeError:
                        condition = unresolved_condition.resolve(cls, attrs_cls)
                else:
                    condition = None
                if condition in output.pipelines:
                    existing = output.pipelines[condition]
                    raise ValueError(
                        f"Two pipeline methods, '{pipeline.name}' and '{existing.name}', "
                        f"provide outputs for '{output.name}' "
                        f"column under the same condition '{condition}'"
                    )
                output.pipelines[condition] = pipeline
        elif arcana.core.mark.SWICTH_ANNOT in attr_anots:
            inputs, parameters = get_args_automagically(analysis=attrs_cls, method=attr)
            switch_specs[attr.__name__] = Switch(
                name=attr.__name__,
                desc=__doc__,
                inputs=tuple(inputs),
                parameters=tuple(parameters),
                method=attr,
            )
        elif arcana.core.mark.CHECK_ANNOT in attr_anots:
            column = column_specs[
                _attr_name(cls, attr_anots[arcana.core.mark.CHECK_ANNOT])
            ]
            inputs, parameters = get_args_automagically(analysis=attrs_cls, method=attr)
            check = CheckSpec(
                name=attr.__name__,
                column=column,
                desc=attr.__doc__,
                inputs=inputs,
                parameters=parameters,
                method=attr,
            )
            column.checks.append(check)

    return attrs_cls


def _attr_name(cls, counting_attr):
    """Get the name of a counting attribute by reading the original class dict"""
    if isinstance(counting_attr, Inherited):
        assert counting_attr.resolved_to is not None
        return counting_attr.resolved_to
    try:
        return next(n for n, v in cls.__dict__.items() if v is counting_attr)
    except StopIteration:
        raise AttributeError(f"Attribute {counting_attr} not found in cls {cls}")


def _attr(cls, counting_attr):
    return cls.__dict__[_attr_name(cls, counting_attr)]


def get_args_automagically(analysis, method, index_start=2):
    """Automagically determine inputs to pipeline or switched by matching
    a methods argument names with columns and parameters of the class

    Parameters
    ----------
    analysis : type
        the analysis class to search the columns and parameters from
    method : bound-method
        the method to automagically determine the inputs for
    index_start : int
        the argument index to start from (i.e. can be used to skip the workflow
        arg passed to pipelines classes)

    Returns
    -------
    list[ColumnSpec]
        the input columns to automagically provide to the method
    list[ParameterSpec]
        the parameters to automagically provide to the method
    """
    inputs = []
    parameters = []
    signature = inspect.signature(method)
    for arg in list(signature.parameters)[
        index_start:
    ]:  # First arg is self and second is the workflow object to add to
        required_type = method.__annotations__.get(arg)
        if arg in analysis.__column_specs__:
            spec = analysis.__column_specs__[arg]
            if required_type is not None:
                if required_type is not spec.type:
                    # Check to see whether conversion is possible
                    required_type.find_converter(spec.type)
            inputs.append(spec)
        elif arg in analysis.__parameters__:
            # TODO: Type check required type can be cast from parameter type
            param = analysis.__parameters__[arg]
            parameters.append(param)
        else:
            raise ValueError(f"Unrecognised argument {arg}")
    return inputs, parameters


@attrs.define(frozen=True)
class _Op:

    operator: str
    operands: ty.Tuple[str]

    def evaluate(self, analysis, dataset):
        operands = [o.evaluate(analysis) for o in self.operands]
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


@attrs.define
class _UnresolvedOp:
    """An operation within a conditional expression that hasn't been resolved"""

    operator: str
    operands: ty.Tuple[str]

    def resolve(self, cls, attrs_cls):
        """Resolves counting attribute operands to the names of attributes in the class

        Parameters
        ----------
        klass : type
            the class being wrapped by attrs.define

        Return
        ------
        _Op
            the operator with counting-attributes by attrs resolved to attribute names
        """
        resolved = []
        for operand in self.operands:
            if isinstance(operand, _UnresolvedOp):
                operand = operand.resolve(cls)
            else:
                try:
                    operand = _attr_name(cls, operand)
                except AttributeError:
                    pass
            resolved.append(operand)
        if self.operator == "value_of":
            assert len(resolved) == 1
            if resolved[0] not in attrs_cls.__parameters__:
                raise ValueError(
                    f"'value_of' can only be used on parameter attributes not '{operand}'"
                )
        elif self.operator == "is_provided":
            assert len(resolved) <= 2
            if resolved[0] not in attrs_cls.__column_specs__:
                raise ValueError(
                    f"'is_provided' can only be used on column specs not '{operand}'"
                )
        return _Op(self.operator, tuple(resolved))

    def __eq__(self, o):
        return _UnresolvedOp("eq", (o,))

    def __ne__(self, o):
        return _UnresolvedOp("ne", (o,))

    def __lt__(self, o):
        return _UnresolvedOp("lt", (o,))

    def __le__(self, o):
        return _UnresolvedOp("le", (o,))

    def __gt__(self, o):
        return _UnresolvedOp("gt", (o,))

    def __ge__(self, o):
        return _UnresolvedOp("ge", (o,))
