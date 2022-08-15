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
    pipelines: ty.Dict = attrs.Factory(dict)


@attrs.define
class Parameter:

    name: str
    type: type
    desc: str
    salience: ParameterSalience
    inherited: bool


@attrs.define
class Switch:

    name: str
    desc: str
    inherited: bool
    method: ty.Callable = None
    expr: type = None


@attrs.define
class PipelineSpec:

    name: str
    desc: str
    parameters: ty.Tuple[str]
    inputs: ty.Tuple[ColumnSpec]
    outputs: ty.Tuple[ColumnSpec]
    builder: ty.Callable


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
    cls.__switches__ = {}
    cls.__conditions__ = {}

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
                inherited=attr.inherited,
            )
        elif attr_type == "parameter":
            param_specs[attr.name] = Parameter(
                name=attr.name,
                type=attr.type,
                desc=attr.metadata["desc"],
                salience=attr.metadata["salience"],
                inherited=attr.inherited,
            )
        elif attr_type == "switch":
            switch_specs[attr.name] = Switch(
                name=attr.name,
                type=attr.type,
                desc=attr.metadata["desc"],
                salience=attr.metadata["salience"],
                inherited=attr.inherited,
            )
        else:
            raise ValueError(f"Unrecognised attrs type '{attr_type}'")

    pipelines = []

    for attr in attrs_cls.__dict__.values():
        try:
            attr_anots = attr.__annotations__
        except AttributeError:
            continue
        try:
            anots = attr_anots[arcana.core.mark.PIPELINE_ANNOT]
        except KeyError:
            continue
        outputs = [column_specs[_attr_name(cls, o)] for o in anots["outputs"]]
        inputs, parameters = get_args_automagically(
            analysis=attrs_cls, method=attr, index_start=2
        )
        pipeline = PipelineSpec(
            name=attr.__name__,
            desc=attr.__doc__,
            inputs=inputs,
            outputs=outputs,
            parameters=parameters,
            builder=attr,
        )
        pipelines.append(pipeline)
        for output in outputs:
            try:
                switch = _attr(anots["condition"])
            except AttributeError:
                condition = anots["condition"].resolve(attrs_cls)
            else:
                condition = _Op("call", (switch,))
            if condition in output.pipelines:
                existing = output.pipelines[condition]
                raise ValueError(
                    f"Two pipeline builders, '{pipeline.name}' and '{existing.name}', "
                    f"provide outputs for '{output.name}' "
                    f"column under the same condition '{condition}'"
                )
            output.pipelines[condition] = pipeline
    return attrs_cls


def _attr_name(cls, counting_attr):
    """Get the name of a counting attribute by reading the original class dict"""
    try:
        return next(n for n, v in cls.__dict__.items() if v is counting_attr)
    except StopIteration:
        raise AttributeError(f"Attribute {counting_attr} not found in cls {cls}")


def _attr(cls, counting_attr):
    return cls.__dict__[cls._attr_name(counting_attr)]


def get_args_automagically(analysis, method, index_start=1):
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
    for arg in list(signature.parameters)[index_start:]:
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

    def evaluate(self, analysis):
        operands = [o.evaluate(analysis) for o in self.operands]
        if self.operator == "value_of":
            assert len(operands) == 1
            val = getattr(analysis, operands[0])
        elif self.operator == "is_provided":
            assert len(operands) == 1
            val = getattr(analysis, operands[0]) is not None
        else:
            val = getattr(operator_module, self.operator)(*operands)
        return val


@attrs.define
class _UnresolvedOp:

    operator: str
    operands: ty.Tuple[str]

    def resolve(self, klass):
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
                operand = operand.resolve(klass)
            else:
                try:
                    operand = _attr_name(klass, operand)
                except AttributeError:
                    pass
                else:
                    if (
                        self.operator == "value_of"
                        and operand not in klass.__parameters__
                    ):
                        raise ValueError(
                            "'value_of' can only be used on parameter attributes not "
                            f"'{operand}'"
                        )
                    elif (
                        self.operator == "is_provided"
                        and operand not in klass.__column_specs__
                    ):
                        raise ValueError(
                            "'is_provided' can only be used on column specs not "
                            f"'{operand}'"
                        )
            resolved.append(operand)
        return _Op(self.operator, resolved)

    def __eq__(self, o):
        return _UnresolvedOp("eq", o)

    def __ne__(self, o):
        return _UnresolvedOp("ne", o)

    def __lt__(self, o):
        return _UnresolvedOp("lt", o)

    def __le__(self, o):
        return _UnresolvedOp("le", o)

    def __gt__(self, o):
        return _UnresolvedOp("gt", o)

    def __ge__(self, o):
        return _UnresolvedOp("ge", o)
