import attrs
import typing as ty
import inspect
from .data.space import DataSpace
from .enum import ColumnSalience, ParameterSalience


@attrs.define
class ColumnSpec:

    name: str
    format: type
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

    attrs_cls = attrs.define(cls)

    column_specs = attrs_cls.__column_specs__
    param_specs = attrs_cls.__parameters__

    for attr in attrs_cls.__attrs_attrs__:
        try:
            attr_type = attr.metadata["type"]
        except KeyError:
            continue
        if attr_type == "column":
            row_freq = attr.metadata["row_frequency"]
            if row_freq is None:
                row_freq = max(space)  # "Leaf" frequency of the data tree
            column_specs[attr.name] = ColumnSpec(
                name=attr.name,
                format=attr.type,
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
        else:
            raise ValueError(f"Unrecognised attrs type '{attr_type}'")

    pipelines = []

    for attr in attrs_cls.__dict__.values():
        try:
            attr_anots = attr.__annotations__
        except AttributeError:
            continue
        try:
            anots = attr_anots["pipeline"]
        except KeyError:
            continue
        outputs = [column_specs[o] for o in anots["outputs"]]
        inputs = []
        parameters = []
        signature = inspect.signature(attr)
        for arg in list(signature.parameters)[2:]:
            if arg in column_specs:
                # TODO: add check on format conversions
                inputs.append(column_specs[arg])
            elif arg in param_specs:
                parameters.append(param_specs[arg])
            else:
                raise ValueError(f"Unrecognised argument {arg}")
        pipelines.append(
            PipelineSpec(
                name=attr.__name__,
                desc=attr.__doc__,
                inputs=inputs,
                outputs=outputs,
                parameters=parameters,
                builder=attr,
            )
        )
    return attrs_cls
