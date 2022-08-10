import attrs
from .data.space import DataSpace
from .enum import ColumnSalience, ParameterSalience


@attrs.define
class ColumnSpec:

    name: str
    desc: str
    row_frequency: DataSpace
    salience: ColumnSalience


@attrs.define
class Parameter:

    name: str
    desc: str
    salience: ParameterSalience


def menu(cls):
    """Defines a menu method on the analysis class"""
    raise NotImplementedError


def make_analysis_class(cls, space: DataSpace):
    """
    Construct an analysis class
    """

    # Add generated attributes of the class
    cls.__space__ = space
    cls.__column_specs__ = {}
    cls.__parameters__ = {}

    attrs_cls = attrs.define(cls)

    for attr in attrs_cls.__attrs_attrs__:
        try:
            attr_type = attr.metadata["type"]
        except KeyError:
            continue
        if attr_type == "column":
            row_freq = attr.metadata["row_frequency"]
            if row_freq is None:
                row_freq = max(space)  # "Leaf" frequency of the data tree
            attrs_cls.__column_specs__[attr.name] = ColumnSpec(
                name=attr.name,
                desc=attr.metadata["desc"],
                row_frequency=row_freq,
                salience=attr.metadata["salience"],
            )
        elif attr_type == "parameter":
            attrs_cls.__parameters__[attr.name] = Parameter(
                name=attr.name,
                desc=attr.metadata["desc"],
                salience=attr.metadata["salience"],
            )
        else:
            raise ValueError(f"Unrecognised attrs type '{attr_type}'")
    return attrs_cls
