import attrs
from .data.space import DataSpace


@attrs.define
class ColumnSpec:

    name: str


@attrs.define
class Parameter:

    name: str


def make_analysis(cls, space: DataSpace):

    attrs_cls = attrs.define(cls)
    attrs_cls.__annotations__["dataspace"] = space

    cols_dict = attrs_cls.__annotations__["columns"] = {}
    params_dict = attrs_cls.__annotations__["parameters"] = {}

    for attr in attrs_cls.__attrs_attrs__:
        try:
            attr_type = attr.metadata["type"]
        except KeyError:
            continue
        if attr_type == "column":
            cols_dict[attr.name] = ColumnSpec(attr.name)
        elif attr_type == "parameter":
            params_dict[attr.name] = Parameter(attr.name)
        else:
            raise ValueError(f"Unrecognised attrs type '{attr_type}'")
    return attrs_cls
