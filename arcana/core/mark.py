import typing as ty
import attrs
from .analysis import make_analysis_class
from .data.space import DataSpace
from .enum import ColumnSalience, ParameterSalience


def converter(output_format):
    def decorator(meth):
        anot = meth.__annotations__
        anot["arcana_converter"] = output_format
        return meth

    return decorator


def analysis(space: DataSpace):
    def decorator(cls):
        return make_analysis_class(cls, space)

    return decorator


def column(desc, row_frequency=None, salience=ColumnSalience.supplementary):
    return attrs.field(
        metadata={
            "type": "column",
            "desc": desc,
            "row_frequency": row_frequency,
            "salience": salience,
        }
    )


def parameter(desc, salience=ParameterSalience.recommended):
    return attrs.field(
        metadata={"type": "parameter", "desc": desc, "salience": salience}
    )


def pipeline(*outputs, row_frequency=None, condition=None):
    def decorator(meth):
        anots = meth.__annotations__["pipeline"] = {}
        anots["outputs"] = outputs
        anots["row_frequency"] = row_frequency
        anots["condition"] = condition
        return meth

    return decorator


def inherit(attr):
    """Used to explicitly inherit a column or attribute from a base class so it can be used in a
    sub class. This is enforced in order to make the code more readable (so other developers
    can track where columns/parameters are defined
    """
    raise NotImplementedError


@attrs.define
class Equals:

    parameter: ty.Any
    value: ty.Any


def switch(meth):
    def decorator(meth):
        anot = meth.__annotations__
        anot["arcana_switch"] = True
        return meth

    return decorator
