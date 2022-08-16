import attrs
from .analysis import _UnresolvedOp, make_analysis_class, Inherited
from .data.space import DataSpace
from .enum import ColumnSalience, ParameterSalience

PIPELINE_ANNOT = "__arcana_pipeline__"
CONVERTER_ANNOT = "__arcana_converter__"
SWICTH_ANNOT = "__arcana_switch__"
CHECK_ANNOT = "__arcana_check__"

ATTR_TYPE = "__arcana_type__"


def converter(output_format):
    def decorator(meth):
        anot = meth.__annotations__
        anot[CONVERTER_ANNOT] = output_format
        return meth

    return decorator


def analysis(space: DataSpace):
    def decorator(cls):
        return make_analysis_class(cls, space)

    return decorator


def column(desc, row_frequency=None, salience=ColumnSalience.supplementary):
    return attrs.field(
        default=None,
        metadata={
            ATTR_TYPE: "column",
            "desc": desc,
            "row_frequency": row_frequency,
            "salience": salience,
        },
    )


def parameter(desc, default=None, choices=None, salience=ParameterSalience.recommended):
    if default is None and salience != ParameterSalience.required:
        raise ValueError(
            "Default value must be provided unless parameter salience is '"
            + str(ParameterSalience.required)
            + "'"
        )
    return attrs.field(
        default=default,
        metadata={
            ATTR_TYPE: "parameter",
            "desc": desc,
            "salience": salience,
            "choices": choices,
        },
    )


def pipeline(*outputs, condition=None):
    """Decorate a instance method that adds nodes to an existing Pydra workflow"""

    def decorator(meth):
        anots = meth.__annotations__[PIPELINE_ANNOT] = {}
        anots["outputs"] = outputs
        anots["condition"] = condition
        return meth

    return decorator


def inherit_from(base, **kwargs):
    """Used to explicitly inherit a column or attribute from a base class so it can be used in a
    sub class. This is enforced in order to make the code more readable (so other developers
    can track where columns/parameters are defined
    """
    return Inherited(base, kwargs)


def value_of(param):
    return _UnresolvedOp("value_of", (param,))


def is_provided(column, in_format: type = None):
    return _UnresolvedOp(
        "is_provided",
        (
            column,
            in_format,
        ),
    )


def switch(meth):
    """Designates a method as being a "switch" that is used to determine which version
    of a pipeline is run

    Parameters
    ----------
    in_task : bool
        whether to wrap the switch in its own task or whether it adds its own nodes
        explicitly"""
    anot = meth.__annotations__
    anot[SWICTH_ANNOT] = True
    return meth


def check(column):
    """Decorate a method, which adds a quality control check to be run against a column"""

    def decorator(meth):
        meth.__annotations__[CHECK_ANNOT] = column
        return meth

    return decorator


def subanalysis(analysis):

    raise NotImplementedError
