import attrs
from .analysis import _UnresolvedOp, make_analysis_class, _Inherited
from .enum import ColumnSalience, ParameterSalience, CheckSalience

PIPELINE_ANNOT = "__arcana_pipeline__"
CONVERTER_ANNOT = "__arcana_converter__"
SWICTH_ANNOT = "__arcana_switch__"
CHECK_ANNOT = "__arcana_check__"

ATTR_TYPE = "__arcana_type__"


def analysis(space: type):
    """Designate a class to be an analysis class

    Parameters
    ----------
    space : type (subclass of DataSpace)
        The data space the analysis operates on, see"""

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
    return attrs.field(
        default=default,
        metadata={
            ATTR_TYPE: "parameter",
            "desc": desc,
            "salience": salience,
            "choices": choices,
        },
    )


def subanalysis(analysis, columns, parameters, desc=None):

    return attrs.field(
        default=None,
        metadata={
            ATTR_TYPE: "subanalysis",
            "analysis": analysis,
            "desc": desc,
            "columns": columns,
            "parameters": parameters,
        },
    )


def pipeline(*outputs, condition=None, switch=None):
    """Decorate a instance method that adds nodes to an existing Pydra workflow

    Parameters
    ----------
    *outputs : list[column]
        outputs produced by the pipeline
    condition : Operation, optional
        condition on which the pipeline will be used instead of the default (the pipeline
        with condition is None)
    switch : str or tuple[str, Any], optional
        name of a "switch" method in the analysis class, which selects nodes to be run
        with this pipeline instead of the default. If a tuple, then the first element
        is the switch name and the second is the return value it should match (the
        return value should be boolean otherwise)
    """

    def decorator(meth):
        anots = meth.__annotations__[PIPELINE_ANNOT] = {}
        anots["outputs"] = outputs
        anots["condition"] = condition
        anots["switch"] = switch
        return meth

    return decorator


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


def check(column, salience=CheckSalience.prudent):
    """Decorate a method, which adds a quality control check to be run against a column"""

    def decorator(meth):
        meth.__annotations__[CHECK_ANNOT] = {"column": column, "salience": salience}
        return meth

    return decorator


def converter(output_format):
    def decorator(meth):
        anot = meth.__annotations__
        anot[CONVERTER_ANNOT] = output_format
        return meth

    return decorator


def inherited_from(base_class, **kwargs):
    """Used to explicitly inherit a column or attribute from a base class so it can be
    used in a sub class. This explicit inheritance is enforced when the column/parameter
    is referenced in the base class in order to make the code more readable (i.e. so
    other developers can track where columns/parameters are defined)

    Parameters
    ----------
    base_class : type
        the base class to inherit the column/parameter from. The name will be matched
        to the name of the column/parameter in the base class
    **kwargs:
        any attributes to override from the inherited column/parameter
    """
    return _Inherited(base_class, kwargs)


def value_of(param):
    """Specifies that the value of the parameter in question should be returned so it
    can be tested within a condition"""
    return _UnresolvedOp("value_of", (param,))


def is_provided(column, in_format: type = None):
    """Test whether a column mapping is specified when the analysis class is applied to a
    dataset"""
    return _UnresolvedOp(
        "is_provided",
        (
            column,
            in_format,
        ),
    )
