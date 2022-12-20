from __future__ import annotations
from types import MemberDescriptorType
from .spec import (
    _UnresolvedOp,
    _Inherited,
    _MappedFrom,
    ColumnSpec,
    Parameter,
    SubanalysisSpec,
)
from .make import make
from .salience import ColumnSalience, ParameterSalience, CheckSalience
from ..utils.misc import (
    SWICTH_ANNOTATIONS,
    CHECK_ANNOTATIONS,
    PIPELINE_ANNOTATIONS,
    CONVERTER_ANNOTATIONS,
)


def analysis(space: type):
    """Designate a class to be an analysis class

    Parameters
    ----------
    space : type (subclass of DataSpace)
        The data space the analysis operates on, see"""

    def decorator(klass):
        return make(klass, space)

    return decorator


def column(
    desc,
    row_frequency=None,
    salience=ColumnSalience.supplementary,
    metadata=None,
):
    return ColumnSpec(
        desc=desc,
        row_frequency=row_frequency,
        salience=salience,
        metadata=metadata,
    )


def parameter(
    desc,
    default=None,
    choices=None,
    lower_bound=None,
    upper_bound=None,
    salience=ParameterSalience.recommended,
    metadata=None,
):
    return Parameter(
        desc=desc,
        default=default,
        choices=choices,
        lower_bound=lower_bound,
        upper_bound=upper_bound,
        salience=salience,
        metadata=metadata,
    )


def subanalysis(desc, metadata=None, **mappings):
    return SubanalysisSpec(
        desc=desc, mappings=tuple(mappings.items()), metadata=metadata
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
        anots = meth.__annotations__[PIPELINE_ANNOTATIONS] = {}
        anots["outputs"] = outputs
        anots["condition"] = condition
        anots["switch"] = switch.__name__ if switch is not None else None
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
    anot[SWICTH_ANNOTATIONS] = True
    return meth


def check(column, salience=CheckSalience.prudent):
    """Decorate a method, which adds a quality control check to be run against a column"""

    def decorator(meth):
        meth.__annotations__[CHECK_ANNOTATIONS] = {
            "column": column,
            "salience": salience,
        }
        return meth

    return decorator


def inherit(ref: MemberDescriptorType = None, **to_overwrite):
    """Used to explicitly inherit a column or attribute from a base class so it can be
    used in a sub class. This explicit inheritance is enforced when the column/parameter
    is referenced in the base class in order to make the code more readable (i.e. so
    other developers can track where columns/parameters are defined)

    Parameters
    ----------
    ref : MemberDescriptorType, optional
        a reference to the field that is being inherited from. Note that it is not
        actually used for anything, the field to be inherited is determined by scanning
        the method-resolution order for matching names, but it can make the code more
        readable by linking the inherited attribute with its initial definition.
    **to_overwrite:
        any attributes to override from the inherited column/parameter
    """
    if "row_frequency" in to_overwrite:
        raise ValueError("Cannot overwrite row_frequency when inheriting")
    return _Inherited(to_overwrite)


def map_from(subanalysis_name, column_name, **to_overwrite):
    """Used to explicitly inherit a column or attribute from a base class so it can be
    used in a sub class. This explicit inheritance is enforced when the column/parameter
    is referenced in the base class in order to make the code more readable (i.e. so
    other developers can track where columns/parameters are defined)

    Parameters
    ----------
    base_class : type
        the base class to inherit the column/parameter from. The name will be matched
        to the name of the column/parameter in the base class
    **to_overwrite:
        any attributes to override from the inherited column/parameter
    """
    if "row_frequency" in to_overwrite:
        raise ValueError("Cannot overwrite row_frequency when mapping")
    return _MappedFrom(subanalysis_name, column_name, to_overwrite)


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


def converter(output_format):
    def decorator(meth):
        anot = meth.__annotations__
        anot[CONVERTER_ANNOTATIONS] = output_format
        return meth

    return decorator
