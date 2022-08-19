from copy import copy
import attrs
from .analysis import _UnresolvedOp, make_analysis_class, _InheritedFrom, _MappedFrom
from .enum import ColumnSalience, ParameterSalience, CheckSalience
from arcana.exceptions import ArcanaDesignError


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


def column(
    desc, row_frequency=None, salience=ColumnSalience.supplementary, metadata=None
):
    if metadata is None:
        metadata = {}
    else:
        metadata = copy(metadata)
    metadata.update(
        {
            ATTR_TYPE: "column",
            "desc": desc,
            "row_frequency": row_frequency,
            "salience": salience,
        }
    )
    return attrs.field(
        default=None,
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
    if choices is not None:
        if upper_bound is not None or lower_bound is not None:
            raise ArcanaDesignError(
                f"Cannot specify lower ({lower_bound}) or upper ({lower_bound}) bound "
                f"in conjunction with 'choices' arg ({choices})"
            )
    if metadata is None:
        metadata = {}
    else:
        metadata = copy(metadata)
    metadata.update(
        {
            ATTR_TYPE: "parameter",
            "desc": desc,
            "salience": salience,
            "choices": choices,
            "lower_bound": lower_bound,
            "upper_bound": upper_bound,
        }
    )
    return attrs.field(
        default=default,
        validator=_parameter_validator,
        metadata=metadata,
    )


def subanalysis(desc, **mappings):

    return attrs.field(
        metadata={
            ATTR_TYPE: "subanalysis",
            "desc": desc,
            "mappings": tuple(mappings.items()),
        },
        init=False,
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


def inherited_from(base_class, **to_overwrite):
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
        raise ValueError("Cannot overwrite row_frequency when inheriting")
    return _InheritedFrom(base_class, to_overwrite)


def mapped_from(subanalysis_name, column_name, **to_overwrite):
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


def _parameter_validator(_, attr, val):
    if attr.metadata["choices"] is not None:
        choices = attr.metadata["choices"]
        if val not in choices:
            raise ValueError(
                f"{val} is not a valid value for '{attr.name}' parameter: {choices}"
            )
    else:
        lower_bound = attr.metadata.get("lower_bound")
        upper_bound = attr.metadata.get("upper_bound")
        if not (
            (lower_bound is None or val >= lower_bound)
            and (upper_bound is None or val <= upper_bound)
        ):
            raise ValueError(
                f"Value of '{attr.name}' ({val}) is not within the specified bounds: "
                f"{lower_bound} - {upper_bound}"
            )
