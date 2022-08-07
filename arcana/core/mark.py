import attrs
from .analysis import validate_analysis
from .data.space import DataSpace
from .enum import DataSalience, ParamSalience


def converter(output_format):
    def decorator(meth):
        anot = meth.__annotations__
        anot["arcana_converter"] = output_format
        return meth

    return decorator


def analysis(space: DataSpace):
    def decorator(cls):
        cls = attrs.define(cls)
        cls.__annotations__["arcana_dataspace"] = space
        validate_analysis(cls)
        return cls

    return decorator


def column(desc, row_frequency=None, salience=DataSalience.supplementary):
    return attrs.field(
        metadata={
            "type": "column",
            "desc": desc,
            "row_frequency": row_frequency,
            "salience": salience,
        }
    )


def parameter(desc, salience=ParamSalience.recommended):
    return attrs.field(
        metadata={"type": "parameter", "desc": desc, "salience": salience}
    )


def pipeline(*outputs):
    def decorator(meth):
        anot = meth.__annotations__
        anot["arcana_outputs"] = outputs
        return meth

    return decorator
