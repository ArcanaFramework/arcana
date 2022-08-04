import attrs
from arcana.core.data.space import DataSpace


def analysis(space: DataSpace):
    def decorator():
        return attrs.define()

    return decorator


def column(desc, row_frequency=None):
    return attrs.field(metadata={"desc": desc, "row_frequency": row_frequency})


def pipeline():
    pass
