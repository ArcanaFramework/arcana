from __future__ import annotations
import typing as ty
import attrs
from .space import DataSpace


@attrs.define(kw_only=True)
class ExpDatatypeBlueprint:

    datatype: type
    filenames: list[str]


@attrs.define(kw_only=True)
class DerivBlueprint:

    name: str
    row_frequency: DataSpace
    datatype: type
    filenames: ty.List[str]


@attrs.define(slots=False, kw_only=True)
class TestDatasetBlueprint:

    hierarchy: ty.List[DataSpace]
    dim_lengths: ty.List[int]  # size of layers a-d respectively
    files: ty.List[str]  # files present at bottom layer
    id_inference: ty.List[ty.Tuple[DataSpace, str]] = attrs.field(
        factory=list
    )  # id_inference dict
    expected_datatypes: ty.Dict[str, ExpDatatypeBlueprint] = attrs.field(
        factory=dict
    )  # expected formats
    derivatives: ty.List[DerivBlueprint] = attrs.field(
        factory=list
    )  # files to insert as derivatives

    @property
    def space(self):
        return type(self.hierarchy[0])
