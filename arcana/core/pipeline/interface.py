from __future__ import annotations
import logging
import typing as ty
import attrs
from arcana.core.utils import (
    path2varname,
    str2datatype,
    str2class,
)
import arcana.data.formats.common
from arcana.core.data.space import DataSpace


logger = logging.getLogger("arcana")


@attrs.define
class PipelineInput:
    name: str  # How the input will be referred to in the XNAT dialog, defaults to the field name
    help_string: str  # description of the input
    datatype: type = attrs.field(converter=str2datatype)
    path: str = attrs.field()
    field: str = attrs.field()  # Must match the name of the Pydra task input field
    stored_format: type = attrs.field(
        converter=str2datatype
    )  # the format the input is stored in the data store in
    row_frequency: DataSpace = None

    @format.default
    def format_default(self):
        return arcana.data.formats.common.File

    @stored_format.default
    def stored_format_default(self):
        return self.datatype

    @field.default
    def field_default(self):
        return self.name

    @path.default
    def path_default(self):
        return self.name


@attrs.define
class PipelineOutput:
    name: str
    help_string: str  # description of the input
    path: str = attrs.field()  # The path the output is stored at in XNAT
    datatype: type = attrs.field(converter=str2datatype)
    field: str = (
        attrs.field()
    )  # Must match the name of the Pydra task output, defaults to the path
    stored_format: type = attrs.field(
        converter=str2datatype
    )  # the format the output is to be stored in the data store in

    @format.default
    def format_default(self):
        return arcana.data.formats.common.File

    @stored_format.default
    def stored_format_default(self):
        return self.datatype

    @field.default
    def field_default(self):
        return self.name

    @path.default
    def path_default(self):
        return self.name


@attrs.define
class PipelineParameter:
    name: str  # How the input will be referred to in the XNAT dialog, defaults to field name
    help_string: str  # description of the parameter
    type: type = attrs.field(converter=str2class)
    field: str = attrs.field()  # Name of parameter to expose in Pydra task
    required: bool = False
    default: ty.Union[str, int, float, bool] = None

    @field.default
    def field_default(self):
        return path2varname(self.name)
