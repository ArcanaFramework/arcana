import typing as ty
import attrs
from arcana.core.utils import path2varname, ListDictConverter, format_resolver
import arcana.data.formats.common
from arcana.core.data.space import DataSpace


@attrs.define
class CommandInput:
    name: str  # How the input will be referred to in the XNAT dialog, defaults to the task_field name
    description: str  # description of the input
    format: type = attrs.field(converter=format_resolver)
    path: str = attrs.field()
    stored_format: type = attrs.field(
        converter=format_resolver
    )  # the format the input is stored in the data store in
    task_field: str = attrs.field()  # Must match the name of the Pydra task input
    row_frequency: DataSpace = None

    @format.default
    def format_default(self):
        return arcana.data.formats.common.File

    @stored_format.default
    def stored_format_default(self):
        return self.format

    @task_field.default
    def field_default(self):
        return self.name

    @path.default
    def path_default(self):
        return self.name


@attrs.define
class CommandOutput:
    name: str
    path: str = attrs.field()  # The path the output is stored at in XNAT
    format: type = attrs.field(converter=format_resolver)
    task_field: str = (
        attrs.field()
    )  # Must match the name of the Pydra task output, defaults to the path
    stored_format: type = attrs.field(
        converter=format_resolver
    )  # the format the output is to be stored in the data store in

    @format.default
    def format_default(self):
        return arcana.data.formats.common.File

    @stored_format.default
    def stored_format_default(self):
        return self.format

    @task_field.default
    def field_default(self):
        return self.name

    @path.default
    def path_default(self):
        return self.name


@attrs.define
class CommandParameter:
    name: str  # How the input will be referred to in the XNAT dialog, defaults to task_field name
    description: str  # description of the parameter
    task_field: str = attrs.field()  # Name of parameter to expose in Pydra task
    type: type = str
    required: bool = False
    default = None

    @task_field.default
    def task_field_default(self):
        return path2varname(self.name)


@attrs.define
class ContainerCommandSpec:
    """
    Parameters
    ----------
    long_description : str
        A long description of the pipeline, used in documentation and ignored
        here. Only included in the signature so that an error isn't thrown when
        it is encountered.
    known_issues : str
        Any known issues with the pipeline. To be used in auto-doc generation
    """

    name: str
    task: str
    description: str
    long_description: str
    known_issues: str
    row_frequency: DataSpace
    inputs: ty.List[CommandInput] = attrs.field(
        factory=list, converter=ListDictConverter(CommandInput)
    )
    outputs: ty.List[CommandOutput] = attrs.field(
        factory=list, converter=ListDictConverter(CommandOutput)
    )
    parameters: ty.List[CommandParameter] = attrs.field(
        factory=list, converter=ListDictConverter(CommandParameter)
    )
    configuration: dict[str, ty.Any] = None
    image = None
