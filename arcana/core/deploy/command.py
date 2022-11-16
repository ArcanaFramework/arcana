from __future__ import annotations
import typing as ty
import json
import attrs
from arcana.core.utils import (
    path2varname,
    ListDictConverter,
    format_resolver,
    class_location,
)
import arcana.data.formats.common
from arcana.core.data.space import DataSpace

if ty.TYPE_CHECKING:
    from .image import ContainerImageSpec


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

    def command_arg(self, value):
        """Returns a formatted command argument that can be passed to the
        `arcana deploy run-in-image` command

        Parameters
        ----------
        value : str
            the value to pass to the input, or a variable to be replaced by the value
            at runtime (e.g. environment variable)

        Returns
        -------
        str
            an argument formatted for the `arcana deploy run-in-image` command
        """
        return (
            f"--input {self.name} {self.stored_format.class_location()} {value} "
            f"{self.task_field} {self.format.class_location()}"
        )


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

    def command_arg(self, value=None):
        """Returns a formatted command argument that can be passed to the
        `arcana deploy run-in-image` command

        Parameters
        ----------
        value : str
            the value to pass to the input, or a variable to be replaced by the value
            at runtime (e.g. environment variable)

        Returns
        -------
        str
            an argument formatted for the `arcana deploy run-in-image` command
        """
        if value is None:
            value = self.path

        return (
            f"--output {self.name} {self.stored_format.class_location()} {value} "
            f"{self.task_field} {self.format.class_location()}"
        )


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

    def command_arg(self, value):
        """Returns a formatted command argument that can be passed to the
        `arcana deploy run-in-image` command

        Parameters
        ----------
        value : str
            the value to pass to the input, or a variable to be replaced by the value
            at runtime (e.g. environment variable)

        Returns
        -------
        str
            an argument formatted for the `arcana deploy run-in-image` command
        """
        return f"--parameter {self.task_field} {value}"


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

    STORE_TYPE = "file"

    name: str
    task: str
    description: str
    long_description: str
    known_issues: str
    row_frequency: DataSpace
    inputs: list[CommandInput] = attrs.field(
        factory=list, converter=ListDictConverter(CommandInput)
    )
    outputs: list[CommandOutput] = attrs.field(
        factory=list, converter=ListDictConverter(CommandOutput)
    )
    parameters: list[CommandParameter] = attrs.field(
        factory=list, converter=ListDictConverter(CommandParameter)
    )
    configuration: dict[str, ty.Any] = None
    image: ContainerImageSpec = None

    def command_line(
        self,
        project_id,
        dataset_hierarchy=None,
        dynamic_licenses=None,
        site_licenses_dataset=None,
    ):

        data_space = type(self.row_frequency)
        if dataset_hierarchy is None:
            dataset_hierarchy = data_space.default.span()

        hierarchy_str = ",".join(str(h) for h in dataset_hierarchy)

        cmdline = (
            f"conda run --no-capture-output -n {self.image.CONDA_ENV} "  # activate conda
            f"arcana deploy run-in-image {self.STORE_TYPE}//{project_id} {self.name} {self.task} "  # run pydra task in Arcana
            f"--dataset-space {class_location(data_space)} "
            f"--dataset-hierarchy {hierarchy_str} "
            f"--row-frequency {self.row_frequency} "
            + " ".join(self.get_configuration_args())
        )

        return cmdline

    def get_configuration_args(self):

        # Set up fixed arguments used to configure the workflow at initialisation
        cmd_args = []
        for cname, cvalue in self.configuration.items():
            cvalue_json = json.dumps(cvalue)
            cmd_args.append(f"--configuration {cname} '{cvalue_json}' ")

        return " ".join(cmd_args)
