import typing as ty
import attrs
from arcana.core.utils import resolve_class
import arcana.data.formats.common


@attrs.define
class CommandInput:
    name: str  # How the input will be referred to in the XNAT dialog, defaults to the pydra_field name
    path: str = None
    format: type = arcana.data.formats.common.File
    pydra_field: str = None  # Must match the name of the Pydra task input
    row_frequency: Clinical = Clinical.session
    description: str = ""  # description of the input
    stored_format: type = None  # the format the input is stored in the data store in

    def __post_init__(self):
        if self.path is None:
            self.path = self.name
        if self.pydra_field is None:
            self.pydra_field = self.name
        if self.stored_format is None:
            self.stored_format = self.format
        if isinstance(self.format, str):
            self.format = resolve_class(self.format, prefixes=["arcana.data.formats"])
        if isinstance(self.stored_format, str):
            self.stored_format = resolve_class(
                self.stored_format, prefixes=["arcana.data.formats"]
            )


@attrs.define
class CommandOutput:
    name: str
    path: str = None  # The path the output is stored at in XNAT
    format: type = arcana.data.formats.common.File
    pydra_field: str = (
        None  # Must match the name of the Pydra task output, defaults to the path
    )
    stored_format: type = (
        None  # the format the output is to be stored in the data store in
    )

    def __post_init__(self):
        if self.path is None:
            self.path = self.name
        if self.pydra_field is None:
            self.pydra_field = self.name
        if self.stored_format is None:
            self.stored_format = self.format
        if isinstance(self.format, str):
            self.format = resolve_class(self.format, prefixes=["arcana.data.formats"])
        if isinstance(self.stored_format, str):
            self.stored_format = resolve_class(
                self.stored_format, prefixes=["arcana.data.formats"]
            )


@attrs.define
class CommandParameter:
    name: str  # How the input will be referred to in the XNAT dialog, defaults to pydra_field name
    type: type = str
    pydra_field: str = None  # Name of parameter to expose in Pydra task
    required: bool = False
    default: str = None
    description: str = ""  # description of the parameter

    def __post_init__(self):
        if self.pydra_field is None:
            self.pydra_field = path2varname(self.name)


@attrs.define
class ContainerCommandSpec:

    name: str
    task: str
    description: str

    inputs: ty.List[CommandInput]
    outputs: ty.List[CommandOutput]
    parameters: ty.List[CommandParameter] = attrs.field(factory=list)
    configuration = None
    row_frequency: DataSpace = "session"

    def generate(
        self,
        wrapper_version: str,
        image_tag: str,
        version: str,
        info_url: str,
        pkg_version: str,
        site_licenses_dataset: ty.Tuple[str, str, str] = None,
    ):
        pass
