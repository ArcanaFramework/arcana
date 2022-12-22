from __future__ import annotations
import typing as ty
import attrs
from pydra import ShellCommandTask
import pydra.engine.specs
from pydra.engine.specs import SpecInfo, ShellSpec, ShellOutSpec
from arcana.core.utils.serialize import ClassResolver, ObjectListConverter
from arcana.core.data.type.base import FileGroup, DataType


@attrs.define(kw_only=True)
class ShellCmdField:
    name: str
    datatype: type = attrs.field(
        converter=ClassResolver(DataType, alternative_types=[bool, str, float, int])
    )
    help_string: str = ""

    def attrs_metadata(self, skip_fields=None):
        if skip_fields is None:
            skip_fields = []
        skip_fields += ["name", "datatype"]
        metadata = {
            n: v
            for n, v in attrs.asdict(self).items()
            if n not in skip_fields and v is not None
        }
        if issubclass(self.datatype, FileGroup) and "argstr" not in metadata:
            metadata["argstr"] = ""
        return metadata

    @property
    def attrs_type(self):
        if issubclass(self.datatype, FileGroup):
            if self.datatype.is_dir:
                tp = pydra.engine.specs.Directory
            else:
                tp = pydra.engine.specs.File
        elif self.datatype.__module__ == "builtins":
            tp = self.datatype
        else:
            raise ValueError(f"Unsupported shell command input type {self.datatype}")
        return tp


@attrs.define(kw_only=True)
class ShellCmdInput(ShellCmdField):
    """Specifies an input field to the shell command

    Parameters
    ----------
    name : str
        the name of the input field
    datatype : type
        the type of the input field
    mandatory (bool, default: False):
        If True user has to provide a value for the field.
    sep (str):
        A separator if a list is provided as a value.
    argstr (str):
        A flag or string that is used in the command before the value, e.g. -v or
        -v {inp_field}, but it could be and empty string, “”. If … are used, e.g. -v…,
        the flag is used before every element if a list is provided as a value. If no
        argstr is used the field is not part of the command.
    position (int):
        Position of the field in the command, could be nonnegative or negative integer.
        If nothing is provided the field will be inserted between all fields with
        nonnegative positions and fields with negative positions.
    allowed_values (list):
        List of allowed values for the field.
    requires (list):
        List of field names that are required together with the field.
    xor (list):
        List of field names that are mutually exclusive with the field.
    copyfile (bool, default: False):
        If True, a hard link is created for the input file in the output directory. If
        hard link not possible, the file is copied to the output directory.
    container_path (bool, default: False, only for ContainerTask):
        If True a path will be consider as a path inside the container (and not as a
        local path).
    output_file_template (str):
        If provided, the field is treated also as an output field and it is added to the
        output spec. The template can use other fields, e.g. {file1}. Used in order to
        create an output specification.
    output_field_name (str, used together with output_file_template)
        If provided the field is added to the output spec with changed name. Used in
        order to create an output specification.
    keep_extension (bool, default: True):
        A flag that specifies if the file extension should be removed from the field
        value. Used in order to create an output specification.
    readonly (bool, default: False):
        If True the input field can’t be provided by the user but it aggregates other
        input fields (for example the fields with argstr: -o {fldA} {fldB}).
    formatter (function)
        If provided the argstr of the field is created using the function. This function
        can for example be used to combine several inputs into one command argument.
        The function can take field (this input field will be passed to the function),
        inputs (entire inputs will be passed) or any input field name (a specific input
        field will be sent).
    """

    sep: str = None
    argstr: str = None
    position: int = None
    allowed_values: list = None
    requires: list = None
    xor: list = None
    copyfile: bool = None
    mandatory: bool = None
    readonly: bool = None
    formatter: ty.Callable = attrs.field(
        default=None,
        converter=ClassResolver(allow_none=True),
    )


@attrs.define(kw_only=True)
class ShellCmdOutput(ShellCmdField):
    """Specifies an output field from the shell command

    Parameters
    ----------
    name : str
        the name of the input field
    datatype : type
        the type of the input field
    mandatory : bool, default: False
        If True the output file has to exist, otherwise an error will be raised.
    output_file_template : str
        If provided the output file name (or list of file names) is created using the
        template. The template can use other fields, e.g. {file1}. The same as in input_spec.
    output_field_name : str
        If provided the field is added to the output spec with changed name. The same as in
        input_spec.
    keep_extension : bool, default: True
        A flag that specifies if the file extension should be removed from the field value.
        The same as in input_spec.
    requires : list
        List of field names that are required to create a specific output. The fields do not
        have to be a part of the output_file_template and if any field from the list is not
        provided in the input, a NOTHING is returned for the specific output. This has a
        different meaning than the requires form the input_spec.
    callable : function
        If provided the output file name (or list of file names) is created using the
        function. The function can take field (the specific output field will be passed
        to the function), output_dir (task output_dir will be used), stdout, stderr
        (stdout and stderr of the task will be sent) inputs (entire inputs will be
        passed) or any input field name (a specific input field will be sent).
    """

    argstr: bool = attrs.field(default=None, metadata={"input": True})
    mandatory: bool = None
    position: int = attrs.field(default=None, metadata={"input": True})
    output_file_template: str = None
    output_field_name: str = None
    keep_extension: bool = attrs.field(default=None, metadata={"input": True})
    requires: list = None
    formatter: ty.Callable = attrs.field(
        default=None, converter=ClassResolver(allow_none=True), metadata={"input": True}
    )
    callable: ty.Callable = attrs.field(
        default=None, converter=ClassResolver(allow_none=True)
    )

    @property
    def input_required(self):
        input_only_values = [getattr(self, f) for f in self.input_only_fields]
        return any(v is not None for v in input_only_values)

    @property
    def input_only_fields(self):
        return [f.name for f in attrs.fields(type(self)) if f.metadata.get("input")]

    @property
    def attrs_type(self):
        tp = super().attrs_type
        # FIXME: this shouldn't be necessary. Pydra should be smart enough that this is
        # an output file and shouldn't need to exist
        if tp in (pydra.engine.specs.File, pydra.engine.specs.Directory):
            tp = str
        return tp


def shell_cmd(
    name: str,
    inputs: list[ty.Union[ShellCmdInput, dict[str, str]]],
    outputs: list[ty.Union[ShellCmdOutput, dict[str, str]]],
    executable: str = "",  # Use entrypoint of container,
    parameters: list[ty.Union[ShellCmdInput, dict[str, str]]] = None,
):
    """Creates a Pydra shell command task which takes file inputs and runs it on the
    provided inputs, outputs and parameters

    Parameters
    ----------
    name : str
        Name of the workflow/BIDS app. Will be used to name the 'derivatives'
        sub-directory where the app outputs are stored
    inputs : list[tuple[str, type] or dict[str, str]]
        The inputs to be inserted into the BIDS dataset. Should be a list of tuples
        consisting of the the path the file/directory should be stored within a BIDS subject/session,
        e.g. anat/T1w, func/bold, and the DataFormat class it should be stored in, e.g.
        arcana.bids.data.NiftiGzX.
    outputs : list[tuple[str, type] or dict[str, str]]
        The outputs to be extracted from the derivatives directory. Should be a list of tuples
        consisting of the the path the file/directory is saved by the app within a BIDS subject/session,
        e.g. freesurfer/recon-all, and the DataFormat class it is stored in, e.g.
        arcana.dirtree.data.Directory.
    executable : str, optional
        Name of the executable within the image to run (i.e. the entrypoint of the image).
        Required when extending the base image and launching Arcana within it. Defaults to
        empty string, i.e. the entrypoint of the BIDS app container image
    parameters : dict[str, type], optional
        a list of parameters of the app (i.e. CLI flags) to be exposed to the user
        mapped to their data type.
    row_frequency : Clinical, optional
        Frequency to run the app at, i.e. per-"session" or per-"dataset"

    Returns
    -------
    pydra.ShellCommmandTask
        A Pydra shell command task that can be deployed using the deployment framework
    """
    inputs = ObjectListConverter(ShellCmdInput)(inputs)
    outputs = ObjectListConverter(ShellCmdOutput)(outputs)
    parameters = ObjectListConverter(ShellCmdInput)(parameters)

    input_fields = []
    for inpt in inputs + parameters:
        input_fields.append(
            (
                inpt.name,
                inpt.attrs_type,
                inpt.attrs_metadata(),
            )
        )

    output_fields = []
    for outpt in outputs:
        if outpt.input_required:
            input_fields.append(
                (
                    outpt.name,
                    outpt.attrs_type,
                    outpt.attrs_metadata(),
                )
            )
        output_fields.append(
            (
                outpt.name,
                outpt.attrs_type,
                outpt.attrs_metadata(skip_fields=outpt.input_only_fields),
            )
        )

    return ShellCommandTask(
        name=name,
        executable=executable,
        input_spec=SpecInfo(name="Input", fields=input_fields, bases=(ShellSpec,)),
        output_spec=SpecInfo(
            name="Output", fields=output_fields, bases=(ShellOutSpec,)
        ),
    )
