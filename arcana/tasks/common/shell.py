import typing as ty
import dataclasses
from pathlib import Path
from pydra import ShellCommandTask
from pydra.engine.specs import SpecInfo, ShellSpec, ShellOutSpec
from arcana.core.utils import resolve_class
from arcana.core.data.format import FileGroup


@dataclasses.dataclass
class Input:

    name: str
    format: type
    argstr: str = ""
    description: str = ""

    @classmethod
    def fromdict(cls, dct):
        field_names = [f.name for f in dataclasses.fields(cls)]
        return cls(**{k: v for k, v in dct.items() if k in field_names})

    def __post_init__(self):
        if isinstance(self.format, str):
            self.format = resolve_class(self.format, prefixes=["arcana.data.formats"])


@dataclasses.dataclass
class Output:

    name: str
    format: type
    requires: ty.List[ty.Tuple[str, ty.Any]] = None
    argstr: str = ""
    position: int = None
    output_file_template: str = None
    description: str = ""

    @classmethod
    def fromdict(cls, dct):
        field_names = [f.name for f in dataclasses.fields(cls)]
        return cls(**{k: v for k, v in dct.items() if k in field_names})

    def __post_init__(self):
        if isinstance(self.format, str):
            self.format = resolve_class(self.format, prefixes=["arcana.data.formats"])


@dataclasses.dataclass
class Parameter:

    name: str
    type: type = str
    argstr: str = ""
    position: int = None
    default = None
    description: str = ""

    @classmethod
    def fromdict(cls, dct):
        field_names = [f.name for f in dataclasses.fields(cls)]
        return cls(**{k: v for k, v in dct.items() if k in field_names})

    def __post_init__(self):
        if isinstance(self.type, str):
            self.type = resolve_class(self.type)


def shell_cmd(
    name: str,
    inputs: ty.List[Input or ty.Dict[str, str]],
    outputs: ty.List[Output or ty.Dict[str, str]],
    executable: str = "",  # Use entrypoint of container,
    parameters: ty.List[Parameter or ty.Dict[str, type]] = None,
):
    """Creates a Pydra shell command task which takes file inputs, maps them to
    a BIDS dataset, executes a BIDS app, and then extracts the
    the derivatives that were stored back in the BIDS dataset by the app

    Parameters
    ----------
    name : str
        Name of the workflow/BIDS app. Will be used to name the 'derivatives'
        sub-directory where the app outputs are stored
    inputs : list[tuple[str, type] or dict[str, str]]
        The inputs to be inserted into the BIDS dataset. Should be a list of tuples
        consisting of the the path the file/directory should be stored within a BIDS subject/session,
        e.g. anat/T1w, func/bold, and the DataFormat class it should be stored in, e.g.
        arcana.data.formats.bids.NiftiGzX.
    outputs : list[tuple[str, type] or dict[str, str]]
        The outputs to be extracted from the derivatives directory. Should be a list of tuples
        consisting of the the path the file/directory is saved by the app within a BIDS subject/session,
        e.g. freesurfer/recon-all, and the DataFormat class it is stored in, e.g.
        arcana.data.formats.common.Directory.
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
    inputs = [Input.fromdict(i) if not isinstance(i, Input) else i for i in inputs]
    outputs = [Output.fromdict(o) if not isinstance(o, Output) else o for o in outputs]
    parameters = [
        Parameter.fromdict(p) if not isinstance(p, Parameter) else p for p in parameters
    ]

    input_fields = []
    positions = set()
    for param in parameters:
        metadata = {"help_string": param.description, "argstr": param.argstr}
        if param.position is not None:
            metadata["position"] = param.position
            positions.add(param.position)
        input_fields.append((param.name, param.type, metadata))

    output_fields = []
    for outpt in outputs:
        metadata = {"help_string": param.description}
        outpt_type = (
            Path or str if issubclass(outpt.format, FileGroup) else outpt.format
        )
        if outpt.output_file_template is not None:
            metadata["output_file_template"] = outpt.output_file_template
        if outpt.position is not None:
            inpt_metadata = {"argstr": outpt.argstr}
            inpt_metadata.update(metadata)
            input_fields.append((outpt.name, outpt_type, inpt_metadata))
            if outpt.output_file_template is None:
                metadata["output_file_template"] = "{{{outpt.name}}}"
        if outpt.requires is not None:
            metadata["requires"] = outpt.requires
        output_fields.append(
            (
                outpt.name,
                outpt_type,
                metadata,
            )
        )

    pos = len(parameters)
    for inpt in inputs:
        # Set the position as the next available around explicit output and parameter
        # positions
        if pos in positions:
            pos += 1
        metadata = {
            "help_string": inpt.description,
            "position": pos,
            "argstr": param.argstr,
        }
        positions.add(pos)
        if inpt.argstr is not None:
            metadata["argstr"] = inpt.argstr
        input_fields.append(
            (
                inpt.name,
                Path or str if issubclass(inpt.format, FileGroup) else inpt.format,
                metadata,
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
