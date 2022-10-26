import attrs
import re
from copy import copy
import tempfile
import logging
import typing as ty
import shutil
import shlex
from pathlib import Path
import dataclasses
from pydra import Workflow, mark
from pydra.engine.task import DockerTask, SingularityTask, ShellCommandTask
from pydra.engine.specs import (
    ShellSpec,
    SpecInfo,
    DockerSpec,
    SingularitySpec,
    ShellOutSpec,
)
from arcana.core.data.set import Dataset
from arcana.data.spaces.medimage import Clinical
from arcana.data.stores.bids.structure import JsonEdit
from arcana.data.stores.bids.dataset import BidsDataset
from arcana.exceptions import ArcanaUsageError
from arcana.core.utils import func_task, path2varname, resolve_class

logger = logging.getLogger("arcana")


@dataclasses.dataclass
class Input:

    path: str
    format: type
    name: str = None

    @classmethod
    def fromdict(cls, dct):
        return cls(**{f.name: dct.get(f.name) for f in dataclasses.fields(cls)})

    def __post_init__(self):
        if isinstance(self.format, str):
            self.format = resolve_class(self.format, prefixes=["arcana.data.formats"])
        if self.name is None:
            self.name = path2varname(self.path)


@dataclasses.dataclass
class Output:

    name: str
    format: type
    path: str = None

    @classmethod
    def fromdict(cls, dct):
        return cls(**{f.name: dct.get(f.name) for f in dataclasses.fields(cls)})

    def __post_init__(self):
        if self.path is None:
            self.path = ""
        if isinstance(self.format, str):
            self.format = resolve_class(self.format, prefixes=["arcana.data.formats"])


logger = logging.getLogger("arcana")


def bids_app(
    name: str,
    inputs: ty.List[Input or ty.Dict[str, str]],
    outputs: ty.List[Output or ty.Dict[str, str]],
    executable: str = "",  # Use entrypoint of container,
    container_image: str = None,
    parameters: ty.Dict[str, type] = None,
    row_frequency: Clinical or str = Clinical.session,
    container_type: str = "docker",
    dataset: ty.Optional[ty.Union[str, Path, Dataset]] = None,
    app_output_dir: Path = None,
    app_work_dir: Path = None,
    json_edits: ty.List[ty.Tuple[str, str]] = None,
) -> Workflow:
    """Creates a Pydra workflow which takes file inputs, maps them to
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
    container_image : str, optional
        Name of the BIDS app image to wrap
    parameters : dict[str, type], optional
        a list of parameters of the app (i.e. CLI flags) to be exposed to the user
        mapped to their data type.
    row_frequency : Clinical, optional
        Frequency to run the app at, i.e. per-"session" or per-"dataset"
    container_type : str, optional
        The virtualisation method to run the main app task, can be one of
        'docker' or 'singularity'
    dataset : str or Dataset, optional
        The dataset to run the BIDS app on. If a string or Path is provided
        then a new BIDS dataset is created at that location with a single
        subject (sub-DEFAULT). If nothing is provided then a dataset is
        created in a temporary directory.
    app_output_dir : Path, optional
        file system path where the app outputs will be written before being
        copied to the dataset directory
    app_work_dir: Path, optional
        the directory used to run the app within. Can be used to avoid overly long path
        lengths that can occur running nested workflows (e.g. fmriprep)
    json_edits: ty.List[ty.Tuple[str, str]]
        Ad-hoc edits to JSON side-cars that are fixed during the configuration
        of the app, i.e. not passed as an input. Input JSON edits are appended
        to these fixed

    Returns
    -------
    pydra.Workflow
        A Pydra workflow
    """
    if parameters is None:
        parameters = {}
    if app_output_dir is None:
        app_output_dir = Path(tempfile.mkdtemp())
    else:
        app_output_dir = Path(app_output_dir)
        app_output_dir.mkdir(parents=True, exist_ok=True)
    if json_edits is None:
        json_edits = []

    if isinstance(row_frequency, str):
        row_frequency = Clinical[row_frequency]

    # Create BIDS dataset to hold translated data
    if dataset is None:
        dataset = Path(tempfile.mkdtemp()) / "arcana_bids_dataset"
    if not isinstance(dataset, Dataset):
        dataset = BidsDataset.create(
            path=dataset, name=name + "_dataset", subject_ids=[DEFAULT_BIDS_ID]
        )

    # Convert from JSON format inputs/outputs to tuples with resolved data formats
    inputs = [Input.fromdict(i) if not isinstance(i, Input) else i for i in inputs]
    outputs = [Output.fromdict(o) if not isinstance(o, Output) else o for o in outputs]

    # Ensure output paths all start with 'derivatives
    input_names = [i.name for i in inputs]
    output_names = [o.name for o in outputs]

    input_spec = set(["id", "flags", "json_edits"] + input_names + list(parameters))

    wf = Workflow(name=name, input_spec=list(input_spec))

    # Check id startswith 'sub-' as per BIDS
    wf.add(bidsify_id(name="bidsify_id", id=wf.lzin.id))

    # Can't use a decorated function as we need to allow for dynamic
    # arguments
    wf.add(
        func_task(
            to_bids,
            in_fields=(
                [
                    ("row_frequency", Clinical),
                    ("inputs", ty.List[ty.Tuple[str, type, str]]),
                    ("dataset", Dataset or str),
                    ("id", str),
                    ("json_edits", str),
                    ("fixed_json_edits", ty.List[ty.Tuple[str, str]]),
                ]
                + [(i, ty.Union[str, Path]) for i in input_names]
            ),
            out_fields=[
                ("dataset", BidsDataset),
                ("completed", bool),
            ],
            name="to_bids",
            row_frequency=row_frequency,
            inputs=inputs,
            dataset=dataset,
            id=wf.bidsify_id.lzout.out,
            json_edits=wf.lzin.json_edits,
            fixed_json_edits=json_edits,
            **{i: getattr(wf.lzin, i) for i in input_names},
        )
    )

    # dataset_path=Path(dataset.id),
    # output_dir=app_output_dir,
    # parameters={p: type(p) for p in parameters},
    # id=wf.bidsify_id.lzout.no_prefix,

    input_fields = copy(BIDS_APP_INPUTS)

    for param in parameters.items():
        argstr = f"--{param}"
        if type(param) is not bool:
            argstr += " %s"
        input_fields.append(
            (
                param,
                type(param),
                {"help_string": f"Optional parameter {param}", "argstr": argstr},
            )
        )

    kwargs = {p: getattr(wf.lzin, p) for p in parameters}

    # If 'image' is None, don't use any virtualisation (i.e. assume we are running from "inside" the
    # container or extension of it)
    if container_image is None:
        task_cls = ShellCommandTask
        base_spec_cls = ShellSpec
        kwargs["executable"] = executable
        app_output_path = str(app_output_dir)
        app_dataset_path = Path(dataset.id)
    else:

        # Set input and output directories to "internal" paths within the
        # container
        app_dataset_path = CONTAINER_DATASET_PATH
        app_output_path = CONTAINER_DERIV_PATH
        kwargs["image"] = container_image

        if container_type == "docker":
            task_cls = DockerTask
            base_spec_cls = DockerSpec
        elif container_type == "singularity":
            task_cls = SingularityTask
            base_spec_cls = SingularitySpec
        else:
            raise ArcanaUsageError(
                f"Unrecognised container type {container_type} "
                "(can be docker or singularity)"
            )

    if row_frequency == Clinical.session:
        analysis_level = "participant"
        kwargs["participant_label"] = wf.bidsify_id.lzout.no_prefix
    else:
        analysis_level = "group"

    main_task = task_cls(
        name="bids_app",
        input_spec=SpecInfo(name="Input", fields=input_fields, bases=(base_spec_cls,)),
        output_spec=SpecInfo(
            name="Output", fields=BIDS_APP_OUTPUTS, bases=(ShellOutSpec,)
        ),
        dataset_path=str(app_dataset_path),
        output_path=str(app_output_path),
        work_dir=app_work_dir,
        analysis_level=analysis_level,
        flags=wf.lzin.flags,
        setup_completed=wf.to_bids.lzout.completed,
        **kwargs,
    )

    if container_image is not None:
        main_task.bindings = {
            dataset.id: (CONTAINER_DATASET_PATH, "ro"),
            app_output_dir: (CONTAINER_DERIV_PATH, "rw"),
        }

    wf.add(main_task)

    wf.add(
        func_task(
            extract_bids,
            in_fields=[
                ("dataset", Dataset),
                ("row_frequency", Clinical),
                ("app_name", str),
                ("output_dir", Path),
                ("outputs", ty.List[ty.Tuple[str, type, str]]),
                ("id", str),
                ("app_completed", bool),
            ],
            out_fields=[(o, str) for o in output_names],
            name="extract_bids",
            app_name=name,
            dataset=wf.to_bids.lzout.dataset,  # We pass dataset object modified by to_bids rather than initial one passed to the bids_app method
            output_dir=app_output_dir,
            row_frequency=row_frequency,
            outputs=outputs,
            id=wf.bidsify_id.lzout.out,
            app_completed=wf.bids_app.lzout.completed,
        )
    )

    for output_name in output_names:
        wf.set_output((output_name, getattr(wf.extract_bids.lzout, output_name)))

    return wf


# For running
CONTAINER_DERIV_PATH = "/arcana_bids_outputs"
CONTAINER_DATASET_PATH = "/arcana_bids_dataset"

DEFAULT_BIDS_ID = "sub-DEFAULT"


@mark.task
@mark.annotate({"return": {"out": str, "no_prefix": str}})
def bidsify_id(id):
    if id == attrs.NOTHING:
        id = DEFAULT_BIDS_ID
    else:
        id = re.sub(r"[^a-zA-Z0-9]", "", id)
        if not id.startswith("sub-"):
            id = "sub-" + id
    return id, id[len("sub-") :]


def to_bids(
    row_frequency, inputs, dataset, id, json_edits, fixed_json_edits, **input_values
):
    """Takes generic inptus and stores them within a BIDS dataset"""
    # Update the Bids store with the JSON edits requested by the user
    je_args = shlex.split(json_edits) if json_edits else []
    dataset.store.json_edits = JsonEdit.attr_converter(
        fixed_json_edits + list(zip(je_args[::2], je_args[1::2]))
    )
    for inpt in inputs:
        dataset.add_sink(inpt.name, inpt.format, path=inpt.path)
    row = dataset.row(row_frequency, id)
    with dataset.store:
        for inpt_name, inpt_value in input_values.items():
            if inpt_value is attrs.NOTHING:
                logger.warning("No input provided for '%s' input", inpt_name)
                continue
            row_item = row[inpt_name]
            row_item.put(inpt_value)  # Store value/path in store
    return (dataset, True)


def extract_bids(
    dataset: Dataset,
    row_frequency: Clinical,
    app_name: str,
    output_dir: Path,
    outputs: ty.List[ty.Tuple[str, type]],
    id: str,
    app_completed: bool,
):
    """Selects the items from the dataset corresponding to the input
    sources and retrieves them from the store to a cache on
    the host

    Parameters
    ----------
    dataset : Dataset
    row_frequency : Clinical
    output_dir : Path
    outputs : ty.List[ty.Tuple[str, type]]
    id : str
        id of the row to be processed
    app_completed : bool
        a dummy field produced by the main BIDS app task on output, to ensure
        'extract_bids' is run after the app has completed.
    """
    # Copy output dir into BIDS dataset
    shutil.copytree(output_dir, Path(dataset.id) / "derivatives" / app_name / id)
    output_paths = []
    row = dataset.row(row_frequency, id)
    for output in outputs:
        dataset.add_sink(
            output.name,
            output.format,
            path="derivatives/" + app_name + "/" + output.path,
        )
    with dataset.store:
        for output in outputs:
            item = row[output.name]
            item.get()  # download to host if required
            output_paths.append(item.value)
    return tuple(output_paths) if len(outputs) > 1 else output_paths[0]


BIDS_APP_INPUTS = [
    (
        "dataset_path",
        str,
        {
            "help_string": "Path to BIDS dataset in the container",
            "position": 1,
            "mandatory": True,
            "argstr": "'{dataset_path}'",
        },
    ),
    (
        "output_path",
        str,
        {
            "help_string": "Directory where outputs will be written in the container",
            "position": 2,
            "argstr": "'{output_path}'",
        },
    ),
    (
        "analysis_level",
        str,
        {
            "help_string": "The analysis level the app will be run at",
            "position": 3,
            "argstr": "",
        },
    ),
    (
        "participant_label",
        ty.List[str],
        {
            "help_string": "The IDs to include in the analysis",
            "argstr": "--participant-label ",
            "position": 4,
        },
    ),
    (
        "flags",
        str,
        {
            "help_string": "Additional flags to pass to the app",
            "argstr": "",
            "position": -1,
        },
    ),
    (
        "work_dir",
        str,
        {
            "help_string": "Directory where the nipype temporary working directories will be stored",
            "argstr": "--work-dir '{work_dir}'",
        },
    ),
    (
        "setup_completed",
        bool,
        {
            "help_string": "Dummy field to ensure that the BIDS dataset construction completes first"
        },
    ),
]

BIDS_APP_OUTPUTS = [
    (
        "completed",
        bool,
        {
            "help_string": "a simple flag to indicate app has completed",
            "callable": lambda: True,
        },
    )
]
