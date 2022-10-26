import sys
import re
import typing as ty
from pathlib import Path
import tempfile
import inspect
import json
from copy import copy
from dataclasses import dataclass
from neurodocker.reproenv import DockerRenderer
from arcana import __version__
from arcana.core.data.format import FileGroup
import arcana.data.formats.common
from arcana.data.spaces.medimage import Clinical
from arcana.data.stores.medimage import XnatViaCS
import arcana.core.data.row
from arcana.core.deploy.build import construct_dockerfile, dockerfile_build, CONDA_ENV
from arcana.core.deploy.utils import DOCKER_HUB
from arcana.core.utils import resolve_class, class_location, path2varname
from arcana.core.data.store import DataStore
from arcana.exceptions import ArcanaUsageError


def path2xnatname(path):
    return re.sub(r"[^a-zA-Z0-9_]+", "_", path)


IN_DOCKER_ARCANA_HOME_DIR = "/arcana-home"


def build_xnat_cs_image(
    image_tag: str,
    commands: ty.List[ty.Dict[str, ty.Any]],
    authors: ty.List[ty.Dict[str, str]],
    info_url: str,
    docker_registry: str = DOCKER_HUB,
    build_dir: Path = None,
    test_config: bool = False,
    generate_only: bool = False,
    pkg_version: str = None,  # Ignored here, just included to allow specs to be passed directly as kwargs
    wrapper_version: str = None,  # ditto
    **kwargs,
):
    """Creates a Docker image containing one or more XNAT commands ready
    to be installed in XNAT's container service plugin

    Parameters
    ----------
    image_tag : str
        Tag to name the built Docker image with
    commands: dict[str, Any]
        List of command specifications (in dicts) to be installed on the
        image, see `generate_xnat_command` for valid args (dictionary keys).
    authors : list[dict[str, str]]
        Names and emails of the maintainers of the wrapper pipeline
    info_url : str
        The URL of the package website explaining the analysis software
        and what it does
    docker_registry : str
        The Docker registry the image will be uploaded to
    wrapper_version : str
        The version of the wrapper specific to the pkg version. It will be
        appended to the package version, e.g. 0.16.2 -> 0.16.2--1
    build_dir : Path
        the directory to build the docker image within, i.e. where to write
        Dockerfile and supporting files to be copied within the image
    test_config : bool
        whether to create the container so that it will work with the test
        XNAT configuration (i.e. hard-coding the XNAT server IP)
    pkg_version : str, optional
        ignored by function, just included to allow specs to be passed directly
        as kwargs **specs
    wrapper_version : str, optional
        ignored by function, just included to allow specs to be passed directly
        as kwargs as **specs
    **kwargs:
        Passed on to `construct_dockerfile` method

    Returns
    -------
    DockerRenderer
        the Neurodocker renderer
    Path
        path to build directory
    """
    # Save a snapshot of the arguments to this function in a dict to save
    # within the built image
    ARGS_TO_IGNORE = [
        "kwargs",
        "build_dir",
        "generate_only",
        "license_dir",
        "pkg_version",
        "wrapper_version",
    ]
    spec = {k: v for k, v in kwargs.items() if k not in ARGS_TO_IGNORE}
    for argname in inspect.signature(build_xnat_cs_image).parameters:
        if argname not in ARGS_TO_IGNORE:
            spec[argname] = locals()[argname]
    spec["arcana_version"] = __version__

    if build_dir is None:
        build_dir = tempfile.mkdtemp()
    build_dir = Path(build_dir)

    xnat_commands = []
    for cmd_spec in commands:

        if "info_url" not in cmd_spec:
            cmd_spec = copy(cmd_spec)
            cmd_spec["info_url"] = info_url

        xnat_cmd = generate_xnat_cs_command(
            image_tag=image_tag,
            registry=docker_registry,
            pkg_version=pkg_version,
            wrapper_version=wrapper_version,
            **cmd_spec,
        )

        xnat_commands.append(xnat_cmd)

    # Convert XNAT command label into string that can by placed inside the
    # Docker label
    command_label = json.dumps(xnat_commands).replace("$", r"\$")

    dockerfile = construct_dockerfile(
        build_dir,
        labels={"org.nrg.commands": command_label, "maintainer": authors[0]["email"]},
        spec=spec,
        **kwargs,
    )

    # Copy the generated XNAT commands inside the container for ease of reference
    copy_command_ref(dockerfile, xnat_commands, build_dir)

    save_store_config(dockerfile, build_dir, test_config=test_config)

    if not generate_only:
        dockerfile_build(dockerfile, build_dir, image_tag)

    return dockerfile, build_dir


def generate_xnat_cs_command(
    name: str,
    pydra_task: str,
    image_tag: str,
    inputs,
    outputs,
    description: str,
    version: str,
    info_url: str,
    pkg_version: str,
    wrapper_version: str,
    parameters=None,
    configuration=None,
    row_frequency="session",
    registry=DOCKER_HUB,
    long_description: str = None,  # Ignored here, just part of the specs for the docs
    known_issues: str = None,  # ditto
):
    """Constructs the XNAT CS "command" JSON config, which specifies how XNAT
    should handle the containerised pipeline

    Parameters
    ----------
    name : str
        Name of the container service pipeline
    pydra_task
        The module path and name (separated by ':') to the Pydra task, or function
        that returns a Pydra Workflow, to execute, e.g. arcana.tasks.bids:bids_app
    image_tag : str
        Name + version of the Docker image to be created
    inputs : ty.List[ty.Union[InputArg, tuple]]
        Inputs to be provided to the container (pydra_field, format, name, row_frequency).
        'pydra_field' and 'format' will be passed to "inputs" arg of the Dataset.pipeline() method,
        'row_frequency' to the Dataset.add_source() method and 'name' is displayed in the XNAT
        UI
    outputs : ty.List[ty.Union[OutputArg, tuple]]
        Outputs to extract from the container (pydra_field, format, output_path).
        'pydra_field' and 'format' will be passed as "outputs" arg the Dataset.pipeline() method,
        'output_path' determines the path the output will saved in the XNAT data tree.
    description : str
        User-facing description of the pipeline
    version : str
        Version string for the wrapped pipeline
    info_url : str
        URI explaining in detail what the pipeline does
    pkg_version : str
        Version of the package to be wrapped by the Docker image (used in the command
        description)
    wrapper_version : str
        Version of the wrapper script used to generate the Docker image for the given
        package version (used in the command description)
    parameters : ty.List[str]
        Parameters to be exposed in the CS command
    row_frequency : str
        Frequency of the pipeline to generate (can be either 'dataset' or 'session' currently)
    registry : str
        URI of the Docker registry to upload the image to
    configuration : dict[str, Any]
        Fixed arguments passed to the workflow at initialisation. Can be used to specify
        the input fields of the workflow/task
    long_description : str
        A long description of the pipeline, used in documentation and ignored
        here. Only included in the signature so that an error isn't thrown when
        it is encountered.
    known_issues : str
        Any known issues with the pipeline. Only included in the signature so that an
        error isn't thrown when it is encountered.

    Returns
    -------
    dict
        JSON that can be used

    Raises
    ------
    ArcanaUsageError
        [description]
    """
    if parameters is None:
        parameters = []
    if configuration is None:
        configuration = {}
    if isinstance(row_frequency, str):
        row_frequency = Clinical[row_frequency]
    if row_frequency not in VALID_FREQUENCIES:
        raise ArcanaUsageError(
            f"'{row_frequency}'' is not a valid option ('"
            + "', '".join(VALID_FREQUENCIES)
            + "')"
        )

    # Convert tuples to appropriate dataclasses for inputs, outputs and parameters
    def parse_specs(args, klass):
        parsed_args = []
        for arg in args:
            if isinstance(arg, klass):
                parsed = arg
            elif isinstance(arg, dict):
                parsed = klass(**arg)
            else:
                parsed = klass(*arg)
            if parsed.format is not arcana.core.data.row.DataRow:
                parsed_args.append(parsed)
        return parsed_args

    inputs = parse_specs(inputs, InputArg)
    outputs = parse_specs(outputs, OutputArg)

    parsed_params = []
    for param in parameters:
        if isinstance(param, ParamArg):
            parsed = param
        elif isinstance(param, str):
            parsed = ParamArg(pydra_field=param)
        else:
            parsed = ParamArg(**param)
        parsed_params.append(parsed)
    parameters = parsed_params

    # JSON to define all inputs and parameters to the pipelines
    inputs_json = []

    # Add task inputs to inputs JSON specification
    input_args = []
    for inpt in inputs:
        replacement_key = f"[{inpt.pydra_field.upper()}_INPUT]"
        if issubclass(inpt.format, FileGroup):
            desc = f"Match resource [SCAN_TYPE]: {inpt.description} "
            input_type = "string"
        else:
            desc = (
                f"Match field ({inpt.format.dtype}) [FIELD_NAME]: {inpt.description} "
            )
            input_type = COMMAND_INPUT_TYPES.get(inpt.format, "string")
        inputs_json.append(
            {
                "name": path2xnatname(inpt.name),
                "description": desc,
                "type": input_type,
                "default-value": inpt.path,
                "required": False,
                "user-settable": True,
                "replacement-key": replacement_key,
            }
        )
        input_args.append(
            f"--input {inpt.name} {inpt.stored_format.location()} '{replacement_key}' {inpt.pydra_field} {inpt.format.location()} "
        )

    # Add parameters as additional inputs to inputs JSON specification
    param_args = []
    for param in parameters:
        desc = f"Parameter ({param.type}): " + param.description

        replacement_key = f"[{param.pydra_field.upper()}_PARAM]"

        inputs_json.append(
            {
                "name": param.name,
                "description": desc,
                "type": COMMAND_INPUT_TYPES.get(param.type, "string"),
                "default-value": (param.default if param.default else ""),
                "required": param.required,
                "user-settable": True,
                "replacement-key": replacement_key,
            }
        )
        param_args.append(f"--parameter {param.pydra_field} '{replacement_key}' ")

    # Set up output handlers and arguments
    outputs_json = []
    output_handlers = []
    output_args = []
    for output in outputs:
        label = output.path.split("/")[0]
        out_fname = output.path + ("." + output.format.ext if output.format.ext else "")
        # Set the path to the
        outputs_json.append(
            {
                "name": output.name,
                "description": f"{output.pydra_field} ({output.format.location()})",
                "required": True,
                "mount": "out",
                "path": out_fname,
                "glob": None,
            }
        )
        output_handlers.append(
            {
                "name": f"{output.name}-resource",
                "accepts-command-output": output.name,
                "via-wrapup-command": None,
                "as-a-child-of": "SESSION",
                "type": "Resource",
                "label": label,
                "format": output.format.class_name(),
            }
        )
        output_args.append(
            f"--output {output.name} {output.stored_format.location()} '{output.path}' {output.pydra_field} {output.format.location()} "
        )

    # Set up fixed arguments used to configure the workflow at initialisation
    config_args = []
    for cname, cvalue in configuration.items():
        cvalue_json = json.dumps(cvalue)  # .replace('"', '\\"')
        config_args.append(f"--configuration {cname} '{cvalue_json}' ")

    # Add input for dataset name
    FLAGS_KEY = "#ARCANA_FLAGS#"
    inputs_json.append(
        {
            "name": "Arcana_flags",
            "description": "Flags passed to `run-arcana-pipeline` command",
            "type": "string",
            "default-value": (
                "--plugin serial "
                "--work /wl "  # NB: work dir moved inside container due to file-locking issue on some mounted volumes (see https://github.com/tox-dev/py-filelock/issues/147)
                "--dataset-name default "
                "--loglevel info "
                f"--export-work {XnatViaCS.WORK_MOUNT}"
            ),
            "required": False,
            "user-settable": True,
            "replacement-key": FLAGS_KEY,
        }
    )

    input_args_str = " ".join(input_args)
    output_args_str = " ".join(output_args)
    param_args_str = " ".join(param_args)
    config_args_str = " ".join(config_args)

    cmdline = (
        f"conda run --no-capture-output -n {CONDA_ENV} "  # activate conda
        f"run-arcana-pipeline xnat-cs//[PROJECT_ID] {name} {pydra_task} "  # run pydra task in Arcana
        + input_args_str
        + output_args_str
        + param_args_str
        + config_args_str
        + FLAGS_KEY
        + " "
        + f"--dataset-space medimage:Clinical "
        f"--dataset-hierarchy subject,session "
        "--single-row [SUBJECT_LABEL],[SESSION_LABEL] "
        f"--row-frequency {row_frequency} "
    )  # pass XNAT API details
    # TODO: add option for whether to overwrite existing pipeline

    # Create Project input that can be passed to the command line, which will
    # be populated by inputs derived from the XNAT object passed to the pipeline
    inputs_json.append(
        {
            "name": "PROJECT_ID",
            "description": "Project ID",
            "type": "string",
            "required": True,
            "user-settable": False,
            "replacement-key": "[PROJECT_ID]",
        }
    )

    # Access session via Container service args and derive
    if row_frequency == Clinical.session:
        # Set the object the pipeline is to be run against
        context = ["xnat:imageSessionData"]
        # Create Session input that  can be passed to the command line, which
        # will be populated by inputs derived from the XNAT session object
        # passed to the pipeline.
        inputs_json.extend(
            [
                {
                    "name": "SESSION_LABEL",
                    "description": "Imaging session label",
                    "type": "string",
                    "required": True,
                    "user-settable": False,
                    "replacement-key": "[SESSION_LABEL]",
                },
                {
                    "name": "SUBJECT_LABEL",
                    "description": "Subject label",
                    "type": "string",
                    "required": True,
                    "user-settable": False,
                    "replacement-key": "[SUBJECT_LABEL]",
                },
            ]
        )
        # Add specific session to process to command line args
        cmdline += " --ids [SESSION_LABEL] "
        # Access the session XNAT object passed to the pipeline
        external_inputs = [
            {
                "name": "SESSION",
                "description": "Imaging session",
                "type": "Session",
                "source": None,
                "default-value": None,
                "required": True,
                "replacement-key": None,
                "sensitive": None,
                "provides-value-for-command-input": None,
                "provides-files-for-command-mount": "in",
                "via-setup-command": None,
                "user-settable": False,
                "load-children": True,
            }
        ]
        # Access to project ID and session label from session XNAT object
        derived_inputs = [
            {
                "name": "__SESSION_LABEL__",
                "type": "string",
                "derived-from-wrapper-input": "SESSION",
                "derived-from-xnat-object-property": "label",
                "provides-value-for-command-input": "SESSION_LABEL",
                "user-settable": False,
            },
            {
                "name": "__SUBJECT_ID__",
                "type": "string",
                "derived-from-wrapper-input": "SESSION",
                "derived-from-xnat-object-property": "subject-id",
                "provides-value-for-command-input": "SUBJECT_LABEL",
                "user-settable": False,
            },
            {
                "name": "__PROJECT_ID__",
                "type": "string",
                "derived-from-wrapper-input": "SESSION",
                "derived-from-xnat-object-property": "project-id",
                "provides-value-for-command-input": "PROJECT_ID",
                "user-settable": False,
            },
        ]

    else:
        raise NotImplementedError(
            "Wrapper currently only supports session-level pipelines"
        )

    # Generate the complete configuration JSON
    xnat_command = {
        "name": name,
        "description": f"{name} {pkg_version} ({wrapper_version}): {description}",
        "label": name,
        "version": version,
        "schema-version": "1.0",
        "image": image_tag,
        "index": registry,
        "type": "docker",
        "command-line": cmdline,
        "override-entrypoint": True,
        "mounts": [
            {"name": "in", "writable": False, "path": str(XnatViaCS.INPUT_MOUNT)},
            {"name": "out", "writable": True, "path": str(XnatViaCS.OUTPUT_MOUNT)},
            {  # Saves the Pydra-cache directory outside of the container for easier debugging
                "name": "work",
                "writable": True,
                "path": str(XnatViaCS.WORK_MOUNT),
            },
        ],
        "ports": {},
        "inputs": inputs_json,
        "outputs": outputs_json,
        "xnat": [
            {
                "name": name,
                "description": description,
                "contexts": context,
                "external-inputs": external_inputs,
                "derived-inputs": derived_inputs,
                "output-handlers": output_handlers,
            }
        ],
    }

    if info_url:
        xnat_command["info-url"] = info_url

    return xnat_command


def copy_command_ref(dockerfile: DockerRenderer, xnat_commands, build_dir):
    """Generate Neurodocker instructions to copy a version of the XNAT commands
    into the image for reference

    Parameters
    ----------
    dockerfile : DockerRenderer
        Neurodocker renderer to build
    xnat_commands : list[dict]
        XNAT command JSONs to copy into the Dockerfile for reference
    build_dir : Path
        path to build directory
    """
    # Copy command JSON inside dockerfile for ease of reference
    cmds_dir = build_dir / "xnat_commands"
    cmds_dir.mkdir(exist_ok=True)
    for cmd in xnat_commands:
        fname = cmd.get("name", "command") + ".json"
        with open(cmds_dir / fname, "w") as f:
            json.dump(cmd, f, indent="    ")
    dockerfile.copy(source=["./xnat_commands"], destination="/xnat_commands")


def save_store_config(dockerfile: DockerRenderer, build_dir: Path, test_config=False):
    """Save a configuration for a XnatViaCS store.

    Parameters
    ----------
    dockerfile : DockerRenderer
        Neurodocker renderer to build
    build_dir : Path
        the build directory to save supporting files
    test_config : bool
        whether the target XNAT is using the local test configuration, in which
        case the server location will be hard-coded rather than rely on the
        XNAT_HOST environment variable passed to the container by the XNAT CS
    """
    xnat_cs_store_entry = {"class": "<" + class_location(XnatViaCS) + ">"}
    if test_config:
        if sys.platform == "linux":
            ip_address = "172.17.0.1"  # Linux + GH Actions
        else:
            ip_address = "host.docker.internal"  # Mac/Windows local debug
        xnat_cs_store_entry["server"] = "http://" + ip_address + ":8080"
    DataStore.save_entries(
        {"xnat-cs": xnat_cs_store_entry}, config_path=build_dir / "stores.yaml"
    )
    dockerfile.run(command="mkdir -p /root/.arcana")
    dockerfile.run(command=f"mkdir -p {str(XnatViaCS.CACHE_DIR)}")
    dockerfile.copy(
        source=["./stores.yaml"], destination=IN_DOCKER_ARCANA_HOME_DIR + "/stores.yaml"
    )
    dockerfile.env(ARCANA_HOME=IN_DOCKER_ARCANA_HOME_DIR)


def create_metapackage(image_tag, manifest, use_local_packages=False):

    build_dir = Path(tempfile.mkdtemp())

    dockerfile = construct_dockerfile(
        build_dir=build_dir, use_local_packages=use_local_packages
    )

    with open(build_dir / "manifest.json", "w") as f:
        json.dump(manifest, f)

    dockerfile.copy(["./manifest.json"], "/manifest.json")

    dockerfile.entrypoint(
        [
            "conda",
            "run",
            "--no-capture-output",
            "-n",
            "arcana",
            "arcana",
            "deploy",
            "xnat",
            "pull-images",
            "/manifest.json",
        ]
    )

    dockerfile_build(dockerfile, build_dir, image_tag)


@dataclass
class InputArg:
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


@dataclass
class OutputArg:
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


@dataclass
class ParamArg:
    name: str  # How the input will be referred to in the XNAT dialog, defaults to pydra_field name
    type: type = str
    pydra_field: str = None  # Name of parameter to expose in Pydra task
    required: bool = False
    default: str = None
    description: str = ""  # description of the parameter

    def __post_init__(self):
        if self.pydra_field is None:
            self.pydra_field = path2varname(self.name)


COMMAND_INPUT_TYPES = {bool: "bool", str: "string", int: "number", float: "number"}


VALID_FREQUENCIES = (Clinical.session, Clinical.dataset)
