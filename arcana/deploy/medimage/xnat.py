import typing as ty
from pathlib import Path
import tempfile
import json
from attr import NOTHING
from dataclasses import dataclass
import arcana.data.formats.common
from arcana.data.spaces.medimage import Clinical
from arcana.data.stores.medimage import XnatViaCS
from arcana.core.deploy.build import (
    generate_neurodocker_specs, render_dockerfile, docker_build)
from arcana.core.deploy.utils import DOCKER_HUB
from arcana.core.utils import resolve_class, class_location, asdict
from arcana.core.data.store import DataStore
from arcana.exceptions import ArcanaUsageError


def build_xnat_cs_image(image_tag: str,
                        commands: ty.List[ty.Dict[str, ty.Any]],
                        authors: ty.List[ty.Tuple[str, str]],
                        info_url: str,
                        python_packages: ty.Iterable[str]=(),
                        system_packages: ty.Iterable[str]=(),
                        readme: str=None,
                        docker_registry: str=DOCKER_HUB,
                        build_dir: Path=None,
                        **kwargs):
    """Creates a Docker image containing one or more XNAT commands ready
    to be installed in XNAT's container service plugin

    Parameters
    ----------
    pkg_name : str
        Name of the package as a whole
    commands
        List of command specifications (in dicts) to be installed on the
        image, see `generate_xnat_command` for valid args (dictionary keys).
    authors
        Names and emails of the maintainers of the wrapper pipeline
    info_url
        The URL of the package website explaining the analysis software
        and what it does
    docker_org
        The docker organisation the image will uploaded to
    docker_registry
        The Docker registry the image will be uploaded to
    wrapper_version
        The version of the wrapper specific to the pkg version. It will be
        appended to the package version, e.g. 0.16.2 -> 0.16.2--1
    build_dir : Path
        the directory to build the docker image within, i.e. where to write
        Dockerfile and supporting files to be copied within the image
    **kwargs:
        Passed on to `generate_neurodocker_specs` method
    """   

    if build_dir is None:
        build_dir = tempfile.mkdtemp()
    build_dir = Path(build_dir)

    xnat_commands = []
    for cmd_spec in commands:

        if 'info_url' not in cmd_spec:
            cmd_spec['info_url'] = info_url

        xnat_cmd = generate_xnat_cs_command(
            image_tag=image_tag,
            registry=docker_registry,
            **cmd_spec)

        xnat_commands.append(xnat_cmd)

    nd_specs = generate_neurodocker_specs(
        build_dir,
        labels={'org.nrg.commands': json.dumps(xnat_commands),
                'maintainer': authors[0][1]},
        python_packages=python_packages,
        system_packages=system_packages,
        readme=readme,
        **{k: v for k, v in kwargs.items() if not k.startswith('_')})

    # Copy the generated XNAT commands inside the container for ease of reference
    nd_specs['instructions'].append(
        copy_command_ref(xnat_commands, build_dir))

    nd_specs['instructions'].extend(
        save_store_config(build_dir))

    try:
        render_dockerfile(nd_specs, build_dir)
    except Exception as e:
        e.msg += json.dumps('\n\n' + nd_specs)
        raise e

    docker_build(build_dir, image_tag)

    return build_dir


def generate_xnat_cs_command(name: str,
                             pydra_task: str,
                             image_tag: str,
                             inputs,
                             outputs,
                             description,
                             version,
                             info_url,
                             parameters=None,
                             frequency='session',
                             registry=DOCKER_HUB):
    """Constructs the XNAT CS "command" JSON config, which specifies how XNAT
    should handle the containerised pipeline

    Parameters
    ----------
    name : str
        Name of the container service pipeline
    pydra_task
        The module path and name (separated by ':') to the task to execute,
        e.g. australianimagingservice.mri.neuro.mriqc:task
    image_tag : str
        Name + version of the Docker image to be created
    inputs : ty.List[ty.Union[InputArg, tuple]]
        Inputs to be provided to the container (pydra_field, format, xnat_name, frequency).
        'pydra_field' and 'format' will be passed to "inputs" arg of the Dataset.pipeline() method,
        'frequency' to the Dataset.add_source() method and 'xnat_name' is displayed in the XNAT
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
    parameters : ty.List[str]
        Parameters to be exposed in the CS command    
    frequency : str
        Frequency of the pipeline to generate (can be either 'dataset' or 'session' currently)
    registry : str
        URI of the Docker registry to upload the image to

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
    if isinstance(frequency, str):
        frequency = Clinical[frequency]
    if frequency not in VALID_FREQUENCIES:
        raise ArcanaUsageError(
            f"'{frequency}'' is not a valid option ('"
            + "', '".join(VALID_FREQUENCIES) + "')")

    # Convert tuples to appropriate dataclasses for inputs, outputs and parameters
    def parse_specs(args, klass):
        parsed_args = []
        for arg in args:
            if isinstance(arg, klass):
                parsed = arg
            else:
                parsed = klass(**arg)
                if isinstance(parsed.format, str):
                    parsed.format = resolve_class(
                        parsed.format, prefixes=['arcana.data.formats'])
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
        replacement_key = f'[{inpt.pydra_field.upper()}_INPUT]'
        if inpt.format in (str, Path):
            desc = (f"Match resource [PATH:STORED_DTYPE]: {inpt.description} ")
            input_type = 'string'
        else:
            desc = f"Match field ({inpt.format.class_name()}) [PATH:STORED_DTYPE]: {inpt.description} "
            input_type = COMMAND_INPUT_TYPES.get(inpt.format, 'string')
        inputs_json.append({
            "name": inpt.xnat_name,
            "description": desc,
            "type": input_type,
            "default-value": "",
            "required": True,
            "user-settable": True,
            "replacement-key": replacement_key})
        input_args.append(
            f"--input '{replacement_key}' {inpt.pydra_field} {inpt.format.location()} ")

    # Add parameters as additional inputs to inputs JSON specification
    param_args = []
    for param in parameters:
        desc = f"Parameter ({param.type}): " + param.description
        
        replacement_key = f'[{param.pydra_field.upper()}_PARAM]'

        inputs_json.append({
            "name": param.xnat_name,
            "description": desc,
            "type": COMMAND_INPUT_TYPES.get(param.type, 'string'),
            "default-value": (param.default if not param.required else ""),
            "required": param.required,
            "user-settable": True,
            "replacement-key": replacement_key})
        param_args.append(
            f"--parameter {param.pydra_field} '{replacement_key}' ")

    # Set up output handlers and arguments
    outputs_json = []
    output_handlers = []
    output_args = []
    for output in outputs:
        xnat_path = output.xnat_path if output.xnat_path else output.pydra_field
        label = xnat_path.split('/')[0]
        out_fname = xnat_path + ('.' + output.format.ext if output.format.ext else '')
        # output_fname = xnat_path
        # if output.format.ext is not None:
        #     output_fname += output.format.ext
        # Set the path to the 
        outputs_json.append({
            "name": output.pydra_field,
            "description": f"{output.pydra_field} ({output.format.location()})",
            "required": True,
            "mount": "out",
            "path": out_fname,
            "glob": None})
        output_handlers.append({
            "name": f"{output.pydra_field}-resource",
            "accepts-command-output": output.pydra_field,
            "via-wrapup-command": None,
            "as-a-child-of": "SESSION",
            "type": "Resource",
            "label": label,
            "format": output.format.class_name()})
        output_args.append(
            f'--output {xnat_path} {output.pydra_field} {output.format.location()} ')

    input_args_str = ' '.join(input_args)
    output_args_str = ' '.join(output_args)
    param_args_str = ' '.join(param_args)

    cmdline = (
        f"conda run --no-capture-output -n arcana "  # activate conda
        f"run-arcana-pipeline  xnat-cs//[PROJECT_ID] {name} {pydra_task} "  # run pydra task in Arcana
        + input_args_str
        + output_args_str
        + param_args_str +
        f"--plugin serial "  # Use serial processing instead of parallel to simplify outputs
        f"--loglevel info "
        f"--work {XnatViaCS.WORK_MOUNT} "  # working directory
        f"--dataset_space medimage:Clinical "
        f"--dataset_hierarchy subject,session "
        f"--frequency {frequency} ")  # pass XNAT API details
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
            "replacement-key": "[PROJECT_ID]"
        })

    # Access session via Container service args and derive 
    if frequency == Clinical.session:
        # Set the object the pipeline is to be run against
        context = ["xnat:imageSessionData"]
        # Create Session input that  can be passed to the command line, which
        # will be populated by inputs derived from the XNAT session object
        # passed to the pipeline.
        inputs_json.append(
            {
                "name": "SESSION_LABEL",
                "description": "Imaging session label",
                "type": "string",
                "required": True,
                "user-settable": False,
                "replacement-key": "[SESSION_LABEL]"
            })
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
                "load-children": True}]
        # Access to project ID and session label from session XNAT object
        derived_inputs = [
            {
                "name": "__SESSION_LABEL__",
                "type": "string",
                "derived-from-wrapper-input": "SESSION",
                "derived-from-xnat-object-property": "label",
                "provides-value-for-command-input": "SESSION_LABEL",
                "user-settable": False
            },
            {
                "name": "__PROJECT_ID__",
                "type": "string",
                "derived-from-wrapper-input": "SESSION",
                "derived-from-xnat-object-property": "project-id",
                "provides-value-for-command-input": "PROJECT_ID",
                "user-settable": False
            }]
    
    else:
        raise NotImplementedError(
            "Wrapper currently only supports session-level pipelines")

    # Generate the complete configuration JSON
    xnat_command = {
        "name": name,
        "description": description,
        "label": name,
        "version": version,
        "schema-version": "1.0",
        "image": image_tag,
        "index": registry,
        "type": "docker",
        "command-line": cmdline,
        "override-entrypoint": True,
        "mounts": [
            {
                "name": "in",
                "writable": False,
                "path": str(XnatViaCS.INPUT_MOUNT)
            },
            {
                "name": "out",
                "writable": True,
                "path": str(XnatViaCS.OUTPUT_MOUNT)
            },
            {  # Saves the Pydra-cache directory outside of the container for easier debugging
                "name": "work",
                "writable": True,
                "path": str(XnatViaCS.WORK_MOUNT)
            }
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
                "output-handlers": output_handlers
            }
        ]
    }

    if info_url:
        xnat_command['info-url'] = info_url

    return xnat_command


def copy_command_ref(xnat_commands, build_dir):
    """Generate Neurodocker instructions to copy a version of the XNAT commands
    into the image for reference

    Parameters
    ----------
    xnat_commands : list[dict]
        XNAT command JSONs to copy into the Dockerfile for reference
    build_dir : Path
        path to build directory

    Returns
    -------
    list[str, list[str, str]]
        Instruction to copy the XNAT commands into the Dockerfile
    """
    # Copy command JSON inside dockerfile for ease of reference
    cmds_dir = build_dir / 'xnat_commands'
    cmds_dir.mkdir()
    for cmd in xnat_commands:
        fname = cmd.get('name', 'command') + '.json'
        with open(cmds_dir / fname, 'w') as f:
            json.dump(cmd, f, indent='    ')
    return ['copy', ['./xnat_commands', '/xnat_commands']]


def save_store_config(build_dir: Path):
    """Save a configuration for a XnatViaCS store.

    Parameters
    ----------
    build_dir : Path
        the build directory to save supporting files

    Returns
    -------
    list[list[str]]
        Docker instructions to add stores to the image
    """
    DataStore.save_entries(
        {'xnat-cs': {'class': '<' + class_location(XnatViaCS) + '>'}},
        config_path=build_dir / 'stores.yml')
    return [
        ['run', 'mkdir -p /root/.arcana'],
        ['run', f'mkdir -p {str(XnatViaCS.CACHE_DIR)}'],
        ['copy', ['./stores.yml', '/root/.arcana/stores.yml']]]


@dataclass
class InputArg():
    pydra_field: str  # Must match the name of the Pydra task input
    format: type = arcana.data.formats.common.File
    xnat_name: str = None # How the input will be referred to in the XNAT dialog, defaults to the pydra_field name
    frequency: Clinical = Clinical.session
    description: str = ''  # description of the input

    def __post_init__(self):
        if self.xnat_name is None:
            self.xnat_name = self.pydra_field

@dataclass
class OutputArg():
    pydra_field: str  # Must match the name of the Pydra task output
    format: type = arcana.data.formats.common.File
    xnat_path: str = None  # The path the output is stored at in XNAT, defaults to the pydra name

@dataclass
class ParamArg():
    pydra_field: str  # Name of parameter to expose in Pydra task
    xnat_name: str = None  # How the input will be referred to in the XNAT dialog, defaults to pydra_field name
    type: type = str
    required: bool = False
    description: str = ''  # description of the parameter

    def __post_init__(self):
        if self.xnat_name is None:
            self.xnat_name = self.pydra_field


COMMAND_INPUT_TYPES = {
    bool: 'bool',
    str: 'string',
    int: 'number',
    float: 'number'}


VALID_FREQUENCIES = (Clinical.session, Clinical.dataset)
