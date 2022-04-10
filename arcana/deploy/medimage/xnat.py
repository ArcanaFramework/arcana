import typing as ty
from pathlib import Path
import tempfile
import json
from attr import NOTHING
from dataclasses import dataclass
from arcana.data.spaces.medimage import Clinical
from arcana.data.stores.medimage import XnatViaCS
from arcana.core.deploy.build import (
    generate_neurodocker_specs, render_dockerfile, docker_build)
from arcana.core.deploy.utils import DOCKER_HUB
from arcana.core.utils import resolve_class
from arcana.core.data.store import DataStore
from arcana.exceptions import ArcanaUsageError


def build_xnat_cs_image(image_tag: str,
                        commands: ty.List[ty.Dict[str, ty.Any]],
                        pkg_version: str,
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
    pkg_version
        Version of the package the commands are drawn from (could be 3.0.3
        for MRtrix3 for example)
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
            version=pkg_version,
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
        **kwargs)

    # Copy the generated XNAT commands inside the container for ease of reference
    nd_specs['instructions'].append(
        copy_command_ref(xnat_commands, build_dir))

    nd_specs['instructions'].extend(
        save_store_config(build_dir))

    render_dockerfile(nd_specs, build_dir)

    docker_build(build_dir, image_tag)


def generate_xnat_cs_command(name: str,
                             pydra_task: str,
                             image_tag: str,
                             inputs,
                             outputs,
                             description,
                             version,
                             parameters=None,
                             frequency='session',
                             registry=DOCKER_HUB,
                             info_url=None):
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
        Inputs to be provided to the container (pydra_field, format, ui_name, frequency).
        'pydra_field' and 'format' will be passed to "inputs" arg of the Dataset.pipeline() method,
        'frequency' to the Dataset.add_source() method and 'ui_name' is displayed in the XNAT
        UI
    outputs : ty.List[ty.Union[OutputArg, tuple]]
        Outputs to extract from the container (pydra_field, format, output_path).
        'pydra_field' and 'format' will be passed as "outputs" arg the Dataset.pipeline() method,
        'output_path' determines the path the output will saved in the XNAT data tree.
    description : str
        User-facing description of the pipeline
    version : str
        Version string for the wrapped pipeline
    parameters : ty.List[str]
        Parameters to be exposed in the CS command    
    frequency : str
        Frequency of the pipeline to generate (can be either 'dataset' or 'session' currently)
    registry : str
        URI of the Docker registry to upload the image to
    info_url : str
        URI explaining in detail what the pipeline does

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
    inputs = [InputArg(*i) if not isinstance(i, InputArg) else i
              for i in inputs]
    outputs = [OutputArg(*o) if not isinstance(o, OutputArg) else o
               for o in outputs]
    parameters = [
        ParamArg(p) if isinstance(p, str) else (
            ParamArg(*p) if not isinstance(p, ParamArg) else p)
        for p in parameters]

    pydra_task = resolve_class(pydra_task)()
    input_specs = dict(f[:2] for f in pydra_task.input_spec.fields)
    # output_specs = dict(f[:2] for f in pydra_task.output_spec.fields)

    # JSON to define all inputs and parameters to the pipelines
    inputs_json = []

    # Add task inputs to inputs JSON specification
    input_args = []
    for inpt in inputs:
        ui_name = inpt.ui_name if inpt.ui_name else inpt.pydra_field
        replacement_key = f'[{ui_name.upper()}_INPUT]'
        spec = input_specs[inpt.pydra_field]
        
        desc = spec.metadata.get('help_string', '')
        if spec.type in (str, Path):
            desc = (f"Match resource [PATH:STORED_DTYPE]: {desc} ")
            input_type = 'string'
        else:
            desc = f"Match field ({spec.type}) [PATH:STORED_DTYPE]: {desc} "
            input_type = COMMAND_INPUT_TYPES.get(spec.type, 'string')
        inputs_json.append({
            "name": ui_name,
            "description": desc,
            "type": input_type,
            "default-value": "",
            "required": True,
            "user-settable": True,
            "replacement-key": replacement_key})
        input_args.append(
            f"--input {inpt.pydra_field} {inpt.format} {replacement_key}")

    # Add parameters as additional inputs to inputs JSON specification
    param_args = []
    for param in parameters:
        ui_name = param.ui_name if param.ui_name else param.pydra_field
        spec = input_specs[param.pydra_field]
        desc = f"Parameter ({spec.type}): " + spec.metadata.get('help_string', '')
        required = spec._default is NOTHING
        
        replacement_key = f'[{ui_name.upper()}_PARAM]'

        inputs_json.append({
            "name": ui_name,
            "description": desc,
            "type": COMMAND_INPUT_TYPES.get(spec.type, 'string'),
            "default-value": (spec._default if not required else ""),
            "required": required,
            "user-settable": True,
            "replacement-key": replacement_key})
        param_args.append(
            f"--parameter {param.pydra_field} {replacement_key}")

    # Set up output handlers and arguments
    outputs_json = []
    output_handlers = []
    output_args = []
    for output in outputs:
        xnat_path = output.xnat_path if output.xnat_path else output.pydra_field
        label = xnat_path.split('/')[0]
        out_fname = xnat_path + (output.format.ext if output.format.ext else '')
        # output_fname = xnat_path
        # if output.format.ext is not None:
        #     output_fname += output.format.ext
        # Set the path to the 
        outputs_json.append({
            "name": output.pydra_field,
            "description": f"{output.pydra_field} ({output.format})",
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
            "format": output.format.name})
        output_args.append(
            f'--output {output.pydra_field} {output.format} {xnat_path}')

    input_args_str = ' '.join(input_args)
    output_args_str = ' '.join(output_args)
    param_args_str = ' '.join(param_args)

    cmdline = (
        f"conda run --no-capture-output -n arcana "  # activate conda
        f"run-arcana-pipeline  xnat-cs//[PROJECT_ID] {name} {pydra_task} "  # run pydra task in Arcana
        f" {input_args_str} {output_args_str} {param_args_str} " # inputs, outputs + params
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
        cmdline += ' [SESSION_LABEL]'  # Pass node-id to XnatViaCS repo
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
        with open(build_dir / fname, 'w') as f:
            json.dump(cmd, f, indent='    ')
    return ['copy', ['./xnat_commands', '/xnat_commands']]


def save_store_config(build_dir: Path):
    """Save a configuration for 

    Parameters
    ----------
    build_dir : Path
        _description_

    Returns
    -------
    _type_
        _description_
    """
    DataStore.save_entries({'xnat-cs': {}},
                           config_path=build_dir / 'stores.yml')
    return [
        ['run', ['mkdir', '-p', '/root/.arcana']],
        ['copy', ['./stores.yml', '/root/.arcana/stores.yml']]]


@dataclass
class InputArg():
    pydra_field: str  # Must match the name of the Pydra task input
    format: type
    ui_name: str = None # The name of the parameter in the XNAT dialog, defaults to the pydra name
    frequency: Clinical = Clinical.session


@dataclass
class OutputArg():
    pydra_field: str  # Must match the name of the Pydra task output
    format: type
    xnat_path: str = None  # The path the output is stored at in XNAT, defaults to the pydra name


@dataclass
class ParamArg():
    pydra_field: str  # Name of parameter to expose in Pydra task
    ui_name: str = None  # defaults to pydra_field


COMMAND_INPUT_TYPES = {
    bool: 'bool',
    str: 'string',
    int: 'number',
    float: 'number'}


VALID_FREQUENCIES = (Clinical.session, Clinical.dataset)
