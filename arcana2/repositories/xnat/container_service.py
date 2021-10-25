
import json
import logging
from pathlib import Path
import site
import shutil
import tempfile
import pkg_resources
from dataclasses import dataclass
import cloudpickle as cp
from attr import NOTHING
import neurodocker as nd
from natsort import natsorted
from arcana2.dataspaces.clinical import Clinical
from arcana2.core.data.datatype import FileFormat
from arcana2.core.data import DataSpace
from arcana2.core.utils import resolve_class, DOCKER_HUB, ARCANA_PIP
from arcana2.exceptions import ArcanaUsageError
from arcana2.__about__ import PACKAGE_NAME, python_versions

logger = logging.getLogger('arcana')


def generate_dockerfile(pydra_task, image_tag, inputs, outputs, description, 
                        maintainer, build_dir, parameters=None,
                        requirements=None, packages=None,
                        frequency=Clinical.session, registry=DOCKER_HUB,
                        extra_labels=None):
    """Constructs a dockerfile that wraps a with dependencies

    Parameters
    ----------
    pydra_task : pydra.Task or pydra.Workflow
        The Pydra Task or Workflow to be run in XNAT container service
    image_tag : str
        The name of the Docker image, preceded by the registry it will be stored
    inputs : list[InputArg]
        Inputs to be provided to the container
    outputs : list[OutputArg]
        Outputs from the container
    description : str
        User-facing description of the pipeline
    maintainer : str
        The name and email of the developer creating the wrapper (i.e. you)
    build_dir : Path
        Path to the directory to create the Dockerfile in and copy any local
        files to
    parameters : list[str]
        Parameters to be exposed in the CS command
    requirements : list[tuple[str, str]]
        Name and version of the Neurodocker requirements to add to the image
    packages : list[tuple[str, str]]
        Name and version of the Python PyPI packages to add to the image
    registry : str
        URI of the Docker registry to upload the image to
    extra_labels : dict[str, str], optional
        Additional labels to be added to the image

    Returns
    -------
    str
        Generated Dockerfile
    """

    labels = {}
    packages = list(packages)

    if build_dir is None:
        build_dir = tempfile.mkdtemp()
    if parameters is None:
        parameters = []
    if requirements is None:
        requirements = []
    if packages is None:
        packages = []

    if maintainer:
        labels["maintainer"] = maintainer

    pipeline_name = pydra_task.name.replace('.', '_').capitalize()

    cmd_json = generate_json_config(
        pipeline_name, pydra_task, image_tag, inputs, outputs,
        parameters, description, frequency=frequency, registry=registry)

    # Convert JSON into Docker label
    labels['org.nrg.commands'] = '[' + json.dumps(cmd_json) + ']'
    if extra_labels:
        labels.update(extra_labels)

    instructions = [
        ["base", "debian:bullseye"],
        ["install", ["git", "vim", "ssh-client", "python3", "python3-pip"]]]

    for req in requirements:
        req_name = req[0]
        install_props = {}
        if len(req) > 1 and req[1] != '.':
            install_props['version'] = req[1]
        if len(req) > 2:
            install_props['method'] = req[2]
        instructions.append([req_name, install_props])

    arcana_pkg = next(p for p in pkg_resources.working_set
                      if p.key == PACKAGE_NAME)
    arcana_pkg_loc = Path(arcana_pkg.location).resolve()
    site_pkg_locs = [Path(p).resolve() for p in site.getsitepackages()]

    # Use local installation of arcana
    if arcana_pkg_loc not in site_pkg_locs:
        shutil.rmtree(build_dir / 'arcana')
        shutil.copytree(arcana_pkg_loc, build_dir / 'arcana')
        arcana_pip = '/arcana'
        instructions.append(['copy', ['./arcana', arcana_pip]])
    else:
        direct_url_path = Path(arcana_pkg.egg_info) / 'direct_url.json'
        if direct_url_path.exists():
            with open(direct_url_path) as f:
                durl = json.load(f)             
            arcana_pip = f"{durl['vcs']}+{durl['url']}@{durl['commit_id']}"
        else:
            arcana_pip = f"{arcana_pkg.key}=={arcana_pkg.version}"
    packages.append(arcana_pip)

    instructions.append(['run', 'pip3 install ' + ' '.join(packages)])

    # instructions.append(
    #     ["miniconda", {
    #         "create_env": "arcana2",
    #         "conda_install": [
    #             "python=" + natsorted(python_versions)[-1],
    #             "numpy",
    #             "traits"],
    #         "pip_install": packages}])

    if labels:
        instructions.append(["label", labels])

    neurodocker_specs = {
        "pkg_manager": "apt",
        "instructions": instructions}

    dockerfile = nd.Dockerfile(neurodocker_specs).render()

    # Save generated dockerfile to file
    out_file = build_dir / 'Dockerfile'
    out_file.parent.mkdir(exist_ok=True, parents=True)
    with open(str(out_file), 'w') as f:
        f.write(dockerfile)
    logger.info("Dockerfile generated at %s", out_file)

    return dockerfile


def generate_json_config(pipeline_name, pydra_task, image_tag,
                         inputs, outputs, parameters, desc,
                         frequency=Clinical.session, registry=DOCKER_HUB):
    """Constructs the XNAT CS "command" JSON config, which specifies how XNAT
    should handle the containerised pipeline

    Parameters
    ----------
    pipeline_name : str
        Name of the pipeline
    pydra_task : Task or Workflow
        [description]
    image_tag : str
        Name + version of the Docker image to be created
    inputs : list[InputArg]
        Inputs to be provided to the container
    outputs : list[OutputArg]
        Outputs from the container
    parameters : list[str]
        Parameters to be exposed in the CS command
    description : str
        User-facing description of the pipeline
    frequency : Clinical
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
    if isinstance(frequency, str):
        frequency = Clinical[frequency]
    if frequency not in VALID_FREQUENCIES:
        raise ArcanaUsageError(
            f"'{frequency}'' is not a valid option ('"
            + "', '".join(VALID_FREQUENCIES) + "')")

    # Convert tuples to appropriate dataclasses for inputs and outputs
    inputs = [InputArg(*i) for i in inputs if isinstance(i, tuple)]
    outputs = [OutputArg(*o) for o in outputs if isinstance(o, tuple)]

    image_name, version = image_tag.split(':')

    cmd_inputs = []
    input_names = []

    field_specs = dict(pydra_task.input_spec.fields)

    for inpt in inputs:
        spec = field_specs[inpt.name]
        
        desc = spec.metadata.get('help_string', '')
        if spec.type in (str, Path):
            desc = (f"Scan match: {desc} "
                    "[SCAN_TYPE [ORDER [TAG=VALUE, ...]]]")
            input_type = 'string'
        else:
            desc = f"Field match ({spec.type}): {desc} [FIELD_NAME]"
            input_type = COMMAND_INPUT_TYPES[spec.type]
        cmd_inputs.append({
            "name": inpt.name,
            "description": desc,
            "type": input_type,
            "default-value": "",
            "required": True,
            "user-settable": True,
            "replacement-key": "#{}_INPUT#".format(inpt.name.upper())})

    for param in parameters:
        spec = field_specs[param]
        desc = "Parameter: " + spec.metadata.get('help_string', '')
        required = spec._default is NOTHING

        cmd_inputs.append({
            "name": param,
            "description": desc,
            "type": COMMAND_INPUT_TYPES[spec.type],
            "default-value": (spec._default if not required else ""),
            "required": required,
            "user-settable": True,
            "replacement-key": "#{}_PARAM#".format(param.upper())})

    cmd_inputs.append(
        {
            "name": "project-id",
            "description": "Project ID",
            "type": "string",
            "required": True,
            "user-settable": False,
            "replacement-key": "#PROJECT_ID#"
        })


    input_args = ' '.join('-i {} #{}_INPUT#'.format(i, i.upper())
                            for i in input_names)
    param_args = ' '.join('-p {} #{}_PARAM#'.format(p, p.upper())
                            for p in parameters)

    func = cp.loads(pydra_task.inputs._func)

    cmdline = (
        # f"conda run --no-capture-output -n arcana2 "  # activate conda
        f"arcana run {func.__module__}.{func.__name__} "  # run pydra task in Arcana
        f"#PROJECT_ID# {input_args} {param_args} --work /work " # inputs + params
        "--repository xnat #PROJECT_URI# #TOKEN# #SECRET#")  # pass XNAT API details

    if frequency == Clinical.session:
        cmd_inputs.append(
            {
                "name": "session-id",
                "description": "",
                "type": "string",
                "required": True,
                "user-settable": False,
                "replacement-key": "#SESSION_ID#"
            })
        cmdline += " --session_ids #SESSION_ID# "

    return {
        "name": pipeline_name,
        "description": desc,
        "label": pipeline_name,
        "version": version,
        "schema-version": "1.0",
        "image": image_name,
        "index": registry,
        "type": "docker",
        "command-line": cmdline,
        "override-entrypoint": True,
        "mounts": [
            {
                "name": "in",
                "writable": False,
                "name_path": "/input"
            },
            {
                "name": "output",
                "writable": True,
                "name_path": "/output"
            },
            {
                "name": "work",
                "writable": True,
                "name_path": "/work"
            }
        ],
        "ports": {},
        "inputs": cmd_inputs,
        "outputs": [
            {
                "name": "output",
                "description": "Derivatives",
                "required": True,
                "mount": "out",
                "name_path": None,
                "glob": None
            },
            {
                "name": "working",
                "description": "Working directory",
                "required": True,
                "mount": "work",
                "name_path": None,
                "glob": None
            }
        ],
        "xnat": [
            {
                "name": pipeline_name,
                "description": desc,
                "contexts": ["xnat:imageSessionData"],
                "external-inputs": [
                    {
                        "name": "session",
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
                        "user-settable": None,
                        "load-children": True
                    }
                ],
                "derived-inputs": [
                    {
                        "name": "session-id",
                        "type": "string",
                        "required": True,
                        "load-children": True,
                        "derived-from-wrapper-input": "session",
                        "derived-from-xnat-object-property": "id",
                        "provides-value-for-command-input": "session-id"
                    },
                    {
                        "name": "subject",
                        "type": "Subject",
                        "required": True,
                        "user-settable": False,
                        "load-children": True,
                        "derived-from-wrapper-input": "session"
                    },
                    {
                        "name": "project-id",
                        "type": "string",
                        "required": True,
                        "load-children": True,
                        "derived-from-wrapper-input": "subject",
                        "derived-from-xnat-object-property": "id",
                        "provides-value-for-command-input": "subject-id"
                    }
                ],
                "output-handlers": [
                    {
                        "name": "output-resource",
                        "accepts-command-output": "output",
                        "via-wrapup-command": None,
                        "as-a-child-of": "session",
                        "type": "Resource",
                        "label": "Derivatives",
                        "format": None
                    },
                    {
                        "name": "working-resource",
                        "accepts-command-output": "working",
                        "via-wrapup-command": None,
                        "as-a-child-of": "session",
                        "type": "Resource",
                        "label": "Work",
                        "format": None
                    }
                ]
            }
        ]
    }


@dataclass
class InputArg():
    name: str
    datatype: FileFormat
    frequency: DataSpace

@dataclass
class OutputArg():
    name: str
    datatype: FileFormat


COMMAND_INPUT_TYPES = {
    bool: 'bool',
    str: 'string',
    int: 'number',
    float: 'number'}

VALID_FREQUENCIES = (Clinical.session, Clinical.dataset)
