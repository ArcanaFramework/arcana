
import json
import logging
from dataclasses import dataclass
import neurodocker as nd
from arcana2.dataspaces.clinical import Clinical
from arcana2.core.data.datatype import FileFormat
from arcana2.core.data import DataSpace
from arcana2.core.utils import resolve_class
from arcana2.exceptions import ArcanaUsageError

logger = logging.getLogger('arcana')


DOCKER_HUB = 'https://index.docker.io/v1/'
ARCANA_PIP = "git+https://github.com/australian-imaging-service/arcana2.git"


def generate_dockerfile(pydra_interface, image_tag, inputs, outputs,
                        parameters, requirements, packages,  description,
                        maintainer, registry=DOCKER_HUB, extra_labels=None):
    """Constructs a dockerfile that wraps a with dependencies

    Parameters
    ----------
    pydra_interface : pydra.Task or pydra.Workflow
        The Pydra Task or Workflow to be run in XNAT container service
    image_tag : str
        The name of the Docker image, preceded by the registry it will be stored
    inputs : list[InputArg]
        Inputs to be provided to the container
    outputs : list[OutputArg]
        Outputs from the container
    parameters : list[str]
        Parameters to be exposed in the CS command
    requirements : list[tuple[str, str]]
        Name and version of the Neurodocker requirements to add to the image
    packages : list[tuple[str, str]]
        Name and version of the Python PyPI packages to add to the image
    registry : str
        URI of the Docker registry to upload the image to
    description : str
        User-facing description of the pipeline
    maintainer : str
        The name and email of the developer creating the wrapper (i.e. you)
    extra_labels : dict[str, str], optional
        Additional labels to be added to the image

    Returns
    -------
    str
        Generated Dockerfile
    """

    labels = {}

    if maintainer:
        labels["maintainer"] = maintainer

    pipeline_name = pydra_interface.replace('.', '_').capitalize()

    cmd_json = generate_json_config(
        pipeline_name, pydra_interface, image_tag, inputs, outputs,
        parameters, description, registry=registry)

    # Convert JSON into Docker label
    print(json.dumps(cmd_json, indent=2))
    cmd_label = json.dumps(cmd_json).replace('"', r'\"').replace(
        '$', r'\$')
    labels['org.nrg.commands'] = '[{' + cmd_label + '}]'
    if extra_labels:
        labels.update(extra_labels)

    instructions = [
        ["base", "debian:stretch"],
        ["install", ["git", "vim"]]]

    for req in requirements:
        req_name = req[0]
        install_props = {}
        if len(req) > 1 and req[1] != '.':
            install_props['version'] = req[1]
        if len(req) > 2:
            install_props['method'] = req[2]
        instructions.append([req_name, install_props])

    if labels:
        instructions.append(["label", labels])

    pip_packages = [ARCANA_PIP] + list(packages)

    instructions.append(
        ["miniconda", {
            "create_env": "arcana2",
            "conda_install": [
                "python=3.8",
                "numpy",
                "traits"],
            "pip_install": pip_packages}])

    neurodocker_specs = {
        "pkg_manager": "apt",
        "instructions": instructions}

    return nd.Dockerfile(neurodocker_specs).render()


def generate_json_config(pipeline_name, pydra_interface, image_tag, version,
                         inputs, outputs, parameters, desc,
                         frequency='session', registry=DOCKER_HUB):
    """Constructs the XNAT CS "command" JSON config, which specifies how XNAT
    should handle the containerised pipeline

    Parameters
    ----------
    pipeline_name : str
        Name of the pipeline
    pydra_interface : Task or Workflow
        [description]
    image_tag : str
        [description]
    version : str
        Version of the 
    inputs : list[InputArg]
        Inputs to be provided to the container
    outputs : list[OutputArg]
        Outputs from the container
    parameters : list[str]
        Parameters to be exposed in the CS command
    description : str
        User-facing description of the pipeline
    frequency : [type]
        [description]
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

    if frequency not in VALID_FREQUENCIES:
        raise ArcanaUsageError(
            f"'{frequency}'' is not a valid option ('"
            + "', '".join(VALID_FREQUENCIES) + "')")

    cmd_inputs = []
    input_names = []
    for inpt in inputs:
        spec = getattr(pydra_interface.inputs, inpt.name)
        
        desc = spec.desc if spec.desc else ""
        if isinstance(spec.required_format, FileFormat):
            desc = (f"Scan match ({spec.datatype}): {desc} "
                    "[SCAN_TYPE [ORDER [TAG=VALUE, ...]]]")
        else:
            desc = f"Field match ({spec.datatype}): {desc} [FIELD_NAME]"
        cmd_inputs.append({
            "name": inpt.name,
            "description": desc,
            "type": "string",
            "default-value": "",
            "required": True,
            "user-settable": True,
            "replacement-key": "#{}_INPUT#".format(inpt.name.upper())})

    for param in parameters:
        # spec = analysis_cls.param_spec(param)
        # desc = "Parameter: " + spec.desc
        # if spec.choices:
        #     desc += " (choices: {})".format(','.join(spec.choices))

        spec = getattr(pydra_interface.inputs, param)

        cmd_inputs.append({
            "name": param,
            "description": desc,
            "type": COMMAND_INPUT_TYPES[spec.datatype],
            "default-value": (spec.default
                                if spec.default is not None else ""),
            "required": spec.default is None,
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

    cmdline = (
        f"arcana run {pydra_interface.__name__} #PROJECT_ID# "
        f"{input_args} {param_args}"
        " --work /work --repository xnat #PROJECT_URI# #TOKEN#", "#SECRET#")

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
        "image": image_tag,
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

VALID_FREQUENCIES = ('session', 'dataset')
