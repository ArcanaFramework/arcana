from dataclasses import dataclass
import tempfile
import sys
import json
from pathlib import Path
from logging import getLogger
import docker
import neurodocker as nd
from arcana2.core.utils import resolve_class
from arcana2.dataspaces.clinical import Clinical
from arcana2.core.data.datatype import FileFormat
from arcana2.core.data import DataSpace
from arcana2.exceptions import ArcanaUsageError
from .run import BaseRunCmd
from arcana2.core.utils import resolve_datatype


logger = getLogger('arcana')


class Wrap4XnatCmd():

    desc = ("Create a containerised pipeline from a given set of inputs to "
            "generate specified derivatives")

    @classmethod
    def construct_parser(cls, parser):
        parser.add_argument('interface',
                            help=("The location (on Python path) of the Pydra "
                                  "interface to wrap"))
        parser.add_argument('image_name', metavar='IMAGE',
                            help=("The name of the Docker image, preceded by "
                                  "the registry it will be stored"))
        parser.add_argument('out_file',
                            help="The path to save the Dockerfile to")
        parser.add_argument('version',
                            help=("Version of the container pipeline"))        
        parser.add_argument('--input', '-i', action='append', default=[],
                            nargs=3, metavar=('NAME', 'DATATYPE', 'FREQUENCY'),
                            help="Inputs to be used by the app")
        parser.add_argument('--output', '-o', action='append', default=[],
                            nargs=2, metavar=('NAME', 'DATATYPE'),
                            help="Outputs of the app to stored back in XNAT")
        parser.add_argument('--parameter', '-p', metavar='NAME', action='append',
                            help=("Fixed parameters of the Pydra workflow to "
                                  "expose to the container service"))
        parser.add_argument('--requirement', '-r', nargs='+', action='append',
                            help=("Software requirements to be added to the "
                                  "the docker image using Neurodocker. "
                                  "Neurodocker requirement name, followed by "
                                  "optional version and installation "
                                  "method args (see Neurodocker docs). Use "
                                  "'.' to skip version arg and use the latest "
                                  "available"))
        parser.add_argument('--package', '-k', action='append',
                            help="PyPI packages to be installed in the env")
        parser.add_argument('--maintainer', '-m', type=str, default=None,
                            help="Maintainer of the pipeline")
        parser.add_argument('--description', '-d', default=None,
                            help="A description of what the pipeline does")
        parser.add_argument('--registry', default=cls.DOCKER_HUB,
                            help="The registry to install the ")
        parser.add_argument('--build_dir', default=None,
                            help=("The directory to build the dockerfile in. "
                                  "Defaults to a temporary directory"))
        parser.add_argument('--dry_run', action='store_true', default=False,
                            help=("Don't build the generated Dockerfile"))
        parser.add_argument('--frequency', default='session',
                            help=("Whether the resultant container runs "
                                  "against a session or a whole dataset "
                                  "(i.e. project). Can be one of either "
                                  "'session' or 'dataset'"))

    @classmethod
    def run(cls, args):

        inputs = BaseRunCmd.parse_input_args(args)
        outputs = BaseRunCmd.parse_output_args(args)

        labels = {}

        image_path = args.image_name + ':' + args.tag

        if args.maintainer:
            labels["maintainer"] = args.maintainer

        pipeline_name = args.interface.replace('.', '_').capitalize()

        cmd_json = cls.construct_command_json(
            pipeline_name, args.interface, image_path, inputs, outputs,
            args.parameter, args.description, registry=args.registry)
        # Convert JSON into Docker label
        print(json.dumps(cmd_json, indent=2))
        cmd_label = json.dumps(cmd_json).replace('"', r'\"').replace(
            '$', r'\$')
        labels['org.nrg.commands'] = '[{' + cmd_label + '}]'
        labels['arcana.wrap4xnat'] = ' '.join(sys.argv)

        instructions = [
            ["base", "debian:stretch"],
            ["install", ["git", "vim"]]]

        for req in args.requirement:
            req_name = req[0]
            install_props = {}
            if len(req) > 1 and req[1] != '.':
                install_props['version'] = req[1]
            if len(req) > 2:
                install_props['method'] = req[2]
            instructions.append([req_name, install_props])

        if labels:
            instructions.append(["label", labels])

        pip_packages = [cls.ARCANA_PIP_PATH] + list(args.packages)

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

        dockerfile = nd.Dockerfile(neurodocker_specs).render()

        out_file = Path(args.out_file)

        out_file.parent.mkdir(exist_ok=True, parents=True)
        with open(str(out_file), 'w') as f:
            f.write(dockerfile)

        logger.info("Dockerfile generated at %s", out_file)

        if not args.dry_run:

            if args.build_dir:
                build_dir = Path(args.build_dir)
            else:
                build_dir = Path(tempfile.mkdtemp())

            logger.info("Building dockerfile at %s dir", str(build_dir))

            dc = docker.from_env()

            image, _ = dc.images.build(path=str(build_dir),
                                       tag=image_path)

            image.push(args.registry)

    DOCKER_HUB = 'https://index.docker.io/v1/'
    ARCANA_PIP_PATH = "git+https://github.com/australian-imaging-service/arcana2.git"


    def construct_command_json(cls, pipeline_name, interface_name, image_name,
                               inputs, version, outputs, parameters, desc,
                               frequency, registry):

        
        pydra_interface = resolve_class(interface_name)

        if frequency not in cls.VALID_FREQUENCIES:
            raise ArcanaUsageError(
                f"'{frequency}'' is not a valid option ('"
                + "', '".join(cls.VALID_FREQUENCIES) + "')")

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
                "type": cls.COMMAND_INPUT_TYPES[spec.datatype],
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
            f"arcana run {interface_name} #PROJECT_ID# "
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


    @classmethod
    def parse_input_args(cls, args):
        for inpt in args.input:
            name, required_datatype_name, frequency = inpt
            required_datatype = resolve_datatype(required_datatype_name)
            yield InputArg(name, required_datatype, frequency)

    @classmethod
    def parse_output_args(cls, args):
        for output in args.output:
            name, datatype_name_name = output
            produced_datatype = resolve_datatype(datatype_name_name)
            yield OutputArg(name, produced_datatype)


    COMMAND_INPUT_TYPES = {
        bool: 'bool',
        str: 'string',
        int: 'number',
        float: 'number'}

    VALID_FREQUENCIES = ('session', 'dataset')
    

@dataclass
class InputArg():
    name: str
    datatype: FileFormat
    frequency: DataSpace

@dataclass
class OutputArg():
    name: str
    datatype: FileFormat
    
