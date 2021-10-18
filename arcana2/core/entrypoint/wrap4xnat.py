import subprocess as sp
import tempfile
import json
from pathlib import Path
from logging import getLogger
import docker
import neurodocker as nd
from arcana2.core.utils import set_loggers, resolve_class
from arcana2.dataspaces.clinical import Clinical
from arcana2.exceptions import ArcanaUsageError, ArcanaRequirementVersionsError


logger = getLogger('arcana')

class Wrap4XnatCmd():

    desc = ("Create a containerised pipeline from a given set of inputs to "
            "generate specified derivatives")

    @classmethod
    def construct_parser(cls, parser):
        parser.add_argument('image_name',
                            help=("The name of the Docker image to generate"
                                  "(with org separated by '/')"))
        parser.add_argument('out_file',
                            help="The path to save the Dockerfile to")
        parser.add_argument('--maintainer', '-m',
                            help="Maintainer of the pipeline")
        parser.add_argument('--description', '-d', default=None,
                            help="A description of what the pipeline does")
        parser.add_argument('--no-build', '-n', action='store_true',
                            default=False,
                            help=("Don't build the generated Dockerfile"))
        parser.add_argument('--push', '-p', default=None, metavar='REGISTRY',
                            help=("Upload the generated dockerfile (requires "
                                  "'--build') to the provided registry"))
        

    @classmethod
    def run(cls, args):

        # requirements = defaultdict(set)
        # for pipeline in pipeline_stack:
        #     for node in pipeline.nodes:
        #         for ver in node.requirements:
        #             requirements[ver.requirement].add(ver)

        # # Add requirements needed for file conversions
        # requirements[mrtrix_req].add(mrtrix_req.v('3.0rc3'))
        # requirements[dcm2niix_req].add(dcm2niix_req.v('1.0.20200331'))

        versions = []
        for req, req_versions in requirements.items():
            min_ver, max_ver = req.reconcile(req_versions)
            if req.max_neurodocker_version is not None:
                if req.max_neurodocker_version < min_ver:
                    raise ArcanaRequirementVersionsError(
                        "Minium required version '{}' is greater than max "
                        "neurodocker version ({})".format(
                            min_ver, req.max_neurodocker_version))
                max_ver = req.max_neurodocker_version
            if max_ver is not None:
                version = max_ver
            else:
                # This is where it would be good to know store the latest
                # supported version of each tool so we could use the newest
                # version instead of the min version (i.e. the only one we 
                # know about here
                version = min_ver
            versions.append(version)

        labels = {}

        if args.maintainer:
            labels["maintainer"] = args.maintainer

        parameters = [c.args[0] for c in mock_analysis.parameter.mock_calls]

        if args.xnat:
            if args.upload:
                docker_index = args.upload
            else:
                docker_index = "https://index.docker.io/v1/"
            cmd = make_command_json(
                args.image_name, analysis_class, inputs, args.sinks,
                parameters, args.description, docker_index=docker_index)
            print(json.dumps(cmd, indent=2))
            cmd_label = json.dumps(cmd).replace('"', r'\"').replace('$', r'\$')
            labels['org.nrg.commands'] = '[{' + cmd_label + '}]'

        instructions = [
            ["base", "debian:stretch"],
            ["install", ["git", "vim"]]]

        for version in versions:
            props = {'version': str(version)}
            if version.requirement.neurodocker_method:
                props['method'] = version.requirement.neurodocker_method
            instructions.append(
                [version.requirement.neurodocker_name, props])

        if labels:
            instructions.append(["label", labels])

        instructions.append(
            ["miniconda", {
                "create_env": "arcana2",
                "conda_install": [
                    "python=3.8",
                    "numpy",
                    "traits"],
                "pip_install": [
                    "git+https://github.com/australian-imaging-service/arcana2.git@xnat-cs"]}])

        neurodocker_specs = {
            "pkg_manager": "apt",
            "instructions": instructions}

        dockerfile = nd.Dockerfile(neurodocker_specs).render()

        if args.out_dir is None:
            out_dir = tempfile.mkdtemp()
        else:
            out_dir = args.out_dir
        out_dir = Path(out_dir)

        out_dir.mkdir(exist_ok=True, parents=True)
        out_file = out_dir / 'Dockerfile'
        with open(str(out_file), 'w') as f:
            f.write(dockerfile)

        logger.info("Dockerfile generated at %s", out_file)

        if args.build:
            sp.check_call('docker build -t {} .'.format(args.image_name),
                          cwd=out_dir, shell=True)

        if args.upload:
            sp.check_call('docker push {}'.format(args.image_name))


def make_command_json(image_name, analysis_cls, inputs, outputs,
                      parameters, desc, frequency=Clinical.session,
                      docker_index="https://index.docker.io/v1/"):

    if frequency != Clinical.session:
        raise NotImplementedError(
            "Support for frequencies other than '{}' haven't been "
            "implemented yet".format(frequency))
    try:
        analysis_name, version = image_name.split('/')[1].split(':')
    except (IndexError, ValueError):
        raise ArcanaUsageError(
            "The Docker organisation and tag needs to be provided as part "
            "of the image, e.g. australianimagingservice/dwiqa:0.1")

    cmd_inputs = []
    input_names = []
    for inpt in inputs:
        input_name = inpt if isinstance(inpt, str) else inpt[0]
        input_names.append(input_name)
        spec = analysis_cls.data_spec(input_name)
        desc = spec.desc if spec.desc else ""
        if spec.is_file_group:
            desc = ("Scan match: {} [SCAN_TYPE [ORDER [TAG=VALUE, ...]]]"
                    .format(desc))
        else:
            desc = "Field match: {} [FIELD_NAME]".format(desc)
        cmd_inputs.append({
            "name": input_name,
            "description": desc,
            "type": "string",
            "default-value": "",
            "required": True,
            "user-settable": True,
            "replacement-key": "#{}_INPUT#".format(input_name.upper())})

    for param in parameters:
        spec = analysis_cls.param_spec(param)
        desc = "Parameter: " + spec.desc
        if spec.choices:
            desc += " (choices: {})".format(','.join(spec.choices))

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


    cmdline = (
        "arcana derive /input {cls} {name} {derivs} {inputs} {params}"
        " --scratch /work --repository xnat_cs #PROJECT_URI#"
        .format(
            cls='.'.join((analysis_cls.__module__, analysis_cls.__name__)),
            name=analysis_name,
            derivs=' '.join(outputs),
            inputs=' '.join('-i {} #{}_INPUT#'.format(i, i.upper())
                            for i in input_names),
            params=' '.join('-p {} #{}_PARAM#'.format(p, p.upper())
                            for p in parameters)))

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
        cmdline += "#SESSION_ID# --session_ids #SESSION_ID# "

    return {
        "name": analysis_name,
        "description": desc,
        "label": analysis_name,
        "version": version,
        "schema-version": "1.0",
        "image": image_name,
        "index": docker_index,
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
                "name": analysis_name,
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


COMMAND_INPUT_TYPES = {
    bool: 'bool',
    str: 'string',
    int: 'number',
    float: 'number'}
