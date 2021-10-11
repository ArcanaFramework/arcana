from argparse import ArgumentParser
import neurodocker as nd
from arcana2.core.utils import set_loggers, resolve_class


class Wrap4XnatCSCmd():

    desc = ("Create a containerised pipeline from a given set of inputs to "
            "generate specified derivatives")

    @classmethod
    def construct_parser(cls, parser):
        parser.add_argument('image_name',
                            help=("The name of the Docker image to generate"
                                  "(with org separated by '/')"))
        parser.add_argument('--maintainer', '-m',
                            help="Maintainer of the pipeline")
        parser.add_argument('--description', '-d', default="Not provided",
                            help="A description of what the pipeline does")
        parser.add_argument('--build', '-b', action='store_true',
                            default=False,
                            help="Build the generated Dockerfile")
        parser.add_argument('--upload', '-u', default=None,
                            help=("Upload the generated dockerfile (requires "
                                  "'-b') to the provided docker index"))
        parser.add_argument('--out_dir', '-o', default=None,
                            help=("The directory to save the Dockerfile to. "
                                  "If not provided then a temp dir will be "
                                  "used instead"))

    @classmethod
    def run(cls, args):

        set_loggers([('arcana', 'INFO')])

        analysis_class = resolve_class(args.analysis_class)

        inputs = []
        for inpt in args.input:
            if len(inpt) == 1:
                inputs.append(inpt[0])
            elif len(inpt) == 2:
                name, var = inpt
                spec = analysis_class.data_spec(name)
                if spec.is_fileset:
                    inputs.append((name, Fileset.from_path(var)))
                else:
                    inputs.append((name, Field(name, value=var)))
            else:
                raise BananaUsageError(
                    "Inputs should either a single name or a "
                    "name-(filename|value) pair, not ({})".format(inpt))

        switches = {}
        if args.switch:
            for name, val in args.switch:
                spec = analysis_class.param_spec(name)
                switches[name] = spec.datatype(val)

        mock_analysis = analysis_class.mock(inputs=inputs,
                                            switches=switches)

        pipeline_stack = mock_analysis.pipeline_stack(*args.sinks)

        requirements = defaultdict(set)
        for pipeline in pipeline_stack:
            for node in pipeline.nodes:
                for ver in node.requirements:
                    requirements[ver.requirement].add(ver)

        # Add requirements needed for file conversions
        requirements[mrtrix_req].add(mrtrix_req.v('3.0rc3'))
        requirements[dcm2niix_req].add(dcm2niix_req.v('1.0.20200331'))

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
            cmd = XnatCSRepo.command_json(
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
                    "git+https://github.com/MonashBI/arcana2.git@xnat-cs",
                    "git+https://github.com/MonashBI/banana.git@xnat-cs"]}])

        neurodocker_specs = {
            "pkg_manager": "apt",
            "instructions": instructions}

        dockerfile = neurodocker.Dockerfile(neurodocker_specs).render()

        if args.out_dir is None:
            out_dir = tempfile.mkdtemp()
        else:
            out_dir = args.out_dir

        os.makedirs(out_dir, exist_ok=True)
        out_file = os.path.join(out_dir, 'Dockerfile')
        with open(out_file, 'w') as f:
            f.write(dockerfile)

        logger.info("Dockerfile generated at %s", out_file)

        if args.build:
            sp.check_call('docker build -t {} .'.format(args.image_name),
                          cwd=out_dir, shell=True)

        if args.upload:
            sp.check_call('docker push {}'.format(args.image_name))
