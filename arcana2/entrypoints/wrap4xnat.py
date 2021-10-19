import tempfile
import sys
from pathlib import Path
from logging import getLogger
import docker
from arcana2.core.utils import resolve_class
from arcana2.repositories.xnat.container_service import (
    generate_dockerfile, InputArg, OutputArg)
from .run import RunCmd
from arcana2.core.entrypoint import BaseCmd
from arcana2.core.utils import resolve_datatype


logger = getLogger('arcana')


class Wrap4XnatCmd(BaseCmd):

    cmd_name = 'wrap4xnat'

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
        parser.add_argument('--frequency', default='session',
                            help=("Whether the resultant container runs "
                                  "against a session or a whole dataset "
                                  "(i.e. project). Can be one of either "
                                  "'session' or 'dataset'"))
        parser.add_argument('--dry_run', action='store_true', default=False,
                            help=("Don't build the generated Dockerfile"))

    @classmethod
    def run(cls, args):
        inputs = RunCmd.parse_input_args(args)
        outputs = RunCmd.parse_output_args(args)

        extra_labels = {'arcana-wrap4xnat-cmd': ' '.join(sys.argv)}
        pydra_interface = resolve_class(args.interface_name)

        # Generate dockerfile
        dockerfile = generate_dockerfile(
            pydra_interface, args.image_name, args.tag, inputs, outputs,
            args.parameter, args.requirement, args.package, args.registry,
            args.description, maintainer=None, extra_labels=extra_labels)

        # Save generated dockerfile to file
        out_file = Path(args.out_file)
        out_file.parent.mkdir(exist_ok=True, parents=True)
        with open(str(out_file), 'w') as f:
            f.write(dockerfile)
        logger.info("Dockerfile generated at %s", out_file)

        # Build and upload docker image
        if not args.dry_run:
            if args.build_dir:
                build_dir = Path(args.build_dir)
            else:
                build_dir = Path(tempfile.mkdtemp())
            logger.info("Building dockerfile at %s dir", str(build_dir))

            dc = docker.from_env()
            image, _ = dc.images.build(path=str(build_dir),
                                        tag=args.image_path)
            image.push(args.registry)

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
