import tempfile
import sys
from pathlib import Path
from logging import getLogger
import docker
from arcana2.core.utils import resolve_class
from arcana2.data.stores.xnat.cs import XnatViaCS
from arcana2.data.spaces.clinical import Clinical
from arcana2.core.entrypoint import BaseCmd
from arcana2.core.utils import resolve_datatype, DOCKER_HUB


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
        parser.add_argument('--input', '-i', action='append', default=[],
                            nargs='+',
                            help="Inputs to be used by the app (NAME DATATYPE [FREQUENCY])")
        parser.add_argument('--output', '-o', action='append', default=[],
                            nargs=2, metavar=('NAME', 'DATATYPE'),
                            help="Outputs of the app to stored back in XNAT")
        parser.add_argument('--name', '-n', type=str, default=None,
                            help="A name for the pipeline")
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
        parser.add_argument('--frequency', default='clinical.Clinical.session',
                            help=("Whether the resultant container runs "
                                  "against a session or a whole dataset "
                                  "(i.e. project). Can be one of either "
                                  "'session' or 'dataset'"))
        parser.add_argument('--registry', default=DOCKER_HUB,
                            help="The registry the image will be installed in")
        parser.add_argument('--build_dir', default=None,
                            help="The directory to build the dockerfile in")
        parser.add_argument('--maintainer', '-m', type=str, default=None,
                            help="Maintainer of the pipeline")
        parser.add_argument('--description', '-d', default=None,
                            help="A description of what the pipeline does")
        parser.add_argument('--build', default=False, action='store_true',
                            help=("Build the generated Dockerfile"))
        parser.add_argument('--install', default=False, action='store_true',
                            help=("Install the built docker image in the "
                                  "specified registry (implies '--build')"))

    @classmethod
    def run(cls, args):

        frequency = cls.parse_frequency(args)
        
        inputs = cls.parse_input_args(args, frequency)
        outputs = cls.parse_output_args(args)

        extra_labels = {'arcana-wrap4xnat-cmd': ' '.join(sys.argv)}
        pydra_task = cls.parse_interface(args)

        
        build_dir = Path(tempfile.mkdtemp()
                         if args.build_dir is None else args.build_dir)

        image_name = cls.parse_image_name(args)

        pipeline_name = args.name if args.name else pydra_task.name

        # Generate "command JSON" to embed in container to let XNAT know how
        # to run the pipeline
        json_config = XnatViaCS.generate_json_config(
            pipeline_name,
            pydra_task,
            inputs=inputs,
            outputs=outputs,
            description=args.description,
            parameters=args.parameter,
            frequency=frequency,
            registry=args.registry)

        # Generate dockerfile
        dockerfile = XnatViaCS.generate_dockerfile(
            pydra_task, json_config, image_name, args.maintainer,
            args.requirement, args.package, build_dir=build_dir,
            extra_labels=extra_labels)

        if args.build or args.install:
            cls.build(image_name, build_dir=build_dir)

        if args.install:
            cls.install(dockerfile, image_name, args.registry,
                        build_dir=build_dir)
        else:
            return dockerfile

    @classmethod
    def parse_interface(cls, args):
        return resolve_class(args.interface)()

    @classmethod
    def parse_frequency(cls, args):
        return Clinical[args.frequency]

    @classmethod
    def parse_image_name(cls, args):
        return (args.image_name + ':latest' if ':' not in args.image_name
                else args.image_name)

    @classmethod
    def build(cls, image_tag, build_dir):

        dc = docker.from_env()

        logger.info("Building image in %s", str(build_dir))

        dc.images.build(path=str(build_dir), tag=image_tag)        

    @classmethod
    def install(cls, image_tag, registry, build_dir):
        # Build and upload docker image

        dc = docker.from_env()

        image_path = f'{registry}/{image_tag}'
        
        logger.info("Uploading %s image to %s", image_tag, registry)

        dc.images.push(image_path)

    @classmethod
    def parse_input_args(cls, args, default_frequency):
        for inpt in args.input:
            name, required_datatype_name = inpt[:2]
            frequency = inpt[2] if len(inpt) > 2 else default_frequency
            required_datatype = resolve_datatype(required_datatype_name)
            yield XnatViaCS.InputArg(name, required_datatype, frequency)

    @classmethod
    def parse_output_args(cls, args):
        for output in args.output:
            name, datatype_name_name = output
            produced_datatype = resolve_datatype(datatype_name_name)
            yield XnatViaCS.OutputArg(name, produced_datatype)
