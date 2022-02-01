# import tempfile
# import sys
# from pathlib import Path
# from logging import getLogger
# import docker
# from arcana.core.utils import resolve_class
# from arcana.data.stores.xnat.cs import XnatViaCS
# from arcana.data.dimensions.clinical import Clinical
# from arcana.core.entrypoint import BaseCmd
# from arcana.core.utils import resolve_datatype, DOCKER_HUB


# logger = getLogger('arcana')


# class Wrap4XnatCmd(BaseCmd):

#     cmd_name = 'wrap4xnat'

#     desc = ("Create a containerised pipeline from a given set of inputs to "
#             "generate specified derivatives")

#     @classmethod
#     def construct_parser(cls, parser):
#         parser.add_argument('interface',
#                             help=("The location (on Python path) of the Pydra "
#                                   "interface to wrap"))
#         parser.add_argument('image_name', metavar='IMAGE',
#                             help=("The name of the Docker image, preceded by "
#                                   "the registry it will be stored"))
#         parser.add_argument('--input', '-i', action='append', default=[],
#                             nargs='+',
#                             help="Inputs to be used by the app (NAME DATATYPE [FREQUENCY])")
#         parser.add_argument('--output', '-o', action='append', default=[],
#                             nargs=2, metavar=('NAME', 'DATATYPE'),
#                             help="Outputs of the app to stored back in XNAT")
#         parser.add_argument('--name', '-n', type=str, default=None,
#                             help="A name for the pipeline")
#         parser.add_argument('--parameter', '-p', metavar='NAME', action='append',
#                             help=("Fixed parameters of the Pydra workflow to "
#                                   "expose to the container service"))
#         parser.add_argument('--requirement', '-r', nargs='+', action='append',
#                             help=("Software requirements to be added to the "
#                                   "the docker image using Neurodocker. "
#                                   "Neurodocker requirement name, followed by "
#                                   "optional version and installation "
#                                   "method args (see Neurodocker docs). Use "
#                                   "'.' to skip version arg and use the latest "
#                                   "available"))
#         parser.add_argument('--package', '-k', action='append',
#                             help="PyPI packages to be installed in the env")
#         parser.add_argument('--frequency', default='clinical.Clinical.session',
#                             help=("Whether the resultant container runs "
#                                   "against a session or a whole dataset "
#                                   "(i.e. project). Can be one of either "
#                                   "'session' or 'dataset'"))
#         parser.add_argument('--registry', default=DOCKER_HUB,
#                             help="The registry the image will be installed in")
#         parser.add_argument('--build_dir', default=None,
#                             help="The directory to build the dockerfile in")
#         parser.add_argument('--maintainer', '-m', type=str, default=None,
#                             help="Maintainer of the pipeline")
#         parser.add_argument('--description', '-d', default=None,
#                             help="A description of what the pipeline does")
#         parser.add_argument('--build', default=False, action='store_true',
#                             help=("Build the generated Dockerfile"))
#         parser.add_argument('--install', default=False, action='store_true',
#                             help=("Install the built docker image in the "
#                                   "specified registry (implies '--build')"))

#     @classmethod
#     def run(cls, args):

#         frequency = cls.parse_frequency(args)
        
#         inputs = cls.parse_input_args(args, frequency)
#         outputs = cls.parse_output_args(args)

#         extra_labels = {'arcana-wrap4xnat-cmd': ' '.join(sys.argv)}
#         pydra_task = cls.parse_interface(args)

        
#         build_dir = Path(tempfile.mkdtemp()
#                          if args.build_dir is None else args.build_dir)

#         image_name = cls.parse_image_name(args)

#         pipeline_name = args.name if args.name else pydra_task.name

#         # Generate "command JSON" to embed in container to let XNAT know how
#         # to run the pipeline
#         json_config = XnatViaCS.generate_json_config(
#             pipeline_name,
#             pydra_task,
#             inputs=inputs,
#             outputs=outputs,
#             description=args.description,
#             parameters=args.parameter,
#             frequency=frequency,
#             registry=args.registry)

#         # Generate dockerfile
#         dockerfile = XnatViaCS.generate_dockerfile(
#             pydra_task, json_config, image_name, args.maintainer,
#             args.requirement, args.package, build_dir=build_dir,
#             extra_labels=extra_labels)

#         if args.build or args.install:
#             cls.build(image_name, build_dir=build_dir)

#         if args.install:
#             cls.install(dockerfile, image_name, args.registry,
#                         build_dir=build_dir)
#         else:
#             return dockerfile

#     @classmethod
#     def parse_interface(cls, args):
#         return resolve_class(args.interface)()

#     @classmethod
#     def parse_frequency(cls, args):
#         return Clinical[args.frequency]

#     @classmethod
#     def parse_image_name(cls, args):
#         return (args.image_name + ':latest' if ':' not in args.image_name
#                 else args.image_name)

#     @classmethod
#     def build(cls, image_tag, build_dir):

#         dc = docker.from_env()

#         logger.info("Building image in %s", str(build_dir))

#         dc.images.build(path=str(build_dir), tag=image_tag)        

#     @classmethod
#     def install(cls, image_tag, registry, build_dir):
#         # Build and upload docker image

#         dc = docker.from_env()

#         image_path = f'{registry}/{image_tag}'
        
#         logger.info("Uploading %s image to %s", image_tag, registry)

#         dc.images.push(image_path)

#     @classmethod
#     def parse_input_args(cls, args, default_frequency):
#         for inpt in args.input:
#             name, required_datatype_name = inpt[:2]
#             frequency = inpt[2] if len(inpt) > 2 else default_frequency
#             required_datatype = resolve_datatype(required_datatype_name)
#             yield XnatViaCS.InputArg(name, required_datatype, frequency)

#     @classmethod
#     def parse_output_args(cls, args):
#         for output in args.output:
#             name, datatype_name_name = output
#             produced_datatype = resolve_datatype(datatype_name_name)
#             yield XnatViaCS.OutputArg(name, produced_datatype)


from pathlib import Path
import logging
import tempfile
import shutil
from importlib import import_module
import click
import docker.errors
from arcana.data.stores.xnat.cs import XnatViaCS
from arcana.core.utils import get_pkg_name

DOCKER_REGISTRY = 'docker.io'
AIS_DOCKER_ORG = 'australianimagingservice'


@click.command(help="""The relative path to a module in the 'australianimagingservice'
        package containing a
        member called `task`, a Pydra task or function that takes a name and
        inputs and returns a workflow, and another called  `spec`,
        a dictionary with the following items:

            name : str\n
                Name of the pipeline\n
            pydra_task : pydra.task\n
                The pydra task to be wrapped for the XNAT container service\n
            inputs : list[XnatViaCS.InputArg or tuple]\n
                Inputs to be provided to the container\n
            outputs : list[XnatViaCS.OutputArg or tuple]\n
                Outputs from the container\n
            parameters : list[str]\n
                Parameters to be exposed in the CS command\n
            description : str\n
                User-facing description of the pipeline\n
            version : str\n
                Version string for the wrapped pipeline\n
            packages : list[tuple[str, str]]\n
                Name and version of the Neurodocker requirements to add to the image\n
            python_packages : list[tuple[str, str]]\n
                Name and version of the Python PyPI packages to add to the image\n
            maintainer : str\n
                The name and email of the developer creating the wrapper (i.e. you)\n
            info_url : str\n
                URI explaining in detail what the pipeline does\n
            frequency : Clinical\n
                The frequency of the data nodes on which the pipeline operates
                on (can be either per- 'dataset' or 'session' at the moment)\n""")
@click.argument('module_path')
@click.option('--registry', default=DOCKER_REGISTRY,
              help="The Docker registry to deploy the pipeline to")
@click.option('--loglevel', default='info',
              help="The level to display logs at")
@click.option('--build_dir', default=None, type=str,
              help="Specify the directory to build the Docker image in")
def deploy(module_path, registry, loglevel, build_dir):
    """Creates a Docker image that wraps a Pydra task so that it can
    be run in XNAT's container service, then pushes it to AIS's Docker Hub
    organisation for deployment
    """

    logging.basicConfig(level=getattr(logging, loglevel.upper()))

    full_module_path = 'australianimagingservice.' + module_path
    module = import_module(full_module_path)

    if build_dir is None:
        build_dir = tempfile.mkdtemp()
    build_dir = Path(build_dir)
    build_dir.mkdir(exist_ok=True)

    pkg_name = module.spec['package_name']
    version = module.spec['version']

    image_tag = f"{AIS_DOCKER_ORG}/{pkg_name.lower().replace('-', '_')}:{version}"

    frequency = module.spec['frequency']

    python_packages = module.spec.get('python_packages', [])

    xnat_commands = []
    for cmd_spec in module.spec['commands']:

        cmd_name = cmd_spec.get('name', pkg_name)
        cmd_desc = cmd_spec.get('description', module.spec['description'])

        pydra_task = cmd_spec['pydra_task']
        if ':' not in pydra_task:
            # Default to the module that the spec is defined in
            pydra_task = full_module_path + ':' + pydra_task
            task_module = full_module_path
        else:
            task_module = pydra_task.split(':')[0]

        python_packages.append(get_pkg_name(task_module))

        xnat_commands.append(XnatViaCS.generate_xnat_command(
            pipeline_name=cmd_name,
            task_location=pydra_task,
            image_tag=image_tag,
            inputs=cmd_spec['inputs'],
            outputs=cmd_spec['outputs'],
            parameters=cmd_spec['parameters'],
            description=cmd_desc,
            version=version,
            registry=registry,
            frequency=frequency,
            info_url=module.spec['info_url']))

    build_dir = XnatViaCS.generate_dockerfile(
        xnat_commands=xnat_commands,
        maintainer=module.spec['maintainer'],
        build_dir=build_dir,
        base_image=module.spec.get('base_image'),
        packages=module.spec.get('packages'),
        python_packages=python_packages,
        package_manager=module.spec.get('package_manager'))

    dc = docker.from_env()
    try:
        dc.images.build(path=str(build_dir), tag=image_tag)
    except docker.errors.BuildError as e:
        logging.error(f"Error building docker file in {build_dir}")
        logging.error('\n'.join(l.get('stream', '') for l in e.build_log))
        raise

    logging.info("Built docker image %s", pkg_name)

    dc.images.push(image_tag)

    logging.info("Pushed %s pipeline to %s Docker Hub organsation",
                 pkg_name, DOCKER_REGISTRY)



@click.command(help="""Extract the executable from a Docker image""")
@click.argument('image_tag')
def detect_docker_executable(image_tag):
    """Pulls a given Docker image tag and inspects the image to get its
    entrypoint/cmd

    Parameters
    ----------
    image_tag : str
        Docker image tag

    Returns
    -------
    str
        The entrypoint or default command of the Docker image
    """
    dc = docker.from_env()

    dc.images.pull(image_tag)

    image_attrs = dc.api.inspect_image(image_tag)['Config']

    executable = image_attrs['Entrypoint']
    if executable is None:
        executable = image_attrs['Cmd']

    print(executable)