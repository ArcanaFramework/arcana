from pathlib import Path
import logging
import click
import docker.errors
from arcana.data.stores.xnat.cs import XnatViaCS
from arcana.core.utils import extract_wrapper_specs

DOCKER_REGISTRY = 'docker.io'


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
@click.argument('package_path')
@click.option('--registry', default=DOCKER_REGISTRY,
              help="The Docker registry to deploy the pipeline to")
@click.option('--loglevel', default='warning',
              help="The level to display logs at")
@click.option('--build_dir', default=None, type=Path,
              help="Specify the directory to build the Docker image in")
def wrap4xnat(package_path, registry, loglevel, build_dir):
    """Creates a Docker image that wraps a Pydra task so that it can
    be run in XNAT's container service, then pushes it to AIS's Docker Hub
    organisation for deployment
    """

    logging.basicConfig(level=getattr(logging, loglevel.upper()))

    org_name = Path(package_path).name

    built_images = []
    for mod_name, spec in extract_wrapper_specs(package_path).items():
        built_images.append(
            XnatViaCS.create_wrapper_image(
                pkg_name=mod_name[len(org_name) + 1:],
                docker_org=org_name,
                build_dir=build_dir / mod_name,
                docker_registry=registry,
                **spec))
        logging.info("Successfully built %s wrapper", mod_name)

    print('\n'.join(built_images))


@click.command(help="""Extract the executable from a Docker image""")
@click.argument('image_tag')
def extract_exec(image_tag):
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