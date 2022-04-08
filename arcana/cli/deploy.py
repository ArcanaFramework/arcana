import typing as ty
import logging
from pathlib import Path
import click
import docker.errors
from arcana.core.cli import cli
from arcana.core.utils import resolve_class
from arcana.core.deploy.utils import load_yaml_spec, walk_spec_paths
from arcana.core.deploy.docs import create_doc
from arcana.core.utils import package_from_module, pydra_asdict
from arcana.deploy.medimage.xnat import build_cs_image


DOCKER_REGISTRY = 'docker.io'


@cli.group()
def deploy():
    pass


@deploy.group()
def xnat():
    pass


@xnat.command(help="""Build a wrapper image specified in a module

SPEC_PATH is the file system path to the specification to build, or directory
containing multiple specifications

DOCKER_ORG is the Docker organisation to build the """)
@click.argument('spec_path', type=click.Path(exists=True, path_type=Path))
@click.argument('docker_org', type=str)
@click.option('--registry', name='docker_registry', default=DOCKER_REGISTRY,
              help="The Docker registry to deploy the pipeline to")
@click.option('--build_dir', default=None, type=Path,
              help="Specify the directory to build the Docker image in")
@click.option('--loglevel', default='warning',
              help="The level to display logs at")
def build(spec_path, docker_org, docker_registry, loglevel, build_dir):

    logging.basicConfig(level=getattr(logging, loglevel.upper()))

    for spath in walk_spec_paths(spec_path):
        spec = load_yaml_spec(spath)

        # Make image tag
        pkg_name = spec['pkg_name'].lower().replace('-', '_')
        tag = '-'.join(spath.relative_to(spec_path).parents.parts + [pkg_name])
        image_version = str(spec['pkg_version'])
        if 'wrapper_version' in spec:
            image_version += f"-{spec['wrapper_version']}"
        image_tag = f"{docker_registry}/{docker_org}/{tag}:{image_version}" 

        build_cs_image(build_dir=build_dir, docker_org=docker_org,
                       docker_registry=docker_registry, **spec)
        logging.info("Successfully built %s wrapper", image_tag)



@deploy.command(name='test', help="""Test container images defined by YAML
specs

Arguments
---------
module_path
    The file system path to the module to build""")
@click.argument('spec_path', type=click.Path(exists=True, path_type=Path))
def test(spec_path):
    raise NotImplementedError



@deploy.command(name='docs', help="""Build docs for one or more yaml wrappers

SPEC_PATH is the path of a YAML spec file or directory containing one or more such files.

The generated documentation will be saved to OUTPUT.
""")
@click.argument('spec_path', type=click.Path(exists=True, path_type=Path))
@click.argument('output', type=click.Path(path_type=Path))
@click.option('--flatten/--no-flatten', default=False)
@click.option('--loglevel', default='warning',
              help="The level to display logs at")
def build_docs(spec_path, output, flatten, loglevel):

    logging.basicConfig(level=getattr(logging, loglevel.upper()))

    output.mkdir(parents=True, exist_ok=True)

    for spath in walk_spec_paths(spec_path):
        spec = load_yaml_spec(spath)
        mod_name = spec['_module_name']
        create_doc(spec, output, mod_name, flatten=flatten)
        logging.info("Successfully created docs for %s", mod_name)



@deploy.command(name='required-packages',
                help="""Detect the Python packages required to run the 
specified workflows and return them and their versions""")
@click.argument('workflow_locations', nargs=-1)
def required_packages(workflow_locations):

    required_modules = set()
    for workflow_location in workflow_locations:
        workflow = resolve_class(workflow_location)
        pydra_asdict(workflow, required_modules)

    for pkg in package_from_module(required_modules):
        click.echo(f"{pkg.key}=={pkg.version}")



@deploy.command(name='inspect-docker-exec',
               help="""Extract the executable from a Docker image""")
@click.argument('image_tag', type=str)
def inspect_docker_exec(image_tag):
    """Pulls a given Docker image tag and inspects the image to get its
entrypoint/cmd

IMAGE_TAG is the tag of the Docker image to inspect"""
    dc = docker.from_env()

    dc.images.pull(image_tag)

    image_attrs = dc.api.inspect_image(image_tag)['Config']

    executable = image_attrs['Entrypoint']
    if executable is None:
        executable = image_attrs['Cmd']

    click.echo(executable)
    