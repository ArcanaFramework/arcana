from pathlib import Path
import logging
import yaml
import pkgutil
import importlib
import sys
from traceback import format_exc
import click
import docker.errors
from arcana.data.stores.xnat.cs import XnatViaCS
from arcana.core.utils import resolve_class
from arcana.core.cli import cli

DOCKER_REGISTRY = 'docker.io'

@cli.group()
def wrap():
    pass


spec_help = """
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
            on (can be either per- 'dataset' or 'session' at the moment)
"""


@wrap.command(name='build-all', help=f"""Build all wrapper images specified
in sub-modules under the package path.

package_path
    The file-system path containing the image specifications: Python dictionaries
    named `spec` in sub-modules with the following keys:{spec_help}""")
@click.argument('package_path')
@click.option('--registry', default=DOCKER_REGISTRY,
              help="The Docker registry to deploy the pipeline to")
@click.option('--loglevel', default='warning',
              help="The level to display logs at")
@click.option('--build_dir', default=None, type=Path,
              help="Specify the directory to build the Docker image in")
@click.option('--docs', '-d', default=None, type=Path,
              help="Create markdown documents in output path")
def build_all(package_path, registry, loglevel, build_dir, docs):
    """Creates a Docker image that wraps a Pydra task so that it can
    be run in XNAT's container service, then pushes it to AIS's Docker Hub
    organisation for deployment
    """

    logging.basicConfig(level=getattr(logging, loglevel.upper()))

    org_name = Path(package_path).name

    if docs:
        docs.mkdir(parents=True, exist_ok=True)

    built_images = []
    for mod_name, spec in extract_wrapper_specs(package_path).items():
        built_images.append(
            XnatViaCS.create_wrapper_image(
                pkg_name=mod_name,
                docker_org=org_name,
                build_dir=build_dir / mod_name,
                docker_registry=registry,
                **spec))
        logging.info("Successfully built %s wrapper", mod_name)
        if docs:
            create_doc(spec, docs, mod_name)

    click.echo('\n'.join(built_images))


@wrap.command(help="""Build a wrapper image specified in a module

module_path
    The file system path to the module to build""")
@click.argument('module_path')
def build(module_path):
    pass


@click.command(name='inspect-docker-exec', 
               help="""Extract the executable from a Docker image""")
@click.argument('image_tag')
def inspect_docker_exec(image_tag):
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

    click.echo(executable)


def extract_wrapper_specs(package_path):
    package_path = Path(package_path)
    def error_msg(name):
        exception = format_exc()
        logging.error(
            f"Error attempting to import {name} module.\n\n{exception}")

    # Ensure base package is importable
    sys.path.append(str(package_path))

    wrapper_specs = {}
    for _, mod_path, ispkg in pkgutil.walk_packages([package_path],
                                                    onerror=error_msg):
        if not ispkg:
            mod = importlib.import_module(mod_path)
            try:
                wrapper_specs[mod.__name__] = mod.spec
            except AttributeError:
                logging.warning("No `spec` dictionary found in %s, skipping",
                                mod_path)

    return wrapper_specs


def create_doc(spec, doc_dir, pkg_name):

    header = {
        "title": spec["package_name"],
        "weight": 10,
        "source_file": pkg_name,
    }

    task = resolve_class(spec['task_location'])

    with open(doc_dir / pkg_name, "w") as f:
        f.write("---\n")
        yaml.dump(header, f)
        f.write("\n---\n\n")

        f.write(f'{spec["description"]}\n\n')

        f.write("### Info\n")
        tbl_info = MarkdownTable(f, "Key", "Value")
        if "version" in spec:
            tbl_info.write_row("Version", spec["version"])
        if "pkg_version" in spec:
            tbl_info.write_row("App version", spec["pkg_version"])
        if task.image:
            tbl_info.write_row("Image", escaped_md(task.image))
        if "base_image" in spec and task.image != spec["base_image"]:
            tbl_info.write_row("Base image", escaped_md(spec["base_image"]))
        if "maintainer" in spec:
            tbl_info.write_row("Maintainer", spec["maintainer"])
        if "info_url" in spec:
            tbl_info.write_row("Info URL", spec["info_url"])
        if "frequency" in spec:
            tbl_info.write_row("Frequency", spec["frequency"].name.title())

        f.write("\n")

        f.write("### Inputs\n")
        tbl_inputs = MarkdownTable(f, "Name", "Bids path", "Data type")
        for x in task.inputs:
            name, dtype, path = x
            tbl_inputs.write_row(escaped_md(name), escaped_md(path), escaped_md(dtype))
        f.write("\n")

        f.write("### Outputs\n")
        tbl_outputs = MarkdownTable(f, "Name", "Data type")
        for x in task.outputs:
            name, dtype, path = x
            tbl_outputs.write_row(escaped_md(name), escaped_md(dtype))
        f.write("\n")

        f.write("### Parameters\n")
        if not spec.get("parameters", None):
            f.write("None\n")
        else:
            tbl_params = MarkdownTable(f, "Name", "Data type")
            for param in spec["parameters"]:
                tbl_params.write_row("Todo", "Todo", "Todo")
        f.write("\n")


def escaped_md(value: str) -> str:
    if not value:
        return ""
    return f"`{value}`"


class MarkdownTable:
    def __init__(self, f, *headers: str) -> None:
        self.headers = tuple(headers)

        self.f = f
        self._write_header()

    def _write_header(self):
        self.write_row(*self.headers)
        self.write_row(*("-" * len(x) for x in self.headers))

    def write_row(self, *cols: str):
        cols = list(cols)
        if len(cols) > len(self.headers):
            raise ValueError(
                f"More entries in row ({len(cols)} than columns ({len(self.headers)})")

        # pad empty column entries if there's not enough
        cols += [""] * (len(self.headers) - len(cols))

        # TODO handle new lines in col
        self.f.write("|" + "|".join(col.replace("|", "\\|") for col in cols) + "|\n")

