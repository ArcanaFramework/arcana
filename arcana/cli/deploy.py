import logging
import os
from pathlib import Path
import click
import docker.errors
import yaml
from arcana.core.cli import cli
from arcana.data.spaces.medimage import Clinical
from arcana.data.stores.medimage import XnatViaCS


DOCKER_REGISTRY = 'docker.io'


@cli.group()
def deploy():
    pass


@deploy.group()
def xnat():
    pass


@deploy.command(help="""Build a wrapper image specified in a module

module_path - the file system path to the module to build""")
@click.argument('module_path')
def build(module_path):
    raise NotImplementedError


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


@deploy.command(name='build-all', help=f"""Build all wrapper images specified
in sub-modules under the package path.

Arguments
---------
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
def build_all(package_path, registry, loglevel, build_dir):
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
                pkg_name=mod_name,
                docker_org=org_name,
                build_dir=build_dir / mod_name,
                docker_registry=registry,
                **spec))
        logging.info("Successfully built %s wrapper", mod_name)

    click.echo('\n'.join(built_images))


@deploy.command(name='docs', help="""Build docs for one or more yaml wrappers

SPEC is the path of a YAML spec file or directory containing one or more such files.

The generated documentation will be saved to OUTPUT.
""")
@click.argument('spec', type=click.Path(exists=True, path_type=Path))
@click.argument('output', type=click.Path(path_type=Path))
@click.option('--flatten/--no-flatten', default=False)
def build_docs(spec, output, flatten):
    output.mkdir(parents=True, exist_ok=True)

    if spec.resolve().is_dir():
        for mod_name, spec_info in extract_wrapper_specs(spec).items():
            click.echo(mod_name)

            create_doc(spec_info, output, mod_name, flatten=flatten)
    else:
        spec_info = load_yaml_spec(spec)
        click.echo(spec_info['_module_name'])

        create_doc(spec_info, output, spec_info['_module_name'], flatten=flatten)


@deploy.command(name='test', help="""Test a wrapper pipeline defined in a module

Arguments
---------
module_path
    The file system path to the module to build""")
@click.argument('module_path')
def test(module_path):
    raise NotImplementedError


@deploy.command(name='test-all', help="""Test all wrapper pipelines in a package.

Arguments
---------
package_path
    The file-system path containing the image specifications: Python dictionaries
    named `spec` in sub-modules with the following keys:{spec_help}""")
@click.argument('package_path')
def test_all(package_path):
    raise NotImplementedError


@click.command(name='inspect-docker', 
               help="""Extract the executable from a Docker image""")
@click.argument('image_tag')
def inspect_docker(image_tag):
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


def load_yaml_spec(path, base_dir=None):
    def concat(loader, node):
        seq = loader.construct_sequence(node)
        return ''.join([str(i) for i in seq])

    def slice(loader, node):
        list, start, end = loader.construct_sequence(node)
        return list[start:end]

    def sliceeach(loader, node):
        _, start, end = loader.construct_sequence(node)
        return [
            loader.construct_sequence(x)[start:end] for x in node.value[0].value
        ]

    yaml.SafeLoader.add_constructor(tag='!join', constructor=concat)
    yaml.SafeLoader.add_constructor(tag='!concat', constructor=concat)
    yaml.SafeLoader.add_constructor(tag='!slice', constructor=slice)
    yaml.SafeLoader.add_constructor(tag='!sliceeach', constructor=sliceeach)

    with open(path, 'r') as f:
        data = yaml.load(f, Loader=yaml.SafeLoader)

    frequency = data.get('frequency', None)
    if frequency:
        # TODO: Handle other frequency types, are there any?
        data['frequency'] = Clinical[frequency.split('.')[-1]]

    data['_relative_dir'] = os.path.dirname(os.path.relpath(path, base_dir)) if base_dir else ''
    data['_module_name'] = os.path.basename(path).rsplit('.', maxsplit=1)[0]

    return data


def extract_wrapper_specs(package_path):
    package_path = Path(package_path)

    wrapper_specs = {}

    for root, dirs, files in os.walk(package_path):
        for fn in files:
            if '.' not in fn:
                continue

            name, ext = fn.rsplit('.', maxsplit=1)
            if ext not in ('yaml', 'yml'):
                continue

            wrapper_specs[name] = load_yaml_spec(os.path.join(root, fn), package_path)

    return wrapper_specs


def create_doc(spec, doc_dir, pkg_name, flatten: bool):
    header = {
        "title": spec["package_name"],
        "weight": 10,
        "source_file": pkg_name,
    }

    if flatten:
        out_dir = doc_dir
    else:
        assert isinstance(doc_dir, Path)

        out_dir = doc_dir.joinpath(spec['_relative_dir'])

        assert doc_dir in out_dir.parents

        out_dir.mkdir(parents=True)

    # task = resolve_class(spec['pydra_task'])

    with open(f"{out_dir}/{pkg_name}.md", "w") as f:
        f.write("---\n")
        yaml.dump(header, f)
        f.write("\n---\n\n")

        f.write(f'{spec["description"]}\n\n')

        f.write("### Info\n")
        tbl_info = MarkdownTable(f, "Key", "Value")
        if spec.get("version", None):
            tbl_info.write_row("Version", spec["version"])
        if spec.get("pkg_version", None):
            tbl_info.write_row("App version", spec["pkg_version"])
        # if task.image and task.image != ':':
        #     tbl_info.write_row("Image", escaped_md(task.image))
        if spec.get("base_image", None):  # and task.image != spec["base_image"]:
            tbl_info.write_row("Base image", escaped_md(spec["base_image"]))
        if spec.get("maintainer", None):
            tbl_info.write_row("Maintainer", spec["maintainer"])
        if spec.get("info_url", None):
            tbl_info.write_row("Info URL", spec["info_url"])
        if spec.get("frequency", None):
            tbl_info.write_row("Frequency", spec["frequency"].name.title())

        f.write("\n")

        first_cmd = spec['commands'][0]

        f.write("### Inputs\n")
        tbl_inputs = MarkdownTable(f, "Name", "Bids path", "Data type")
        # for x in task.inputs:
        for x in first_cmd.get('inputs', []):
            name, dtype, path = x
            tbl_inputs.write_row(escaped_md(name), escaped_md(path), escaped_md(dtype))
        f.write("\n")

        f.write("### Outputs\n")
        tbl_outputs = MarkdownTable(f, "Name", "Data type")
        # for x in task.outputs:
        for name, dtype in first_cmd.get('outputs', []):
            tbl_outputs.write_row(escaped_md(name), escaped_md(dtype))
        f.write("\n")

        f.write("### Parameters\n")
        if not first_cmd.get("parameters", None):
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
