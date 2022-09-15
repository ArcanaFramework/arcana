import logging
import sys
import shutil
from pathlib import Path
import re
import json
from collections import defaultdict
import shlex
from traceback import format_exc
import tempfile
import yaml
import click
import docker
import docker.errors
import xnat as xnatpy
from arcana.core.cli import cli
from arcana.core.pipeline import Input as PipelineInput, Output as PipelineOutput
from arcana.core.utils import resolve_class, parse_value, show_workflow_errors
from arcana.core.deploy.utils import (
    load_yaml_spec,
    walk_spec_paths,
    DOCKER_HUB,
    extract_file_from_docker_image,
    compare_specs,
)
from arcana.core.deploy.docs import create_doc
from arcana.core.deploy.build import SPEC_PATH as spec_path_in_docker
from arcana.core.utils import package_from_module, pydra_asdict
from arcana.core.data.row import DataRow
from arcana.deploy.medimage.xnat import build_xnat_cs_image, create_metapackage
from arcana.core.data.set import Dataset
from arcana.core.data.store import DataStore
from arcana.exceptions import ArcanaBuildError, ArcanaUsageError


logger = logging.getLogger("arcana")


@cli.group()
def deploy():
    pass


@deploy.group()
def xnat():
    pass


@xnat.command(
    help="""Build a wrapper image specified in a module

SPEC_PATH is the file system path to the specification to build, or directory
containing multiple specifications

DOCKER_ORG is the Docker organisation the images should belong to"""
)
@click.argument("spec_path", type=click.Path(exists=True, path_type=Path))
@click.argument("docker_org", type=str)
@click.option(
    "--registry",
    "docker_registry",
    default=DOCKER_HUB,
    help="The Docker registry to deploy the pipeline to",
)
@click.option(
    "--build_dir",
    default=None,
    type=click.Path(exists=True, path_type=Path),
    help=(
        "Specify the directory to build the Docker image in. "
        "Defaults to `.build` in the directory containing the "
        "YAML specification"
    ),
)
@click.option(
    "--release",
    default=None,
    type=str,
    help=("Name of the release for the package as a whole (i.e. for all pipelines)"),
)
@click.option(
    "--save-manifest",
    default=None,
    type=click.Path(writable=True),
    help="File path at which to save the build manifest",
)
@click.option(
    "--logfile",
    default=None,
    type=click.Path(path_type=Path),
    help="Log output to file instead of stdout",
)
@click.option("--loglevel", default="info", help="The level to display logs at")
@click.option(
    "--use-local-packages/--dont-use-local-packages",
    type=bool,
    default=False,
    help=(
        "Use locally installed Python packages, instead of pulling "
        "them down from PyPI"
    ),
)
@click.option(
    "--install-extras",
    type=str,
    default=None,
    help=(
        "Install extras to use when installing Arcana inside the "
        "container image. Typically only used in tests to provide "
        "'test' extra"
    ),
)
@click.option(
    "--use-test-config/--dont-use-test-config",
    type=bool,  # FIXME: This should be replaced with option to set XNAT CS IP address
    default=False,
    help=(
        "Build the image so that it can be run in Arcana's test "
        "configuration (only for internal use)"
    ),
)
@click.option(
    "--raise-errors/--log-errors",
    type=bool,
    default=False,
    help=("Raise exceptions instead of logging failures"),
)
@click.option(
    "--generate-only/--build",
    type=bool,
    default=False,
    help="Just create the build directory and dockerfile",
)
@click.option(
    "--license-dir",
    type=click.Path(exists=True, path_type=Path),
    default=None,
    help="Directory containing licences required to build the images",
)
@click.option(
    "--check-registry/--dont-check-registry",
    type=bool,
    default=False,
    help=(
        "Check the registry to see if an existing image with the "
        "same tag is present, and if so whether the specification "
        "matches (and can be skipped) or not (raise an error)"
    ),
)
@click.option(
    "--push/--dont-push",
    type=bool,
    default=False,
    help=("push built images to registry"),
)
def build(
    spec_path,
    docker_org,
    docker_registry,
    release,
    save_manifest,
    logfile,
    loglevel,
    build_dir,
    use_local_packages,
    install_extras,
    raise_errors,
    generate_only,
    use_test_config,
    license_dir,
    check_registry,
    push,
):

    if isinstance(spec_path, bytes):  # FIXME: This shouldn't be necessary
        spec_path = Path(spec_path.decode("utf-8"))
    if isinstance(build_dir, bytes):  # FIXME: This shouldn't be necessary
        build_dir = Path(build_dir.decode("utf-8"))
    if isinstance(license_dir, bytes):  # FIXME: This shouldn't be necessary
        license_dir = Path(license_dir.decode("utf-8"))

    if install_extras:
        install_extras = install_extras.split(",")
    else:
        install_extras = []

    dc = docker.from_env()

    logging.basicConfig(filename=logfile, level=getattr(logging, loglevel.upper()))

    temp_dir = tempfile.mkdtemp()

    if release or save_manifest:
        manifest = {
            "package": docker_org,
            "images": [],
        }
        if release:
            manifest["release"] = release

    if docker_registry != DOCKER_HUB:
        docker_org_fullpath = docker_registry.lower() + "/" + docker_org

    errors = False
    for spath in walk_spec_paths(spec_path):

        logging.info("Building '%s' image", spath)
        spec = load_yaml_spec(spath, base_dir=spec_path)

        # Make image tag
        pkg_name = spath.stem.lower()
        image_name = ".".join(spath.relative_to(spec_path).parent.parts + (pkg_name,))

        image_name = f"{docker_org_fullpath}/{image_name}"

        image_version = str(spec.pop("pkg_version"))
        if "wrapper_version" in spec:
            image_version += f"-{spec.pop('wrapper_version')}"

        image_tag = f"{image_name}:{image_version}"

        if build_dir is None:
            image_build_dir = spath.parent / ".build" / spath.stem
        else:
            image_build_dir = build_dir
            if spath != spec_path:
                image_build_dir /= spath.parent.relative_to(spec_path)
            image_build_dir /= spath.stem

        image_build_dir.mkdir(exist_ok=True, parents=True)

        # Update the spec to remove '_' prefixed keys and add in build params
        spec = {k: v for k, v in spec.items() if not k.startswith("_")}
        spec.update(
            {
                "image_tag": image_tag,
                "docker_registry": docker_registry,
                "use_local_packages": use_local_packages,
                "arcana_install_extras": install_extras,
                "test_config": use_test_config,
            }
        )

        changelog = None

        # Check the target registry to see a) if an image with the same tag
        # already exists and b) whether it was built with the same specs
        if check_registry:
            extracted_dir = extract_file_from_docker_image(
                image_tag, spec_path_in_docker
            )
            if extracted_dir is not None:
                logger.info(
                    f"Comparing build spec with that of existing image {image_tag}"
                )
                built_spec = load_yaml_spec(
                    extracted_dir / Path(spec_path_in_docker).name
                )
                changelog = compare_specs(built_spec, spec, check_version=True)
                if changelog:
                    msg = (
                        f"Spec for '{image_tag}' doesn't match the one that was "
                        "used to build the image already in the registry (skipping):\n\n"
                        + str(changelog.pretty())
                    )
                    if raise_errors:
                        raise ArcanaBuildError(msg)
                    else:
                        logger.error(msg)
                    continue
                else:
                    logger.info(
                        "Skipping '%s' build as identical image already "
                        "exists in registry"
                    )
                    continue
        try:
            build_xnat_cs_image(
                build_dir=image_build_dir,
                generate_only=generate_only,
                license_dir=license_dir,
                **spec,
            )
        except Exception:
            if raise_errors:
                raise
            logger.error("Could not build %s pipeline:\n%s", image_tag, format_exc())
            errors = True
            continue
        else:
            click.echo(image_tag)
            logger.info("Successfully built %s pipeline", image_tag)

        if push:
            try:
                dc.api.push(image_tag)
            except Exception:
                if raise_errors:
                    raise
                logger.error("Could not push '%s':\n\n%s", image_tag, format_exc())
                errors = True
            else:
                logger.info("Successfully pushed '%s' to registry", image_tag)

        if release or save_manifest:
            manifest["images"].append(
                {
                    "name": image_name,
                    "version": image_version,
                    "commands": [c["name"] for c in spec["commands"]],
                }
            )
    if release:
        release_image_tag = f"{docker_org_fullpath}/release-{release}"
        create_metapackage(
            release_image_tag, manifest, use_local_packages=use_local_packages
        )
        if push:
            try:
                dc.api.push(release_image_tag)
            except Exception:
                if raise_errors:
                    raise
                logger.error(
                    "Could not push release metapackage '%s':\n\n%s",
                    release_image_tag,
                    format_exc(),
                )
                errors = True
            else:
                logger.info(
                    "Successfully pushed release metapackage '%s' to registry",
                    image_tag,
                )
        if save_manifest:
            with open(save_manifest, "w") as f:
                json.dump(manifest, f, indent="    ")

    shutil.rmtree(temp_dir)
    if errors:
        sys.exit(1)


@deploy.command(
    name="list-images",
    help="""Walk through the specification paths and list tags of the images
that will be build from them.

SPEC_PATH is the file system path to the specification to build, or directory
containing multiple specifications

DOCKER_ORG is the Docker organisation the images should belong to""",
)
@click.argument("spec_path", type=click.Path(exists=True, path_type=Path))
@click.argument("docker_org", type=str)
@click.option(
    "--registry",
    "docker_registry",
    default=None,
    help="The Docker registry to deploy the pipeline to",
)
def list_images(spec_path, docker_org, docker_registry):

    if isinstance(spec_path, bytes):  # FIXME: This shouldn't be necessary
        spec_path = Path(spec_path.decode("utf-8"))

    for spath in walk_spec_paths(spec_path):
        spec = load_yaml_spec(spath, base_dir=spec_path)

        # Make image tag
        pkg_name = spath.stem.lower()
        tag = ".".join(spath.relative_to(spec_path).parent.parts + (pkg_name,))
        image_version = str(spec.pop("pkg_version"))
        if "wrapper_version" in spec:
            image_version += f"-{spec.pop('wrapper_version')}"
        image_tag = f"{docker_org}/{tag}:{image_version}"
        if docker_registry is not None:
            image_tag = docker_registry.lower().rstrip("/") + "/" + image_tag
        else:
            docker_registry = DOCKER_HUB
        click.echo(image_tag)


@deploy.command(
    name="test",
    help="""Test container images defined by YAML
specs

Arguments
---------
module_path
    The file system path to the module to build""",
)
@click.argument("spec_path", type=click.Path(exists=True, path_type=Path))
def test(spec_path):
    # FIXME: Workaround for click 7.x, which improperly handles path_type
    if type(spec_path) is bytes:
        spec_path = Path(spec_path.decode("utf-8"))

    raise NotImplementedError


@deploy.command(
    name="docs",
    help="""Build docs for one or more yaml wrappers

SPEC_PATH is the path of a YAML spec file or directory containing one or more such files.

The generated documentation will be saved to OUTPUT.
""",
)
@click.argument("spec_path", type=click.Path(exists=True, path_type=Path))
@click.argument("output", type=click.Path(path_type=Path))
@click.option(
    "--root", type=click.Path(exists=True, path_type=Path), default=Path.cwd()
)
@click.option("--flatten/--no-flatten", default=False)
@click.option("--loglevel", default="warning", help="The level to display logs at")
def build_docs(spec_path, output, root, flatten, loglevel):
    # FIXME: Workaround for click 7.x, which improperly handles path_type
    if type(spec_path) is bytes:
        spec_path = Path(spec_path.decode("utf-8"))
    if type(output) is bytes:
        output = Path(output.decode("utf-8"))

    logging.basicConfig(level=getattr(logging, loglevel.upper()))

    output.mkdir(parents=True, exist_ok=True)

    for spath in walk_spec_paths(spec_path):
        spec = load_yaml_spec(spath, base_dir=spec_path)
        mod_name = spec["_module_name"]

        try:
            src_file = spath.absolute().relative_to(root)
        except ValueError:
            logger.warning(
                f"build_docs: {spath.absolute().as_posix()!r} does not lie "
                f"within {root.as_posix()!r}, do you need to specify --root?"
            )
            src_file = spath.absolute()

        create_doc(spec, output, mod_name, src_file=src_file, flatten=flatten)
        logging.info("Successfully created docs for %s", mod_name)


@deploy.command(
    name="required-packages",
    help="""Detect the Python packages required to run the
specified workflows and return them and their versions""",
)
@click.argument("task_locations", nargs=-1)
def required_packages(task_locations):

    required_modules = set()
    for task_location in task_locations:
        workflow = resolve_class(task_location)
        pydra_asdict(workflow, required_modules)

    for pkg in package_from_module(required_modules):
        click.echo(f"{pkg.key}=={pkg.version}")


@deploy.command(
    name="inspect-docker-exec", help="""Extract the executable from a Docker image"""
)
@click.argument("image_tag", type=str)
def inspect_docker_exec(image_tag):
    """Pulls a given Docker image tag and inspects the image to get its
    entrypoint/cmd

    IMAGE_TAG is the tag of the Docker image to inspect"""
    dc = docker.from_env()

    dc.images.pull(image_tag)

    image_attrs = dc.api.inspect_image(image_tag)["Config"]

    executable = image_attrs["Entrypoint"]
    if executable is None:
        executable = image_attrs["Cmd"]

    click.echo(executable)


@xnat.command(
    name="pull-images",
    help="""Updates the installed pipelines on an XNAT instance from a manifest
JSON file via XNAT's REST API.

MANIFEST_JSON is a JSON file containing a list of container images built in the release
and the commands present in them

CONFIG_YAML a YAML file contains the login details for the XNAT server to update, and
patterns with which to filter the images to install

The XNAT server to update is specified in a YAML configuration file contains the login
details for the XNAT server to update, and optionally lists of image tag wildcards to
include and/or exclude, e.g.

    \b
    server: http://localhost
    alias: er61aee1-fc36-569d-3aef-99dc52f479c9
    secret: To85Tmlhh4JO2BigyQ53q87GLwegXdu9II2FoCiCIevCRCt1Tsd6cvttaglFNqTbqQ
    include:
    - tag: ghcr.io/Australian-Imaging-Service/mri.human.neuro.*
    - tag: ghcr.io/Australian-Imaging-Service/pet.rodent.*
    exclude:
    - tag: ghcr.io/Australian-Imaging-Service/mri.human.neuro.bidsapps.*
""",
)
@click.argument("manifest_json", type=click.File())
@click.argument("config_yaml", type=click.File())
def pull_images(config_yaml, manifest_json):
    config = yaml.load(config_yaml, Loader=yaml.Loader)
    manifest = json.load(manifest_json)

    def matches_entry(entry, match_exprs, default=True):
        """Determines whether an entry meets the inclusion and exclusion criteria

        Parameters
        ----------
        entry : dict[str, Any]
            a image entry in the manifest
        exprs : list[dict[str, str]]
            match criteria
        default : bool
            the value if match_exprs are empty
        """
        if not match_exprs:
            return default
        return re.match(
            "|".join(
                i["name"].replace(".", "\\.").replace("*", ".*") for i in match_exprs
            ),
            entry["name"],
        )

    with xnatpy.connect(
        server=config["server"],
        user=config["alias"],
        password=config["secret"],
    ) as xlogin:

        for entry in manifest["images"]:
            if matches_entry(entry, config.get("include")) and not matches_entry(
                entry, config.get("exclude"), default=False
            ):
                tag = f"{entry['name']}:{entry['version']}"
                xlogin.post(
                    "/xapi/docker/pull", query={"image": tag, "save-commands": True}
                )

                # Enable the commands in the built image
                for cmd in xlogin.get("/xapi/commands").json():
                    if cmd["image"] == tag:
                        for wrapper in cmd["xnat"]:
                            xlogin.put(
                                f"/xapi/commands/{cmd['id']}/"
                                f"wrappers/{wrapper['id']}/enabled"
                            )
                click.echo(f"Installed and enabled {tag}")
            else:
                click.echo(f"Skipping {tag} as it doesn't match filters")

    click.echo(
        f"Successfully updated all container images from '{manifest['release']}' of "
        f"'{manifest['package']}' package that match provided filters"
    )


@xnat.command(
    name="pull-auth-refresh",
    help="""Logs into the XNAT instance and regenerates a new authorisation token
to avoid them expiring (2 days by default)

CONFIG_YAML a YAML file contains the login details for the XNAT server to update
""",
)
@click.argument(
    "config_yaml",
    type=click.Path(exists=True),
)
def pull_auth_refresh(config_yaml):
    with open(config_yaml) as f:
        config = yaml.load(f, Loader=yaml.Loader)

    with xnatpy.connect(
        server=config["server"], user=config["alias"], password=config["secret"]
    ) as xlogin:
        alias, secret = xlogin.services.issue_token()

    config["alias"] = alias
    config["secret"] = secret

    with open(config_yaml, "w") as f:
        yaml.dump(config, f)

    click.echo("Updated XNAT connection token successfully")


@xnat.command(
    """Displays the changelogs found in the release manifest of a deployment build

MANIFEST_JSON is a JSON file containing a list of container images built in the release
and the commands present in them"""
)
@click.argument("manifest_json", type=click.File())
@click.argument("images", nargs=-1)
def changelog(manifest_json):

    manifest = json.load(manifest_json)

    for entry in manifest["images"]:
        click.echo(
            f"{entry['name']} [{entry['version']}] changes "
            f"from {entry['previous_version']}:\n{entry['changelog']}"
        )


@click.command(
    name="run-arcana-pipeline",
    help="""Defines a new dataset, applies and launches a pipeline
in a single command. Given the complexity of combining all these steps in one
CLI, it isn't recommended to use this command manually, it is typically used
by automatically generated code when deploying a pipeline within a container image.

Not all options are be used when defining datasets, however, the
'--dataset_name <NAME>' option can be provided to use an existing dataset
definition.

DATASET_ID_STR string containing the nickname of the data store, the ID of the
dataset (e.g. XNAT project ID or file-system directory) and the dataset's name
in the format <STORE-NICKNAME>//<DATASET-ID>:<DATASET-NAME>

PIPELINE_NAME is the name of the pipeline

WORKFLOW_LOCATION is the location to a Pydra workflow on the Python system path.
It can be omitted if PIPELINE_NAME matches an existing pipeline
""",
)
@click.argument("dataset_id_str")
@click.argument("pipeline_name")
@click.argument("task_location")
@click.option(
    "--parameter",
    "-p",
    nargs=2,
    default=(),
    metavar="<name> <value>",
    multiple=True,
    type=str,
    help=("free parameters of the workflow to be passed by the pipeline user"),
)
@click.option(
    "--input",
    nargs=5,
    default=(),
    metavar="<col-name> <col-format> <match-criteria> <pydra-field> <format>",
    multiple=True,
    type=str,
    help=(
        "link an input of the task/workflow to a column of the dataset, adding a source"
        "column matched by the name/path of the column if it isn't already present. "
        "Automatically generated source columns must be able to be specified by their "
        "path alone and be already in the format required by the task/workflow"
    ),
)
@click.option(
    "--output",
    nargs=5,
    default=(),
    metavar="<col-name> <col-format> <output-path> <pydra-field> <format>",
    multiple=True,
    type=str,
    help=(
        "add a sink to the dataset and link it to an output of the task/workflow "
        "in a single step. The sink column be in the same format as produced "
        "by the task/workflow"
    ),
)
@click.option(
    "--row_frequency",
    "-f",
    default=None,
    type=str,
    help=(
        "the row_frequency of the rows the pipeline will be executed over, i.e. "
        "will it be run once per-session, per-subject or per whole dataset, "
        "by default the highest row_frequency rows (e.g. per-session)"
    ),
)
@click.option(
    "--ids", default=None, type=str, help="List of IDs to restrict the pipeline to"
)
@click.option(
    "--work",
    "-w",
    "work_dir",
    default=None,
    help=(
        "The location of the directory where the working files "
        "created during the pipeline execution will be stored"
    ),
)
@click.option(
    "--plugin",
    default="cf",
    help=("The Pydra plugin with which to process the task/workflow"),
)
@click.option(
    "--loglevel",
    type=str,
    default="info",
    help=("The level of detail logging information is presented"),
)
@click.option(
    "--dataset_hierarchy", type=str, default=None, help="Comma-separated hierarchy"
)
@click.option(
    "--dataset_space", type=str, default=None, help="The data space of the dataset"
)
@click.option("--dataset_name", type=str, default=None, help="The name of the dataset")
@click.option(
    "--single-row",
    type=str,
    default=None,
    help=(
        "Restrict the dataset created to a single row (to avoid issues with "
        "unrelated rows that aren't being processed). Comma-separated list "
        "of IDs for each layer of the hierarchy (passed to `Dataset.add_leaf`)"
    ),
)
@click.option(
    "--overwrite/--no-overwrite",
    type=bool,
    help=("Whether to overwrite the saved pipeline with the same name, if present"),
)
@click.option(
    "--configuration",
    nargs=2,
    default=(),
    metavar="<name> <value>",
    multiple=True,
    type=str,
    help=(
        "configuration args of the task/workflow. Differ from parameters in that they is passed to the "
        "task/workflow at initialisation (and can therefore help specify its inputs) not as inputs. Values "
        "can be any valid JSON (including basic types)."
    ),
)
@click.option(
    "--export-work",
    default=None,
    type=click.Path(exists=False, path_type=Path),
    help="Export the work directory to another location after the task/workflow exits",
)
@click.option(
    "--raise-errors/--catch-errors",
    type=bool,
    default=False,
    help="raise exceptions instead of capturing them to suppress call stack",
)
@click.option(
    "--keep-running-on-errors/--exit-on-errors",
    type=bool,
    default=False,
    help=(
        "Keep the the pipeline running in infinite loop on error (will need "
        "to be manually killed). Can be useful in situations where the "
        "enclosing container will be removed on completion and you need to "
        "be able to 'exec' into the container to debug."
    ),
)
def run_pipeline(
    dataset_id_str,
    pipeline_name,
    task_location,
    parameter,
    input,
    output,
    row_frequency,
    overwrite,
    work_dir,
    plugin,
    loglevel,
    dataset_name,
    dataset_space,
    dataset_hierarchy,
    ids,
    configuration,
    single_row,
    export_work,
    raise_errors,
    keep_running_on_errors,
):

    if type(export_work) is bytes:
        export_work = Path(export_work.decode("utf-8"))

    if loglevel != "none":
        logging.basicConfig(stream=sys.stdout, level=getattr(logging, loglevel.upper()))

    if work_dir is None:
        work_dir = tempfile.mkdtemp()
    work_dir = Path(work_dir)
    work_dir.mkdir(parents=True, exist_ok=True)

    store_cache_dir = work_dir / "store-cache"
    pipeline_cache_dir = work_dir / "pydra"

    try:
        dataset = Dataset.load(dataset_id_str)
    except KeyError:

        store_name, id, name = Dataset.parse_id_str(dataset_id_str)

        if dataset_name is not None:
            name = dataset_name

        if dataset_hierarchy is None or dataset_space is None:
            raise RuntimeError(
                f"If the dataset ID string ('{dataset_id_str}') doesn't "
                "reference an existing dataset '--dataset_hierarchy' and "
                "'--dataset_space' must be provided"
            )

        store = DataStore.load(store_name, cache_dir=store_cache_dir)
        space = resolve_class(dataset_space, ["arcana.data.spaces"])
        hierarchy = dataset_hierarchy.split(",")

        try:
            dataset = store.load_dataset(id, name)
        except KeyError:
            dataset = store.new_dataset(id, hierarchy=hierarchy, space=space)

    if single_row is not None:
        # Adds a single row to the dataset (i.e. skips a full scan)
        dataset.add_leaf(single_row.split(","))

    def extract_qualifiers_from_path(user_input: str):
        """Extracts out "qualifiers" from the user-inputted paths. These are
        in the form 'path kw1=val1 kw2=val2...

        Parameters
        ----------
        col_name : str
            name of the column the
        user_input : str
            The path expression + qualifying keyword args to extract

        Returns
        -------
        path : str
            the path expression stripped of qualifiers
        qualifiers : defaultdict[dict]
            the extracted qualifiers
        """
        qualifiers = defaultdict(dict)
        if "=" in user_input:  # Treat user input as containing qualifiers
            parts = shlex.split(user_input)
            path = parts[0]
            for part in parts[1:]:
                try:
                    full_name, val = part.split("=", maxsplit=1)
                except ValueError as e:
                    e.args = ((e.args[0] + f" attempting to split '{part}' by '='"),)
                    raise e
                try:
                    ns, name = full_name.split(".", maxsplit=1)
                except ValueError as e:
                    e.args = (
                        (e.args[0] + f" attempting to split '{full_name}' by '.'"),
                    )
                    raise e
                qualifiers[ns][name] = val
        else:
            path = user_input
        return path, qualifiers

    pipeline_inputs = []
    converter_args = {}  # Arguments passed to converter
    for col_name, col_format_name, match_criteria, pydra_field, format_name in input:
        col_format = resolve_class(col_format_name, prefixes=["arcana.data.formats"])
        format = resolve_class(format_name, prefixes=["arcana.data.formats"])
        if not match_criteria and format != DataRow:
            logger.warning(
                f"Skipping '{col_name}' source column as no input was provided"
            )
            continue
        pipeline_inputs.append(PipelineInput(col_name, pydra_field, format))
        if DataRow in (col_format, format):
            if (col_format, format) != (DataRow, DataRow):
                raise ArcanaUsageError(
                    "Cannot convert to/from built-in data type `DataRow`: "
                    f"col_format={col_format}, format={format}"
                )
            logger.info(
                f"No column added for '{col_name}' column as it uses built-in "
                "type `arcana.core.data.row.DataRow`"
            )
            continue
        path, qualifiers = extract_qualifiers_from_path(match_criteria)
        source_kwargs = qualifiers.pop("criteria", {})
        converter_args[col_name] = qualifiers.pop("converter", {})
        if qualifiers:
            raise ArcanaUsageError(
                "Unrecognised qualifier namespaces extracted from path for "
                f"{col_name} (expected ['criteria', 'converter']): {qualifiers}"
            )
        if col_name in dataset.columns:
            column = dataset[col_name]
            logger.info(f"Found existing source column {column}")
        else:
            logger.info(f"Adding new source column '{col_name}'")
            dataset.add_source(
                name=col_name,
                format=col_format,
                path=path,
                is_regex=True,
                **source_kwargs,
            )

    logger.debug("Pipeline inputs: %s", pipeline_inputs)

    pipeline_outputs = []
    for col_name, col_format_name, path_expr, pydra_field, format_name in output:
        format = resolve_class(format_name, prefixes=["arcana.data.formats"])
        col_format = resolve_class(col_format_name, prefixes=["arcana.data.formats"])
        pipeline_outputs.append(PipelineOutput(col_name, pydra_field, format))
        path, qualifiers = extract_qualifiers_from_path(path_expr)
        converter_args[col_name] = qualifiers.pop("converter", {})
        if qualifiers:
            raise ArcanaUsageError(
                "Unrecognised qualifier namespaces extracted from path for "
                f"{col_name} (expected ['criteria', 'converter']): {qualifiers}"
            )
        if col_name in dataset.columns:
            column = dataset[col_name]
            if not column.is_sink:
                raise ArcanaUsageError(
                    "Output column name '{col_name}' shadows existing source column"
                )
            logger.info(f"Found existing sink column {column}")
        else:
            logger.info(f"Adding new source column '{col_name}'")
            dataset.add_sink(name=col_name, format=col_format, path=path)

    logger.debug("Pipeline outputs: %s", pipeline_outputs)

    kwargs = {n: parse_value(v) for n, v in configuration}
    if "name" not in kwargs:
        kwargs["name"] = "workflow_to_run"

    task = resolve_class(task_location)(**kwargs)

    for pname, pval in parameter:
        if pval != "":
            setattr(task.inputs, pname, parse_value(pval))

    if pipeline_name in dataset.pipelines and not overwrite:
        pipeline = dataset.pipelines[pipeline_name]
        if task != pipeline.workflow:
            raise RuntimeError(
                f"A pipeline named '{pipeline_name}' has already been applied to "
                "which differs from one specified. Please use '--overwrite' option "
                "if this is intentional"
            )
    else:
        pipeline = dataset.apply_pipeline(
            pipeline_name,
            task,
            inputs=pipeline_inputs,
            outputs=pipeline_outputs,
            row_frequency=row_frequency,
            overwrite=overwrite,
            converter_args=converter_args,
        )

    # Instantiate the Pydra workflow
    wf = pipeline(cache_dir=pipeline_cache_dir)

    if ids is not None:
        ids = ids.split(",")

    # execute the workflow
    try:
        result = wf(ids=ids, plugin=plugin)
    except Exception:
        msg = show_workflow_errors(pipeline_cache_dir, omit_nodes=["per_node", wf.name])
        logger.error("Pipeline failed with errors for the following nodes:\n\n%s", msg)
        if raise_errors or not msg:
            raise
        else:
            errors = True
    else:
        logger.info(
            "Pipeline %s ran successfully for the following data rows:\n%s",
            pipeline_name,
            "\n".join(result.output.processed),
        )
        errors = False
    finally:
        if export_work:
            logger.info("Exporting work directory to '%s'", export_work)
            export_work.mkdir(parents=True, exist_ok=True)
            shutil.copytree(pipeline_cache_dir, export_work / "pydra")
    # Abort at the end after the working directory can be copied back to the
    # host so that XNAT knows there was an error
    if errors:
        if keep_running_on_errors:
            while True:
                pass
        else:
            sys.exit(1)


if __name__ == "__main__":
    run_pipeline()
