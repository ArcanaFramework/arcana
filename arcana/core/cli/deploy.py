import logging
import sys
import shutil
from pathlib import Path
import json
import typing as ty
from traceback import format_exc
import tempfile
import click
import docker
import docker.errors
from pydra.engine.core import TaskBase
from arcana.core.utils.serialize import (
    package_from_module,
    pydra_asdict,
    ClassResolver,
)
from arcana.core.deploy.image import Metapackage, CommandImage
from arcana.deploy.xnat.image import XnatCSImage
from arcana.core.exceptions import ArcanaBuildError
from arcana.core.utils.misc import extract_file_from_docker_image, DOCKER_HUB
from .base import cli

logger = logging.getLogger("arcana")


@cli.group()
def deploy():
    pass


@deploy.command(
    help="""Build a wrapper image specified in a module

BUILD_TARGET is the type of image to build, e.g. arcana.deploy.xnat:XnatCSImage
the target should resolve to a class deriviing from arcana.core.deploy.CommandImage.
If it is located under the `arcana.deploy`, then that prefix can be dropped, e.g.
common:PipelineImage

SPEC_ROOT is the file system path to the specification to build, or directory
containing multiple specifications"""
)
@click.argument("build_target", type=str)
@click.argument("spec_root", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--registry",
    default=DOCKER_HUB,
    help="The Docker registry to deploy the pipeline to",
)
@click.option(
    "--build-dir",
    default=None,
    type=click.Path(path_type=Path),
    help=(
        "Specify the directory to build the Docker image in. "
        "Defaults to `.build` in the directory containing the "
        "YAML specification"
    ),
)
@click.option(
    "--release",
    default=None,
    nargs=2,
    metavar="<release-name> <release-version>",
    type=str,
    help=("Name of the release for the package as a whole (i.e. for all pipelines)"),
)
@click.option(
    "--tag-latest/--dont-tag-latest",
    default=False,
    type=bool,
    help='whether to tag the release as the "latest" or not',
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
    "--license",
    type=(str, click.Path(exists=True, path_type=Path)),
    default=(),
    nargs=2,
    metavar="<license-name> <path-to-license-file>",
    multiple=True,
    help=(
        "Licenses provided at build time to be stored in the image (instead of "
        "downloaded at runtime)"
    ),
)
@click.option(
    "--license-to-download",
    type=str,
    default=(),
    multiple=True,
    help=(
        "Specify licenses that are not provided at runtime and instead downloaded "
        "from the data store at runtime in order to satisfy their conditions"
    ),
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
@click.option(
    "--clean-up/--dont-clean-up",
    type=bool,
    default=False,
    help=(
        "Remove built images after they are pushed to the registry (requires --push)"
    ),
)
def build(
    build_target,
    spec_root,
    registry,
    release,
    tag_latest,
    save_manifest,
    logfile,
    loglevel,
    build_dir: Path,
    use_local_packages,
    install_extras,
    raise_errors,
    generate_only,
    use_test_config,
    license,
    license_to_download,
    check_registry,
    push,
    clean_up,
):

    if clean_up and not push:
        raise ValueError("'--clean-up' flag requires '--push'")

    if tag_latest and not release:
        raise ValueError("'--tag-latest' flag requires '--release'")

    if isinstance(spec_root, bytes):  # FIXME: This shouldn't be necessary
        spec_root = Path(spec_root.decode("utf-8"))
    if isinstance(build_dir, bytes):  # FIXME: This shouldn't be necessary
        build_dir = Path(build_dir.decode("utf-8"))

    if build_dir is None:
        build_dir = spec_root / ".build"

    if not build_dir.exists():
        build_dir.mkdir()

    install_extras = install_extras.split(",") if install_extras else []

    logging.basicConfig(filename=logfile, level=getattr(logging, loglevel.upper()))

    temp_dir = tempfile.mkdtemp()

    target_cls = ClassResolver(CommandImage)(build_target)

    dc = docker.from_env()

    license_paths = {}
    for lic_name, lic_src in license:
        if isinstance(lic_src, bytes):  # FIXME: This shouldn't be necessary
            lic_src = Path(lic_src.decode("utf-8"))
        license_paths[lic_name] = lic_src

    # Load image specifications from YAML files stored in directory tree

    # Don't error if the modules the task, data stores, data types, etc...
    # aren't present in the build environment
    with ClassResolver.FALLBACK:
        image_specs = target_cls.load_tree(
            spec_root,
            registry=registry,
            license_paths=license_paths,
            licenses_to_download=set(license_to_download),
        )

    # Check the target registry to see a) if the images with the same tag
    # already exists and b) whether it was built with the same specs
    if check_registry:

        conflicting = {}
        to_build = []
        for image_spec in image_specs:

            extracted_dir = extract_file_from_docker_image(
                image_spec.reference, image_spec.SPEC_PATH
            )
            if extracted_dir is None:
                logger.info(
                    f"Did not find existing image matching {image_spec.reference}"
                )
                changelog = None
            else:
                logger.info(
                    f"Comparing build spec with that of existing image {image_spec.reference}"
                )
                built_spec = image_spec.load(
                    extracted_dir / Path(image_spec.SPEC_PATH).name
                )

                changelog = image_spec.compare_specs(built_spec, check_version=True)

            if changelog is None:
                to_build.append(image_spec)
            elif not changelog:
                logger.info(
                    "Skipping '%s' build as identical image already "
                    "exists in registry"
                )
            else:
                conflicting[image_spec.reference] = changelog

        if conflicting:
            msg = ""
            for tag, changelog in conflicting.items():
                msg += (
                    f"spec for '{tag}' doesn't match the one that was "
                    "used to build the image already in the registry:\n\n"
                    + str(changelog.pretty())
                    + "\n\n\n"
                )

            raise ArcanaBuildError(msg)

        image_specs = to_build

    if release or save_manifest:
        manifest = {
            "package": spec_root.stem,
            "images": [],
        }
        if release:
            manifest["release"] = ":".join(release)

    errors = False

    for image_spec in image_specs:
        spec_build_dir = (
            build_dir / image_spec.loaded_from.relative_to(spec_root)
        ).with_suffix("")
        if spec_build_dir.exists():
            shutil.rmtree(spec_build_dir)
        spec_build_dir.mkdir(parents=True)
        try:
            image_spec.make(
                build_dir=spec_build_dir,
                use_test_config=use_test_config,
                use_local_packages=use_local_packages,
                generate_only=generate_only,
            )
        except Exception:
            if raise_errors:
                raise
            logger.error(
                "Could not build %s pipeline:\n%s", image_spec.reference, format_exc()
            )
            errors = True
            continue
        else:
            click.echo(image_spec.reference)
            logger.info("Successfully built %s pipeline", image_spec.reference)

        if push:
            try:
                dc.api.push(image_spec.reference)
            except Exception:
                if raise_errors:
                    raise
                logger.error(
                    "Could not push '%s':\n\n%s", image_spec.reference, format_exc()
                )
                errors = True
            else:
                logger.info(
                    "Successfully pushed '%s' to registry", image_spec.reference
                )
            if clean_up:
                dc.api.remove_image(image_spec.reference)
                dc.containers.prune()
                dc.images.prune(filters={"dangling": False})
                dc.api.remove_image(image_spec.base_image)
                dc.images.prune(filters={"dangling": False})
                logger.info(
                    "Removed '%s' and pruned dangling images to free up disk space",
                    image_spec.reference,
                )

        if release or save_manifest:
            manifest["images"].append(
                {
                    "name": image_spec.path,
                    "version": image_spec.tag,
                }
            )
    if release:
        metapkg = Metapackage(
            name=release[0],
            version=release[1],
            org=spec_root.stem,
            manifest=manifest,
        )
        metapkg.make(use_local_packages=use_local_packages)
        if push:
            try:
                dc.api.push(metapkg.reference)
            except Exception:
                if raise_errors:
                    raise
                logger.error(
                    "Could not push release metapackage '%s':\n\n%s",
                    metapkg.reference,
                    format_exc(),
                )
                errors = True
            else:
                logger.info(
                    "Successfully pushed release metapackage '%s' to registry",
                    metapkg.reference,
                )

            if tag_latest:
                # Also push release to "latest" tag
                image = dc.images.get(metapkg.reference)
                latest_tag = metapkg.path + ":latest"
                image.reference(latest_tag)

                try:
                    dc.api.push(latest_tag)
                except Exception:
                    if raise_errors:
                        raise
                    logger.error(
                        "Could not push latest tag for release metapackage '%s':\n\n%s",
                        metapkg.path,
                        format_exc(),
                    )
                    errors = True
                else:
                    logger.info(
                        (
                            "Successfully pushed latest tag for release metapackage '%s' "
                            "to registry"
                        ),
                        metapkg.path,
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

SPEC_ROOT is the file system path to the specification to build, or directory
containing multiple specifications

DOCKER_ORG is the Docker organisation the images should belong to""",
)
@click.argument("spec_root", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--registry",
    default=None,
    help="The Docker registry to deploy the pipeline to",
)
def list_images(spec_root, registry):

    if isinstance(spec_root, bytes):  # FIXME: This shouldn't be necessary
        spec_root = Path(spec_root.decode("utf-8"))

    for image_spec in XnatCSImage.load_tree(spec_root, registry=registry):
        click.echo(image_spec.reference)


# @deploy.command(
#     name="test",
#     help="""Test container images defined by YAML
# specs

# Arguments
# ---------
# module_path
#     The file system path to the module to build""",
# )
# @click.argument("spec_path", type=click.Path(exists=True, path_type=Path))
# def test(spec_path):
#     # FIXME: Workaround for click 7.x, which improperly handles path_type
#     if type(spec_path) is bytes:
#         spec_path = Path(spec_path.decode("utf-8"))

#     raise NotImplementedError


@deploy.command(
    name="docs",
    help="""Build docs for one or more yaml wrappers

SPEC_ROOT is the path of a YAML spec file or directory containing one or more such files.

The generated documentation will be saved to OUTPUT.
""",
)
@click.argument("spec_root", type=click.Path(exists=True, path_type=Path))
@click.argument("output", type=click.Path(path_type=Path))
@click.option(
    "--registry",
    default=DOCKER_HUB,
    help="The Docker registry to deploy the pipeline to",
)
@click.option("--flatten/--no-flatten", default=False)
@click.option("--loglevel", default="warning", help="The level to display logs at")
def build_docs(spec_root, output, registry, flatten, loglevel):
    # FIXME: Workaround for click 7.x, which improperly handles path_type
    if type(spec_root) is bytes:
        spec_root = Path(spec_root.decode("utf-8"))
    if type(output) is bytes:
        output = Path(output.decode("utf-8"))

    logging.basicConfig(level=getattr(logging, loglevel.upper()))

    output.mkdir(parents=True, exist_ok=True)

    with ClassResolver.FALLBACK:
        image_specs = XnatCSImage.load_tree(spec_root, registry=registry)

    for image_spec in image_specs:

        image_spec.autodoc(output, flatten=flatten)
        logging.info("Successfully created docs for %s", image_spec.path)


@deploy.command(
    name="required-packages",
    help="""Detect the Python packages required to run the
specified workflows and return them and their versions""",
)
@click.argument("task_locations", nargs=-1)
def required_packages(task_locations):

    required_modules = set()
    for task_location in task_locations:
        workflow = ClassResolver(TaskBase, alternative_types=[ty.Callable])(
            task_location
        )
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


@deploy.command(
    help="""Displays the changelogs found in the release manifest of a deployment build

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


@deploy.command(
    name="image-entrypoint",
    help="""Defines a new dataset, applies and launches a pipeline
in a single command. Given the complexity of combining all these steps in one
CLI, it isn't recommended to use this command manually, it is typically used
by automatically generated code when deploying a pipeline within a container image.

Not all options are be used when defining datasets, however, the
'--dataset-name <NAME>' option can be provided to use an existing dataset
definition.

TASK_LOCATION is the location to a Pydra workflow on the Python system path.
It can be omitted if PIPELINE_NAME matches an existing pipeline

PIPELINE_NAME is the name of the pipeline

DATASET_ID_STR string containing the nickname of the data store, the ID of the
dataset (e.g. XNAT project ID or file-system directory) and the dataset's name
in the format <STORE-NICKNAME>//<DATASET-ID>:<DATASET-NAME>

""",
)
@click.argument("dataset_id_str")
@click.option(
    "--input",
    "-i",
    "input_values",
    nargs=2,
    default=(),
    metavar="<col-name> <match-criteria>",
    multiple=True,
    type=str,
    help=("The match criteria to pass to the column"),
)
@click.option(
    "--output",
    "-o",
    "output_values",
    nargs=2,
    default=(),
    metavar="<col-name> <output-path>",
    multiple=True,
    type=str,
    help=("The path in which to store the output of the pipeline"),
)
@click.option(
    "--parameter",
    "-p",
    "parameter_values",
    nargs=2,
    default=(),
    metavar="<name> <value>",
    multiple=True,
    type=str,
    help=("free parameters of the workflow to be passed by the pipeline user"),
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
    "--dataset-hierarchy", type=str, default=None, help="Comma-separated hierarchy"
)
@click.option("--dataset-name", type=str, default=None, help="The name of the dataset")
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
@click.option(
    "--spec-path",
    type=click.Path(exists=True, path_type=Path),
    default=Path(CommandImage.SPEC_PATH),
    help=(
        "Used to specify a different path to the spec path from the one that is written "
        "to in the image (typically used in debugging/testing)"
    ),
)
def image_entrypoint(
    dataset_id_str,
    work_dir,
    loglevel,
    export_work,
    dataset_hierarchy,
    dataset_name,
    single_row,
    spec_path,
    **kwargs,
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

    image_spec = CommandImage.load_in_image(spec_path)

    dataset = image_spec.command.load_dataset(
        dataset_id_str, store_cache_dir, dataset_hierarchy, dataset_name
    )

    if single_row is not None:
        # Adds a single row to the dataset (i.e. skips a full scan)
        dataset.add_leaf(single_row.split(","))

    image_spec.command.run(
        dataset,
        pipeline_cache_dir=pipeline_cache_dir,
        export_work=export_work,
        **kwargs,
    )