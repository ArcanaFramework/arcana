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
from arcana.core.deploy.image import Metapackage, App
from arcana.core.exceptions import ArcanaBuildError
from arcana.core.data.set import Dataset
from arcana.core.data.store import DataStore
from arcana.core.utils.misc import extract_file_from_docker_image, DOCKER_HUB
from .base import cli
from arcana.core.deploy.command import entrypoint_opts

logger = logging.getLogger("arcana")


@cli.group()
def deploy():
    pass


@deploy.command(
    name="make-app",
    help="""Construct and build a dockerfile/apptainer-file for containing a pipeline

SPEC_ROOT is the file system path to the specification to build, or directory
containing multiple specifications

TARGET is the type of image to build, e.g. arcana.xnat.deploy:XnatApp
the target should resolve to a class deriviing from arcana.core.deploy.App.
If it is located under the `arcana.deploy`, then that prefix can be dropped, e.g.
common:App
""",
)
@click.argument("spec_root", type=click.Path(exists=True, path_type=Path))
@click.argument("target", type=str)
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
def make_app(
    target,
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

    target_cls = ClassResolver(App)(target)

    dc = docker.from_env()

    license_paths = {}
    for lic_name, lic_src in license:
        if isinstance(lic_src, bytes):  # FIXME: This shouldn't be necessary
            lic_src = Path(lic_src.decode("utf-8"))
        license_paths[lic_name] = lic_src

    # Load image specifications from YAML files stored in directory tree

    # Don't error if the modules the task, data stores, data types, etc...
    # aren't present in the build environment
    # FIXME: need to test for this
    with ClassResolver.FALLBACK_TO_STR:
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

    for image_spec in App.load_tree(spec_root, registry=registry):
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
    name="make-docs",
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
def make_docs(spec_root, output, registry, flatten, loglevel):
    # FIXME: Workaround for click 7.x, which improperly handles path_type
    if type(spec_root) is bytes:
        spec_root = Path(spec_root.decode("utf-8"))
    if type(output) is bytes:
        output = Path(output.decode("utf-8"))

    logging.basicConfig(level=getattr(logging, loglevel.upper()))

    output.mkdir(parents=True, exist_ok=True)

    with ClassResolver.FALLBACK_TO_STR:
        image_specs = App.load_tree(spec_root, registry=registry)

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
    name="install-license",
    help="""Installs a license within a store (i.e. site-wide) or dataset (project-specific)
for use in a deployment pipeline

LICENSE_NAME the name of the license to upload. Must match the name of the license specified
in the deployment specification

SOURCE_FILE path to the license file to upload

INSTALL_LOCATIONS a list of installation locations, which are either the "nickname" of a
store (as saved by `arcana store add`) or the ID of a dataset in form
<store-nickname>//<dataset-id>[@<dataset-name>], where the dataset ID
is either the location of the root directory (for file-system based stores) or the
project ID for managed data repositories.
""",
)
@click.argument("license_name")
@click.argument("source_file", type=click.Path(exists=True, path_type=Path))
@click.argument("install_locations", nargs=-1)
@click.option(
    "--logfile",
    default=None,
    type=click.Path(path_type=Path),
    help="Log output to file instead of stdout",
)
@click.option("--loglevel", default="info", help="The level to display logs at")
def install_license(install_locations, license_name, source_file, logfile, loglevel):

    logging.basicConfig(filename=logfile, level=getattr(logging, loglevel.upper()))

    if isinstance(source_file, bytes):  # FIXME: This shouldn't be necessary
        source_file = Path(source_file.decode("utf-8"))

    if not install_locations:
        install_locations = ["file"]

    for install_loc in install_locations:
        if "//" in install_loc:
            dataset = Dataset.load(install_loc)
            store_name, _, _ = Dataset.parse_id_str(install_loc)
            msg = f"for '{dataset.name}' dataset on {store_name} store"
        else:
            store = DataStore.load(install_loc)
            dataset = store.site_licenses_dataset()
            if dataset is None:
                raise ValueError(
                    f"{install_loc} store doesn't support the installation of site-wide "
                    "licenses, please specify a dataset to install it for"
                )
            msg = f"site-wide on {install_loc} store"

        dataset.install_license(license_name, source_file)
        logger.info("Successfully installed '%s' license %s", license_name, msg)


@deploy.command(
    name="pipeline-entrypoint",
    help="""Loads/creates a dataset, then applies and launches a pipeline
in a single command. To be used within the command configuration of an XNAT
Container Service ready Docker image.

DATASET_LOCATOR string containing the nickname of the data store, the ID of the
dataset (e.g. XNAT project ID or file-system directory) and the dataset's name
in the format <store-nickname>//<dataset-id>[@<dataset-name>]

""",
)
@click.argument("dataset_locator")
@entrypoint_opts.data_columns
@entrypoint_opts.parameterisation
@entrypoint_opts.execution
@entrypoint_opts.dataset_config
@entrypoint_opts.debugging
def pipeline_entrypoint(
    dataset_locator,
    spec_path,
    **kwargs,
):

    image_spec = App.load(spec_path)

    image_spec.command.execute(
        dataset_locator,
        **kwargs,
    )
