import logging
import sys
from pathlib import Path
import tempfile
import click
from arcana.core.deploy.image import CommandImage
from .base import xnat_group


@xnat_group.command(
    name="cs-image-entrypoint",
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
def cs_image_entrypoint(
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
