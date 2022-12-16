from pathlib import Path
import logging
import cloudpickle as cp
import click
from arcana.core.data.set import Dataset
from arcana.core.utils.misc import set_loggers
from .base import cli

logger = logging.getLogger("arcana")


@cli.group()
def derive():
    pass


@derive.command(
    name="column",
    help="""Derive data for a data sink column and
all prerequisite columns.

DATASET_LOCATOR string containing the nickname of the data store, the ID of the dataset
(e.g. XNAT project ID or file-system directory) and the dataset's name in the
format <store-nickname>//<dataset-id>[@<dataset-name>]

COLUMNS are the names of the sink columns to derive""",
)
@click.argument("dataset_locator")
@click.argument("columns", nargs=-1)
@click.option(
    "--work",
    "-w",
    default=None,
    help=(
        "The location of the directory where the working files "
        "created during the pipeline execution will be stored"
    ),
)
@click.option(
    "--plugin",
    default="cf",
    help=("The Pydra plugin with which to process the workflow"),
)
@click.option(
    "--loglevel",
    type=str,
    default="info",
    help=("The level of detail logging information is presented"),
)
def derive_column(dataset_locator, columns, work, plugin, loglevel):

    logging.basicConfig(level=getattr(logging, loglevel.upper()))

    if work is not None:
        work_dir = Path(work)
        store_cache = work_dir / "store-cache"
        pipeline_cache = work_dir / "pipeline-cache"
    else:
        store_cache = None
        pipeline_cache = None

    dataset = Dataset.load(dataset_locator, cache_dir=store_cache)

    set_loggers(loglevel)

    dataset.derive(*columns, cache_dir=pipeline_cache, plugin=plugin)

    columns_str = "', '".join(columns)
    logger.info(f"Derived data for '{columns_str}' column(s) successfully")


@derive.command(name="output", help="""Derive an output""")
def derive_output():
    raise NotImplementedError


@derive.command(help="""Display the potential derivatives that can be derived""")
def menu():
    raise NotImplementedError


@derive.command(
    name="show-errors",
    help="""Show a Pydra crash report

NODE_WORK_DIR is the directory containing the error pickle file""",
)
@click.argument("node_work_dir")
def show_errors(node_work_dir):
    node_work_dir = Path(node_work_dir)
    files = ["_task.pklz", "_result.pklz", "_error.pklz"]  #
    for fname in files:
        fpath = node_work_dir / fname
        if fpath.exists():
            with open(fpath, "rb") as f:
                contents = cp.load(f)
            click.echo(f"{fname}:")
            if isinstance(contents, dict):
                for k, v in contents.items():
                    if k == "error message":
                        click.echo(f"{k}:\n" + "".join(v))
                    else:
                        click.echo(f"{k}: {v}")
            else:
                click.echo(contents)


@derive.command(
    name="ignore-diff",
    help="""Ignore difference between provenance of previously generated derivative
and new parameterisation""",
)
def ignore_diff():
    raise NotImplementedError


if __name__ == "__main__":
    from click.testing import CliRunner

    runner = CliRunner()
    runner.invoke(
        show_errors,
        [
            "/Users/tclose/Downloads/892d1907-fe2b-40ac-9b77-a3f2ee21ca76/pipeline-cache/Workflow_f3a2bb7474848840aec86fd87e88f5938217fae5976fc699bfc993d95d48a3b8"
        ],
        catch_exceptions=False,
    )
