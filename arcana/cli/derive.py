from pathlib import Path
import logging
import click
from arcana import __version__
from arcana.core.cli import cli
from arcana.core.data.set import Dataset
from arcana.core.utils import set_loggers


logger = logging.getLogger('arcana')


@cli.group()
def derive():
    pass


@derive.command(name='column', help="""Derive data for a data sink column and
all prerequisite columns.

DATASET_ID_STR string containing the nickname of the data store, the ID of the dataset
(e.g. XNAT project ID or file-system directory) and the dataset's name in the
format <STORE-NICKNAME>//<DATASET-ID>:<DATASET-NAME>

COLUMNS are the names of the sink columns to derive""")
@click.argument('dataset_id_str')
@click.argument('columns', nargs=-1)
@click.option(
    '--work', '-w', default=None,
    help=("The location of the directory where the working files "
          "created during the pipeline execution will be stored"))
@click.option(
    '--plugin', default='cf',
    help=("The Pydra plugin with which to process the workflow"))
@click.option(
    '--loglevel', type=str, default='info',
    help=("The level of detail logging information is presented"))
def derive_column(dataset_id_str, columns, work, plugin, loglevel):

    if work is not None:
        work_dir = Path(work)
        store_cache = work_dir / 'store-cache'
        pipeline_cache = work_dir / 'pipeline-cache'
    else:
        store_cache = None
        pipeline_cache = None
    
    dataset = Dataset.load(dataset_id_str, cache_dir=store_cache)

    set_loggers(loglevel)

    dataset.derive(*columns, cache_dir=pipeline_cache, plugin=plugin)
    
    columns_str = "', '".join(columns)
    logger.info(f"Derived data for '{columns_str}' column(s) successfully")


@derive.command(name='output', help="""Derive an output""")
def derive_output():
    raise NotImplementedError


@derive.command(help="""Derive an output""")
def menu():
    raise NotImplementedError


@derive.command(
    name='ignore-diff',
    help="""Ignore difference between provenance of previously generated derivative
and new parameterisation""")
def ignore_diff():
    raise NotImplementedError
