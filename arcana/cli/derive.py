import re
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

DATASET_ID_STR string containing the nick-name of the store, the ID of the dataset
(e.g. XNAT project ID or file-system directory) and the dataset's name in the
format <NICKNAME>//DATASET_ID:DATASET_NAME

NAME of the pipeline

WORKFLOW_LOCATION is the location to a Pydra workflow on the Python system path,
<MODULE>:<WORKFLOW>""")
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
    
    dataset = Dataset.load(dataset_id_str)

    set_loggers(loglevel)

    dataset.derive(*columns, cache_dir=work, plugin=plugin)
    
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
