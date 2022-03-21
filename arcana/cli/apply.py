from inspect import Arguments
import click
from arcana.core.utils import resolve_class
from arcana.core.data.store import DataStore
from arcana.core.cli import cli
from arcana.core.utils import get_home_dir


@cli.group()
def apply():
    pass


@apply.command(help="""Applies a Pydra workflow to a dataset""")
def apply_workflow():
    raise NotImplementedError


@apply.command(help="""Applies an analysis class to a dataset""")
def apply_analysis():
    raise NotImplementedError
