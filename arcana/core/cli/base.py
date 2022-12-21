from pathlib import Path
import pkgutil
from importlib import import_module
import click
import arcana.core as core


# Define the base CLI entrypoint
@click.group()
@click.version_option(version=core.__version__)
def cli():
    """Base command line group, installed as "arcana"."""


@cli.group()
def ext():
    """Command-line group for extension hooks"""


CLI_EXT_PKG = Path(core.__file__).parent / "cli"

# Ensure that all sub-packages under CLI are loaded so they are added to the
# base command
for module in pkgutil.iter_modules([str(CLI_EXT_PKG)], prefix="arcana.cli."):
    import_module(module.name)
