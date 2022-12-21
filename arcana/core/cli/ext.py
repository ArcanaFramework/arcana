from pathlib import Path
import click
import arcana
import arcana.core as core
from arcana.core.utils.packaging import submodules


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
submodules(arcana, subpkg="cli")
# for module in pkgutil.iter_modules([str(CLI_EXT_PKG)], prefix="arcana.core.cli."):
#     import_module(module.name)
