import arcana
from arcana.core.utils.packaging import submodules
from .base import cli


@cli.group()
def ext():
    """Command-line group for extension hooks"""


# Ensure that all sub-packages under CLI are loaded so they are added to the
# base command
extensions = list(submodules(arcana, subpkg="cli"))
