from pathlib import Path
import arcana.core
from arcana.core.utils.packaging import submodules
from .base import cli


@cli.group()
def ext():
    """Command-line group for extension hooks"""


CLI_EXT_PKG = Path(arcana.core.__file__).parent / "cli"

# Ensure that all sub-packages under CLI are loaded so they are added to the
# base command
submodules(arcana, subpkg="cli")
# for module in pkgutil.iter_modules([str(CLI_EXT_PKG)], prefix="arcana.core.cli."):
#     import_module(module.name)
