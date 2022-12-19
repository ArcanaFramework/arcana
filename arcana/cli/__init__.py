from pathlib import Path
import pkgutil
from importlib import import_module


CLI_EXT_PKG = Path(__file__).parent

# Ensure that all sub-packages under CLI are loaded so they are added to the
# base command
for module in pkgutil.iter_modules([str(CLI_EXT_PKG)], prefix="arcana.cli."):
    import_module(module.name)
