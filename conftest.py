import logging
from pathlib import Path
import pkgutil

# Set DEBUG logging for unittests

debug_level = logging.WARNING

logger = logging.getLogger('arcana')
logger.setLevel(debug_level)

sch = logging.StreamHandler()
sch.setLevel(debug_level)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
sch.setFormatter(formatter)
logger.addHandler(sch)


# Load all test fixtures under `arcana.test.fixtures` package
pytest_plugins = [
    m.name for m in pkgutil.iter_modules(
        [str(Path(__file__).parent / 'arcana'/ 'test'/ 'fixtures')],
        prefix='arcana.test.fixtures.')]
