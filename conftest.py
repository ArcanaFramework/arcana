import logging
import pytest
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

PKG_DIR = Path(__file__).parent


@pytest.fixture(scope='session')
def pkg_dir():
    return PKG_DIR


# Load all test fixtures under `arcana.test.fixtures` package
pytest_plugins = [
    m.name for m in pkgutil.iter_modules(
        [str(PKG_DIR / 'arcana'/ 'test'/ 'fixtures')],
        prefix='arcana.test.fixtures.')]
