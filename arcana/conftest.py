import os
import logging
import shutil
from pathlib import Path
from tempfile import mkdtemp
import pytest
from click.testing import CliRunner


# Set DEBUG logging for unittests

debug_level = logging.WARNING

logger = logging.getLogger('arcana')
logger.setLevel(debug_level)

sch = logging.StreamHandler()
sch.setLevel(debug_level)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
sch.setFormatter(formatter)
logger.addHandler(sch)


@pytest.fixture
def work_dir():
    # work_dir = Path.home() / '.arcana-tests'
    # work_dir.mkdir(exist_ok=True)
    # return work_dir
    work_dir = mkdtemp()
    yield Path(work_dir)
    shutil.rmtree(work_dir)


@pytest.fixture
def nifti_sample_dir():
    return Path(__file__).parent.parent / 'test-data'/ 'nifti'


# Import all test fixtures from `test_fixtures` sub-package
from .data.stores.tests.fixtures import (
    test_dataspace, test_dataspace_location, dataset, tmp_dir,
    test_dicom_dataset_dir, dicom_dataset)
from .tasks.tests.fixtures import (
    pydra_task_details, pydra_task)
from .data.stores.xnat.tests.fixtures import (
    xnat_dataset, mutable_xnat_dataset, xnat_archive_dir, xnat_repository,
    xnat_container_registry, run_prefix, concatenate_container, xnat_root_dir)


# For debugging in IDE's don't catch raised exceptions and let the IDE
# break at it
if os.getenv('_PYTEST_RAISE', "0") != "0":

    @pytest.hookimpl(tryfirst=True)
    def pytest_exception_interact(call):
        raise call.excinfo.value

    @pytest.hookimpl(tryfirst=True)
    def pytest_internalerror(excinfo):
        raise excinfo.value

    catch_cli_exceptions = False
else:
    catch_cli_exceptions = True

@pytest.fixture
def cli_runner():
    def invoke(*args, **kwargs):
        runner = CliRunner()
        result = runner.invoke(*args, catch_exceptions=catch_cli_exceptions,
                               **kwargs)
        return result
    return invoke