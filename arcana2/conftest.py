import os
import logging
import pytest


# Set DEBUG logging for unittests

debug_level = logging.WARNING

logger = logging.getLogger('arcana')
logger.setLevel(debug_level)
fch = logging.FileHandler('./test.log')
fch.setLevel(debug_level)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fch.setFormatter(formatter)
logger.addHandler(fch)

sch = logging.StreamHandler()
sch.setLevel(debug_level)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
sch.setFormatter(formatter)
logger.addHandler(sch)


# Import all test fixtures from `test_fixtures` sub-package
from .test_fixtures.general import work_dir
from .test_fixtures.dataset import (
    test_dataspace, test_dataspace_location, dataset, tmp_dir,
    test_dicom_dataset_dir, dicom_dataset)
from .test_fixtures.tasks import (
    pydra_task_details, pydra_task)
from .test_fixtures.xnat.xnat import (
    xnat_dataset, mutable_xnat_dataset, xnat_archive_dir, xnat_repository,
    xnat_container_registry, run_prefix, xnat_docker_network)


# For debugging in IDE's don't catch raised exceptions and let the IDE
# break at it
if os.getenv('_PYTEST_RAISE', "0") != "0":

    @pytest.hookimpl(tryfirst=True)
    def pytest_exception_interact(call):
        raise call.excinfo.value

    @pytest.hookimpl(tryfirst=True)
    def pytest_internalerror(excinfo):
        raise excinfo.value
