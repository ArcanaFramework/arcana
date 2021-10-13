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


# Import all test fixtures from sub-package
from .test_fixtures import *


# For debugging in IDE's don't catch raised exceptions and let the IDE
# break at it
if os.getenv('_PYTEST_RAISE', "0") != "0":

    @pytest.hookimpl(tryfirst=True)
    def pytest_exception_interact(call):
        raise call.excinfo.value

    @pytest.hookimpl(tryfirst=True)
    def pytest_internalerror(excinfo):
        raise excinfo.value
