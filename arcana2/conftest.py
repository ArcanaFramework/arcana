import os
from tempfile import mkdtemp
import pytest

TEST_DATA_DIR = os.path.realpath(
    os.path.join(os.path.dirname(__file__), "tests", "data"))


@pytest.fixture(scope='session')
def test_data():
    return TEST_DATA_DIR


# For debugging in IDE's don't catch raised exceptions and let the IDE
# break at it
if os.getenv('_PYTEST_RAISE', "0") != "0":

    @pytest.hookimpl(tryfirst=True)
    def pytest_exception_interact(call):
        raise call.excinfo.value

    @pytest.hookimpl(tryfirst=True)
    def pytest_internalerror(excinfo):
        raise excinfo.value
