import os
from pathlib import Path
from tempfile import mkdtemp
import pytest
import shutil
from click.testing import CliRunner


@pytest.fixture
def work_dir():
    # work_dir = Path.home() / '.arcana-tests'
    # work_dir.mkdir(exist_ok=True)
    # return work_dir
    work_dir = mkdtemp()
    yield Path(work_dir)
    # shutil.rmtree(work_dir)


@pytest.fixture(scope='session')
def build_cache_dir():
    build_cache_dir = Path.home() / '.arcana-test-build-cache'
    if build_cache_dir.exists():
        shutil.rmtree(build_cache_dir)
    build_cache_dir.mkdir()
    return build_cache_dir


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
