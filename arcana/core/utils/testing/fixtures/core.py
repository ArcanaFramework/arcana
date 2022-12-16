from pathlib import Path
from tempfile import mkdtemp
import pytest
from click.testing import CliRunner


@pytest.fixture
def work_dir():
    # work_dir = Path.home() / '.arcana-tests'
    # work_dir.mkdir(exist_ok=True)
    # return work_dir
    work_dir = mkdtemp()
    yield Path(work_dir)
    # shutil.rmtree(work_dir)


@pytest.fixture(scope="session")
def build_cache_dir():
    # build_cache_dir = Path.home() / '.arcana-test-build-cache'
    # if build_cache_dir.exists():
    #     shutil.rmtree(build_cache_dir)
    # build_cache_dir.mkdir()
    return Path(mkdtemp())
    # return build_cache_dir


@pytest.fixture
def cli_runner(catch_cli_exceptions):
    def invoke(*args, catch_exceptions=catch_cli_exceptions, **kwargs):
        runner = CliRunner()
        result = runner.invoke(*args, catch_exceptions=catch_exceptions, **kwargs)
        return result

    return invoke
