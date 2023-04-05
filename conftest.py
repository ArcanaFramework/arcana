import os
import logging
from pathlib import Path
from datetime import datetime
import shutil
import docker
from tempfile import mkdtemp
from unittest.mock import patch
import pytest
from click.testing import CliRunner
from fileformats.text import Plain as PlainText
from arcana.testing.tasks import (
    concatenate,
    concatenate_reverse,
    TEST_TASKS,
    BASIC_TASKS,
)
from arcana.testing.data.blueprint import (
    TestDatasetBlueprint,
    FileSetEntryBlueprint as FileBP,
    TEST_DATASET_BLUEPRINTS,
    GOOD_DATASETS,
)
from arcana.testing import TestDataSpace, MockRemote
from arcana.stdlib import DirTree
from pydra import set_input_validator

set_input_validator(True)


# Set DEBUG logging for unittests

log_level = logging.WARNING

logger = logging.getLogger("arcana")
logger.setLevel(log_level)

sch = logging.StreamHandler()
sch.setLevel(log_level)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
sch.setFormatter(formatter)
logger.addHandler(sch)

PKG_DIR = Path(__file__).parent


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


@pytest.fixture(scope="session")
def pkg_dir():
    return PKG_DIR


@pytest.fixture(scope="session")
def run_prefix():
    "A datetime string used to avoid stale data left over from previous tests"
    return datetime.strftime(datetime.now(), "%Y%m%d%H%M%S")


# For debugging in IDE's don't catch raised exceptions and let the IDE
# break at it
if os.getenv("_PYTEST_RAISE", "0") != "0":

    @pytest.hookimpl(tryfirst=True)
    def pytest_exception_interact(call):
        raise call.excinfo.value

    @pytest.hookimpl(tryfirst=True)
    def pytest_internalerror(excinfo):
        raise excinfo.value

    CATCH_CLI_EXCEPTIONS = False
else:
    CATCH_CLI_EXCEPTIONS = True


@pytest.fixture
def catch_cli_exceptions():
    return CATCH_CLI_EXCEPTIONS


@pytest.fixture(params=BASIC_TASKS)
def pydra_task_details(request):
    func_name = request.param
    return ("arcana.analysis.tasks.tests.fixtures" + func_name,) + tuple(
        TEST_TASKS[func_name][1:]
    )


@pytest.fixture(params=BASIC_TASKS)
def pydra_task(request):
    task, args, expected_out = TEST_TASKS[request.param]
    task.test_args = args  # stash args away in task object for future access
    return task


# ------------------------------------
# Pytest fixtures and helper functions
# ------------------------------------

DATA_STORES = ["dirtree", "mock_remote"]


@pytest.fixture
def arcana_home(work_dir):
    arcana_home = work_dir / "arcana-home"
    with patch.dict(os.environ, {"ARCANA_HOME": str(arcana_home)}):
        yield arcana_home


@pytest.fixture(params=DATA_STORES)
def data_store(work_dir: Path, arcana_home: Path, request):
    if request.param == "dirtree":
        store = DirTree()
    elif request.param == "mock_remote":
        cache_dir = work_dir / "mock-remote-store" / "cache"
        cache_dir.mkdir(parents=True)
        remote_dir = work_dir / "mock-remote-store" / "remote"
        remote_dir.mkdir(parents=True)
        store = MockRemote(
            server="http://a.server.com",
            cache_dir=cache_dir,
            user="admin",
            password="admin",
            remote_dir=remote_dir,
        )
        store.save("test_mock_store")
    else:
        assert False, f"Unrecognised store {request.param}"
    yield store


@pytest.fixture
def delayed_mock_remote(
    work_dir: Path,
    arcana_home: Path,  # So we save the store definition in the home dir, not ~/.arcana
):
    cache_dir = work_dir / "mock-remote-store" / "cache"
    cache_dir.mkdir(parents=True)
    remote_dir = work_dir / "mock-remote-store" / "remote"
    remote_dir.mkdir(parents=True)
    store = MockRemote(
        server="http://a.server.com",
        cache_dir=cache_dir,
        user="admin",
        password="admin",
        remote_dir=remote_dir,
        mock_delay=0.01,
    )
    store_name = "delayed_mock_remote"
    store.save(store_name)
    return store


@pytest.fixture(params=GOOD_DATASETS)
def dataset(work_dir, data_store, request):
    dataset_name = request.param
    blueprint = TEST_DATASET_BLUEPRINTS[dataset_name]
    dataset_path = work_dir / dataset_name
    dataset_id = dataset_path if isinstance(data_store, DirTree) else dataset_name
    dataset = blueprint.make_dataset(data_store, dataset_id)
    yield dataset
    # shutil.rmtree(dataset.id)


@pytest.fixture
def simple_dataset_blueprint():
    return TestDatasetBlueprint(
        hierarchy=[
            "abcd"
        ],  # e.g. XNAT where session ID is unique in project but final layer is organised by timepoint
        space=TestDataSpace,
        dim_lengths=[1, 1, 1, 1],
        entries=[
            FileBP(path="file1", datatype=PlainText, filenames=["file1.txt"]),
            FileBP(path="file2", datatype=PlainText, filenames=["file2.txt"]),
        ],
    )


@pytest.fixture
def saved_dataset(data_store, simple_dataset_blueprint, work_dir):
    if isinstance(data_store, DirTree):
        dataset_id = work_dir / "saved-dataset"
    else:
        dataset_id = "saved_dataset"
    return simple_dataset_blueprint.make_dataset(data_store, dataset_id, name="")


@pytest.fixture
def tmp_dir():
    tmp_dir = Path(mkdtemp())
    yield tmp_dir
    shutil.rmtree(tmp_dir)


@pytest.fixture(params=["forward", "reverse"])
def concatenate_task(request):
    if request.param == "forward":
        task = concatenate
        # FIXME: Can be removed after https://github.com/nipype/pydra/pull/533 is merged
        task.__name__ = "concatenate"
    else:
        task = concatenate_reverse
    return task


@pytest.fixture(scope="session")
def command_spec():
    return {
        "task": "arcana.testing.tasks:concatenate",
        "inputs": {
            "first_file": {
                "datatype": "fileformats.text:Plain",
                "field": "in_file1",
                "default_column": {
                    "row_frequency": "core:Samples[sample]",
                },
                "help_string": "the first file to pass as an input",
            },
            "second_file": {
                "datatype": "fileformats.text:Plain",
                "field": "in_file2",
                "default_column": {
                    "row_frequency": "core:Samples[sample]",
                },
                "help_string": "the second file to pass as an input",
            },
        },
        "outputs": {
            "concatenated": {
                "datatype": "fileformats.text:Plain",
                "field": "out_file",
                "help_string": "an output file",
            }
        },
        "parameters": {
            "number_of_duplicates": {
                "field": "duplicates",
                "default": 2,
                "datatype": "int",
                "required": True,
                "help_string": "a parameter",
            }
        },
        "row_frequency": "core:Samples[sample]",
    }


@pytest.fixture(scope="session")
def docker_registry():

    IMAGE = "docker.io/registry"
    PORT = "5557"
    CONTAINER = "test-docker-registry"

    dc = docker.from_env()
    try:
        image = dc.images.get(IMAGE)
    except docker.errors.ImageNotFound:
        image = dc.images.pull(IMAGE)

    try:
        container = dc.containers.get(CONTAINER)
    except docker.errors.NotFound:
        container = dc.containers.run(
            image.tags[0],
            detach=True,
            ports={"5000/tcp": PORT},
            remove=True,
            name=CONTAINER,
        )

    yield f"localhost:{PORT}"
    container.stop()
