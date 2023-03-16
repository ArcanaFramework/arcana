import os
import logging
from pathlib import Path
from datetime import datetime
import decimal
import shutil
import docker
from tempfile import mkdtemp
from unittest.mock import patch
import pytest
from click.testing import CliRunner
from fileformats.generic import Directory
from fileformats.text import Plain as PlainText
from fileformats.archive import Zip
from fileformats.field import Text as TextField, Decimal, Boolean, Integer, Array
from fileformats.serialization import Json
from fileformats.testing import (
    MyFormatGz,
    MyFormatGzX,
    MyFormatX,
    MyFormat,
    ImageWithHeader,
    YourFormat,
)
from arcana.testing.tasks import (
    add,
    path_manip,
    attrs_func,
    A,
    B,
    C,
    concatenate,
    concatenate_reverse,
)
from arcana.testing.data.blueprint import (
    TestDatasetBlueprint,
    FileSetEntryBlueprint as FileBP,
    FieldEntryBlueprint as FieldBP,
)
from arcana.testing import TestDataSpace, MockRemote
from fileformats.testing import Xyz
from arcana.dirtree import DirTree
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


TEST_TASKS = {
    "add": (add, {"a": 4, "b": 5}, {"out": 9}),
    "path_manip": (
        path_manip,
        {"dpath": Path("/home/foo/Desktop"), "fname": "bar.txt"},
        {"path": "/home/foo/Desktop/bar.txt", "suffix": ".txt"},
    ),
    "attrs_func": (
        attrs_func,
        {"a": A(x=2, y=4), "b": B(u=2.5, v=1.25)},
        {"c": C(z=10)},
    ),
}

BASIC_TASKS = ["add", "path_manip", "attrs_func"]

FILE_TASKS = ["concatenate"]


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


TEST_DATASET_BLUEPRINTS = {
    "full": TestDatasetBlueprint(  # dataset name
        space=TestDataSpace,
        hierarchy=["a", "b", "c", "d"],
        dim_lengths=[2, 3, 4, 5],
        entries=[
            FileBP(path="file1", datatype=PlainText, filenames=["file1.txt"]),
            FileBP(path="file2", datatype=MyFormatGz, filenames=["file2.my.gz"]),
            FileBP(path="dir1", datatype=Directory, filenames=["dir1"]),
            FieldBP(
                path="textfield",
                row_frequency="abcd",
                datatype=TextField,
                value="sample-text",
            ),  # Derivatives to insert
            FieldBP(
                path="booleanfield",
                row_frequency="c",
                datatype=Boolean,
                value="no",
                expected_value=False,
            ),  # Derivatives to insert
        ],
        derivatives=[
            FileBP(
                path="deriv1",
                row_frequency="abcd",
                datatype=PlainText,
                filenames=["file1.txt"],
            ),  # Derivatives to insert
            FileBP(
                path="deriv2",
                row_frequency="c",
                datatype=Directory,
                filenames=["dir"],
            ),
            FileBP(
                path="deriv3",
                row_frequency="bd",
                datatype=PlainText,
                filenames=["file1.txt"],
            ),
            FieldBP(
                path="integerfield",
                row_frequency="c",
                datatype=Integer,
                value=99,
            ),
            FieldBP(
                path="decimalfield",
                row_frequency="bd",
                datatype=Decimal,
                value="33.3333",
                expected_value=decimal.Decimal("33.3333"),
            ),
            FieldBP(
                path="arrayfield",
                row_frequency="bd",
                datatype=Array[Integer],
                value=[1, 2, 3, 4, 5],
            ),
        ],
    ),
    "one_layer": TestDatasetBlueprint(
        space=TestDataSpace,
        hierarchy=["abcd"],
        dim_lengths=[1, 1, 1, 5],
        entries=[
            FileBP(
                path="file1",
                datatype=MyFormatGzX,
                filenames=["file1.my.gz", "file1.json"],
                alternative_datatypes=[MyFormatGz, Json],
            ),
            FileBP(
                path="file2",
                datatype=MyFormatX,
                filenames=["file2.my", "file2.json"],
                alternative_datatypes=[MyFormat, Json],
            ),
        ],
        derivatives=[
            FileBP(
                path="deriv1",
                row_frequency="abcd",
                datatype=Json,
                filenames=["file1.json"],
            ),
            FileBP(
                path="deriv2",
                row_frequency="bc",
                datatype=Xyz,
                filenames=["file1.x", "file1.y", "file1.z"],
            ),
            FileBP(
                path="deriv3",
                row_frequency="__",
                datatype=YourFormat,
                filenames=["file1.yr"],
            ),
        ],
    ),
    "skip_single": TestDatasetBlueprint(
        space=TestDataSpace,
        hierarchy=["a", "bc", "d"],
        dim_lengths=[2, 1, 2, 3],
        entries=[
            FileBP(path="doubledir1", datatype=Directory, filenames=["doubledir1"]),
            FileBP(path="doubledir2", datatype=Directory, filenames=["doubledir2"]),
        ],
        derivatives=[
            FileBP(
                path="deriv1",
                row_frequency="ad",
                datatype=Json,
                filenames=["file1.json"],
            )
        ],
    ),
    "skip_with_inference": TestDatasetBlueprint(
        space=TestDataSpace,
        hierarchy=["bc", "ad"],
        dim_lengths=[2, 3, 2, 4],
        id_composition={
            "bc": r"b(?P<b>\d+)c(?P<c>\d+)",
            "ad": r"a(?P<a>\d+)d(?P<d>\d+)",
        },
        entries=[
            FileBP(
                path="file1",
                datatype=ImageWithHeader,
                filenames=["file1.hdr", "file1.img"],
            ),
            FileBP(path="file2", datatype=YourFormat, filenames=["file2.yr"]),
        ],
    ),
    "redundant": TestDatasetBlueprint(
        space=TestDataSpace,
        hierarchy=[
            "abc",
            "abcd",
        ],  # e.g. XNAT where session ID is unique in project but final layer is organised by timepoint
        dim_lengths=[3, 4, 5, 6],
        id_composition={
            "abc": r"a(?P<a>\d+)b(?P<b>\d+)c(?P<c>\d+)",
            "abcd": r"a\d+b\d+c\d+d(?P<d>\d+)",
        },
        entries=[
            FileBP(path="doubledir", datatype=Directory, filenames=["doubledir"]),
            FileBP(
                path="file1", datatype=Xyz, filenames=["file1.x", "file1.y", "file1.z"]
            ),
        ],
        derivatives=[
            FileBP(
                path="deriv1",
                row_frequency="d",
                datatype=Json,
                filenames=["file1.json"],
            )
        ],
    ),
    "concatenate_test": TestDatasetBlueprint(
        space=TestDataSpace,
        hierarchy=[
            "abcd"
        ],  # e.g. XNAT where session ID is unique in project but final layer is organised by timepoint
        dim_lengths=[1, 1, 1, 2],
        entries=[
            FileBP(path="file1", datatype=PlainText, filenames=["file1.txt"]),
            FileBP(path="file2", datatype=PlainText, filenames=["file2.txt"]),
        ],
    ),
    "concatenate_zip_test": TestDatasetBlueprint(
        space=TestDataSpace,
        hierarchy=[
            "abcd"
        ],  # e.g. XNAT where session ID is unique in project but final layer is organised by timepoint
        dim_lengths=[1, 1, 1, 1],
        entries=[
            FileBP(path="file1", datatype=Zip, filenames=["file1.zip"]),
            FileBP(path="file2", datatype=Zip, filenames=["file2.zip"]),
        ],
    ),
}


GOOD_DATASETS = ["full", "one_layer", "skip_single", "skip_with_inference", "redundant"]

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
        mock_delay=1,
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
