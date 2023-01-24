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
from fileformats.generic import Directory
from fileformats.text import Plain as Text
from fileformats.serialization import Json
from arcana.core.utils.testing.data import (
    MyFormatGz,
    MyFormatGzX,
    MyFormatX,
    MyFormat,
    ImageWithHeader,
    YourFormat,
)
from arcana.core.utils.testing.tasks import (
    add,
    path_manip,
    attrs_func,
    A,
    B,
    C,
    concatenate,
    concatenate_reverse,
)
from arcana.core.data.store import (
    TestDatasetBlueprint,
    DerivBlueprint,
    ExpDatatypeBlueprint,
)
from arcana.core.utils.testing.data import (
    TestDataSpace as TDS,
    Xyz,
    FlatDirStore,
)
from arcana.dirtree.data import DirTree


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
def simple_store(work_dir):
    store = FlatDirStore(cache_dir=work_dir / "simple-store-cache")
    with patch.dict(os.environ, {"ARCANA_HOME": str(work_dir / "arcana-home")}):
        store.save("simple")
        yield store


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
        hierarchy=[TDS.a, TDS.b, TDS.c, TDS.d],
        dim_lengths=[2, 3, 4, 5],
        files=["file1.txt", "file2.my.gz", "dir1"],
        id_inference=[],
        expected_datatypes={
            "file1": [ExpDatatypeBlueprint(datatype=Text, filenames=["file1.txt"])],
            "file2": [
                ExpDatatypeBlueprint(datatype=MyFormatGz, filenames=["file2.my.gz"])
            ],
            "dir1": [ExpDatatypeBlueprint(datatype=Directory, filenames=["dir1"])],
        },
        derivatives=[
            DerivBlueprint(
                name="deriv1",
                row_frequency=TDS.abcd,
                datatype=Text,
                filenames=["file1.txt"],
            ),  # Derivatives to insert
            DerivBlueprint(
                name="deriv2",
                row_frequency=TDS.c,
                datatype=Directory,
                filenames=["dir"],
            ),
            DerivBlueprint(
                name="deriv3",
                row_frequency=TDS.bd,
                datatype=Text,
                filenames=["file1.txt"],
            ),
        ],
    ),
    "one_layer": TestDatasetBlueprint(
        hierarchy=[TDS.abcd],
        dim_lengths=[1, 1, 1, 5],
        files=["file1.my.gz", "file1.json", "file2.my", "file2.json"],
        id_inference=[],
        expected_datatypes={
            "file1": [
                ExpDatatypeBlueprint(
                    datatype=MyFormatGzX, filenames=["file1.my.gz", "file1.json"]
                ),
                ExpDatatypeBlueprint(datatype=MyFormatGz, filenames=["file1.my.gz"]),
                ExpDatatypeBlueprint(datatype=Json, filenames=["file1.json"]),
            ],
            "file2": [
                ExpDatatypeBlueprint(
                    datatype=MyFormatX, filenames=["file2.my", "file2.json"]
                ),
                ExpDatatypeBlueprint(datatype=MyFormat, filenames=["file2.my"]),
                ExpDatatypeBlueprint(datatype=Json, filenames=["file2.json"]),
            ],
        },
        derivatives=[
            DerivBlueprint(
                name="deriv1",
                row_frequency=TDS.abcd,
                datatype=Json,
                filenames=["file1.json"],
            ),
            DerivBlueprint(
                name="deriv2",
                row_frequency=TDS.bc,
                datatype=Xyz,
                filenames=["file1.x", "file1.y", "file1.z"],
            ),
            DerivBlueprint(
                name="deriv3",
                row_frequency=TDS._,
                datatype=YourFormat,
                filenames=["file1.yr"],
            ),
        ],
    ),
    "skip_single": TestDatasetBlueprint(
        hierarchy=[TDS.a, TDS.bc, TDS.d],
        dim_lengths=[2, 1, 2, 3],
        files=["doubledir1", "doubledir2"],
        id_inference=[],
        expected_datatypes={
            "doubledir1": [
                ExpDatatypeBlueprint(datatype=Directory, filenames=["doubledir1"])
            ],
            "doubledir2": [
                ExpDatatypeBlueprint(datatype=Directory, filenames=["doubledir2"])
            ],
        },
        derivatives=[
            DerivBlueprint(
                name="deriv1",
                row_frequency=TDS.ad,
                datatype=Json,
                filenames=["file1.json"],
            )
        ],
    ),
    "skip_with_inference": TestDatasetBlueprint(
        hierarchy=[TDS.bc, TDS.ad],
        dim_lengths=[2, 3, 2, 4],
        files=["file1.img", "file1.hdr", "file2.yr"],
        id_inference=[
            (TDS.bc, r"b(?P<b>\d+)c(?P<c>\d+)"),
            (TDS.ad, r"a(?P<a>\d+)d(?P<d>\d+)"),
        ],
        expected_datatypes={
            "file1": [
                ExpDatatypeBlueprint(
                    datatype=ImageWithHeader, filenames=["file1.hdr", "file1.img"]
                )
            ],
            "file2": [
                ExpDatatypeBlueprint(datatype=YourFormat, filenames=["file2.yr"])
            ],
        },
    ),
    "redundant": TestDatasetBlueprint(
        hierarchy=[
            TDS.abc,
            TDS.abcd,
        ],  # e.g. XNAT where session ID is unique in project but final layer is organised by timepoint
        dim_lengths=[3, 4, 5, 6],
        files=["doubledir", "file1.x", "file1.y", "file1.z"],
        id_inference=[
            (TDS.abc, r"a(?P<a>\d+)b(?P<b>\d+)c(?P<c>\d+)"),
            (TDS.abcd, r"a\d+b\d+c\d+d(?P<d>\d+)"),
        ],
        expected_datatypes={
            "doubledir": [
                ExpDatatypeBlueprint(datatype=Directory, filenames=["doubledir"])
            ],
            "file1": [
                ExpDatatypeBlueprint(
                    datatype=Xyz, filenames=["file1.x", "file1.y", "file1.z"]
                )
            ],
        },
        derivatives=[
            DerivBlueprint(
                name="deriv1",
                row_frequency=TDS.d,
                datatype=Json,
                filenames=["file1.json"],
            )
        ],
    ),
    "concatenate_test": TestDatasetBlueprint(
        hierarchy=[
            TDS.abcd
        ],  # e.g. XNAT where session ID is unique in project but final layer is organised by timepoint
        dim_lengths=[1, 1, 1, 2],
        files=["file1.txt", "file2.txt"],
    ),
    "concatenate_zip_test": TestDatasetBlueprint(
        hierarchy=[
            TDS.abcd
        ],  # e.g. XNAT where session ID is unique in project but final layer is organised by timepoint
        dim_lengths=[1, 1, 1, 1],
        files=["file1.zip", "file2.zip"],
    ),
}


GOOD_DATASETS = ["full", "one_layer", "skip_single", "skip_with_inference", "redundant"]

# ------------------------------------
# Pytest fixtures and helper functions
# ------------------------------------


@pytest.fixture
def test_dataspace():
    return TDS


@pytest.fixture
def test_dataspace_location():
    return __name__ + ".TestDataSpace"


@pytest.fixture(params=GOOD_DATASETS)
def dataset(work_dir, request):
    dataset_name = request.param
    blueprint = TEST_DATASET_BLUEPRINTS[dataset_name]
    dataset_path = work_dir / dataset_name
    dataset = DirTree().make_test_dataset(blueprint, dataset_path)
    yield dataset
    # shutil.rmtree(dataset.id)


@pytest.fixture
def saved_dataset(work_dir):
    blueprint = TestDatasetBlueprint(
        [
            TDS.abcd
        ],  # e.g. XNAT where session ID is unique in project but final layer is organised by timepoint
        [1, 1, 1, 1],
        ["file1.txt", "file2.txt"],
        {},
        {},
        [],
    )
    dataset_path = work_dir / "saved-dataset"
    dataset = DirTree().make_test_dataset(blueprint, dataset_path)
    dataset.save()
    return dataset


@pytest.fixture(params=GOOD_DATASETS)
def dirtree_dataset(simple_store, work_dir, request):
    dataset_name = request.param
    blueprint = TEST_DATASET_BLUEPRINTS[dataset_name]
    dataset_path = work_dir / dataset_name
    dataset = simple_store.make_test_dataset(blueprint, dataset_path)
    yield dataset
    # shutil.rmtree(dataset.id)


@pytest.fixture
def saved_dirtree_dataset(simple_store, work_dir):
    blueprint = TestDatasetBlueprint(
        hierarchy=[
            TDS.abcd
        ],  # e.g. XNAT where session ID is unique in project but final layer is organised by timepoint
        dim_lengths=[1, 1, 1, 1],
        files=["file1.txt", "file2.txt"],
    )
    dataset_path = work_dir / "saved-dataset"
    dataset = simple_store.make_test_dataset(blueprint, dataset_path)
    dataset.save()
    return dataset


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
        "task": "arcana.core.utils.testing.tasks:concatenate",
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
