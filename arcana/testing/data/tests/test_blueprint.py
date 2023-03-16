from pathlib import Path
import traceback
import pytest
from functools import partial
from multiprocessing import Pool, cpu_count
from arcana.core.data.store import DataStore
from arcana.testing.data.blueprint import TestDatasetBlueprint
from arcana.testing import MockRemote
from arcana.core.utils.misc import add_exc_note

DELAYED_MOCK_REMOTE_NAME = "delayed_mock_remote"


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
    store_name = DELAYED_MOCK_REMOTE_NAME
    store.save(store_name)
    return store


@pytest.mark.skipif(
    condition=cpu_count() < 2, reason="Not enough cpus to run test with multiprocessing"
)
def make_or_access_dataset(
    store_name: str,
    dataset_id: str,
    blueprint: TestDatasetBlueprint,
    method: str,
):
    try:
        data_store = DataStore.load(store_name)
        if method == "make":
            dataset = blueprint.make_dataset(data_store, dataset_id)
        elif method == "access":
            dataset = blueprint.access_dataset(data_store, dataset_id)
        else:
            assert False
        row = next(iter(dataset.rows()))
        return [e.path for e in row.entries]
    except Exception as e:
        add_exc_note(e, f"attempting to '{method}' the dataset")
        return traceback.format_exc()


def test_blueprint_access_dataset(
    simple_dataset_blueprint: TestDatasetBlueprint,
    delayed_mock_remote: MockRemote,
):

    dataset_id = "blueprint_access"
    worker = partial(
        make_or_access_dataset,
        DELAYED_MOCK_REMOTE_NAME,
        dataset_id,
        simple_dataset_blueprint,
    )
    with Pool(2) as p:
        access_paths, make_paths = p.map(worker, ["access", "make"])

    assert sorted(access_paths) == ["file1", "file2"]
    assert sorted(make_paths) == ["file1", "file2"]


def test_blueprint_access_fail(
    simple_dataset_blueprint: TestDatasetBlueprint, delayed_mock_remote: MockRemote
):
    dataset_id = "blueprint_access_fail"
    with pytest.raises(RuntimeError, match=f"Could not access {dataset_id}"):
        simple_dataset_blueprint.access_dataset(
            delayed_mock_remote, dataset_id, max_num_attempts=10, attempt_interval=0.01
        )
