# import traceback
# import pytest
# from functools import partial
# from multiprocessing import Pool, cpu_count
# from arcana.core.data.store import DataStore
# from arcana.testing.data.blueprint import TestDatasetBlueprint
# from arcana.testing import MockRemote
# from arcana.core.utils.misc import add_exc_note
from pathlib import Path
import pytest
from arcana.stdlib import DirTree, Clinical
from arcana.testing.data.blueprint import TEST_DATASET_BLUEPRINTS


@pytest.mark.parametrize("blueprint_name", TEST_DATASET_BLUEPRINTS)
def test_blueprint_translation(blueprint_name: str, work_dir: Path):
    blueprint = TEST_DATASET_BLUEPRINTS[blueprint_name]
    translated = blueprint.translate_to(Clinical)
    translated.make_dataset(store=DirTree(), dataset_id=work_dir / "blueprint")


# @pytest.mark.skipif(
#     condition=cpu_count() < 2, reason="Not enough cpus to run test with multiprocessing"
# )
# def make_or_access_dataset(
#     store_name: str,
#     dataset_id: str,
#     blueprint: TestDatasetBlueprint,
#     method: str,
# ):
#     try:
#         data_store = DataStore.load(store_name)
#         if method == "make":
#             dataset = blueprint.make_dataset(data_store, dataset_id)
#         elif method == "access":
#             dataset = blueprint.access_dataset(data_store, dataset_id)
#         else:
#             assert False
#         row = next(iter(dataset.rows()))
#         return [e.path for e in row.entries]
#     except Exception as e:
#         add_exc_note(e, f"attempting to '{method}' the dataset")
#         return traceback.format_exc()


# def test_blueprint_access_dataset(
#     simple_dataset_blueprint: TestDatasetBlueprint,
#     delayed_mock_remote: MockRemote,
# ):

#     dataset_id = "blueprint_access"
#     worker = partial(
#         make_or_access_dataset,
#         delayed_mock_remote.name,
#         dataset_id,
#         simple_dataset_blueprint,
#     )
#     with Pool(2) as p:
#         try:
#             access_paths, make_paths = p.map(worker, ["access", "make"])
#         finally:
#             p.close()  # Marks the pool as closed.
#             p.join()  # Required to get the concurrency to show up in test coverage

#     assert sorted(access_paths) == ["file1", "file2"]
#     assert sorted(make_paths) == ["file1", "file2"]


# def test_blueprint_access_fail(
#     simple_dataset_blueprint: TestDatasetBlueprint, delayed_mock_remote: MockRemote
# ):
#     dataset_id = "blueprint_access_fail"
#     with pytest.raises(RuntimeError, match=f"Could not access {dataset_id}"):
#         simple_dataset_blueprint.access_dataset(
#             delayed_mock_remote, dataset_id, max_num_attempts=10, attempt_interval=0.01
#         )
