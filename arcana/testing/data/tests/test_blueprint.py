import asyncio
import pytest
from arcana.core.data.set import Dataset
from arcana.core.data.store import DataStore
from arcana.testing.data.blueprint import TestDatasetBlueprint


@pytest.mark.asyncio
async def test_blueprint_access_dataset(
    data_store: DataStore,
    simple_dataset_blueprint: TestDatasetBlueprint,
    run_prefix: str,
):
    dataset_id = run_prefix + "blueprint_access"
    data_store.create_dataset(
        id=dataset_id,
        leaves=simple_dataset_blueprint.all_ids,
        name="create_and_wait",
        hierarchy=simple_dataset_blueprint.hierarchy,
        id_composition=simple_dataset_blueprint.id_composition,
        space=simple_dataset_blueprint.space,
        metadata={"type": "in-construction"},
    )
    await asyncio.gather(
        wait_to_create_entries(
            data_store, dataset_id, simple_dataset_blueprint, sleep=10
        ),
        access_dataset_row_ids(data_store, dataset_id, simple_dataset_blueprint),
    )


async def wait_to_create_entries(
    dataset: Dataset, blueprint: TestDatasetBlueprint, sleep: int
):

    await asyncio.sleep(sleep)
    for row in dataset.rows():
        blueprint.make_entries(row)
    dataset.metadata.type = "completed"
    dataset.save()
    return dataset


async def access_dataset_row_ids(
    data_store: DataStore, dataset_id: str, blueprint: TestDatasetBlueprint
):
    dataset = blueprint.access_dataset(data_store, dataset_id)
    return list(dataset.row_ids())
