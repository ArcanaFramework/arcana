import operator as op
from itertools import chain
from functools import reduce, partial
import time
from multiprocessing import Pool, cpu_count
import pytest
from fileformats.generic import File
from fileformats.text import TextFile
from fileformats.field import Text as TextField
from arcana.core.data.set.base import Dataset
from arcana.core.data.store import DataStore
from arcana.core.data.entry import DataEntry
from arcana.core.utils.serialize import asdict
from arcana.common import DirTree
from arcana.testing.data.blueprint import (
    TestDatasetBlueprint,
    FileSetEntryBlueprint as FileBP,
    FieldEntryBlueprint as FieldBP,
)
from arcana.testing import MockRemote


def test_populate_tree(dataset: Dataset):
    blueprint = dataset.__annotations__["blueprint"]
    for freq in dataset.space:
        # For all non-zero bases in the row_frequency, multiply the dim lengths
        # together to get the combined number of rows expected for that
        # row_frequency
        num_rows = reduce(
            op.mul, (ln for ln, b in zip(blueprint.dim_lengths, freq) if b), 1
        )
        assert (
            len(dataset.rows(freq)) == num_rows
        ), f"{freq} doesn't match {len(dataset.rows(freq))} vs {num_rows}"


def test_populate_row(dataset):
    blueprint = dataset.__annotations__["blueprint"]
    for row in dataset.rows("abcd"):
        if isinstance(dataset.store, DirTree):
            expected_paths = sorted(
                chain(
                    (e.path for e in blueprint.entries if isinstance(e, FieldBP)),
                    *(e.filenames for e in blueprint.entries if isinstance(e, FileBP)),
                )
            )
        else:
            expected_paths = sorted(e.path for e in blueprint.entries)
        entry_paths = sorted(e.path for e in row.entries)
        assert entry_paths == expected_paths


def test_get(dataset: Dataset):
    blueprint = dataset.__annotations__["blueprint"]
    for entry_bp in blueprint.entries:
        dataset.add_source(entry_bp.path, datatype=entry_bp.datatype)
    for row in dataset.rows(dataset.leaf_freq):
        for entry_bp in blueprint.entries:
            item = row[entry_bp.path]
            if item.is_fileset:
                item.trim_paths()
                assert sorted(p.name for p in item.fspaths) == sorted(
                    entry_bp.filenames
                )
            else:
                assert item.value == entry_bp.expected_value


def test_post(dataset: Dataset):
    blueprint = dataset.__annotations__["blueprint"]

    def check_inserted():
        """Check that the inserted items are present in the dataset"""
        for deriv_bp in blueprint.derivatives:  # name, freq, datatype, _
            for row in dataset.rows(deriv_bp.row_frequency):
                cell = row.cell(deriv_bp.path, allow_empty=False)
                item = cell.item
                if item.is_fileset and isinstance(dataset.store, DirTree):
                    assert item.fspath.relative_to(dataset.id)
                assert isinstance(item, deriv_bp.datatype)
                if deriv_bp.datatype.is_fileset:
                    assert item.hash_files() == all_checksums[deriv_bp.path]
                else:
                    assert item.primitive(item.value) == item.primitive(
                        deriv_bp.expected_value
                    )

    all_checksums = {}
    with dataset.tree:
        for deriv_bp in blueprint.derivatives:  # name, freq, datatype, files
            dataset.add_sink(
                name=deriv_bp.path,
                datatype=deriv_bp.datatype,
                row_frequency=deriv_bp.row_frequency,
            )
            test_file = deriv_bp.make_item()
            if deriv_bp.datatype.is_fileset:
                all_checksums[deriv_bp.path] = test_file.hash_files()
            # Test inserting the new item into the store
            with dataset.tree:
                for row in dataset.rows(deriv_bp.row_frequency):
                    row[deriv_bp.path] = test_file
        check_inserted()  # Check that cached objects have been updated
    check_inserted()  # Check that objects can be recreated from store


def test_dataset_definition_roundtrip(dataset: Dataset):
    definition = asdict(dataset, omit=["store", "name"])
    definition["store-version"] = "1.0.0"

    data_store = dataset.store

    with data_store.connection:
        data_store.save_dataset_definition(
            dataset_id=dataset.id, definition=definition, name="test_dataset"
        )
        reloaded_definition = data_store.load_dataset_definition(
            dataset_id=dataset.id, name="test_dataset"
        )
    assert definition == reloaded_definition


# We use __file__ here as we just need any old file and can guarantee it exists
@pytest.mark.parametrize("datatype,value", [(File, __file__), (TextField, "value")])
def test_provenance_roundtrip(datatype: type, value: str, saved_dataset: Dataset):
    provenance = {"a": 1, "b": [1, 2, 3], "c": {"x": True, "y": "foo", "z": "bar"}}
    data_store = saved_dataset.store

    with data_store.connection:
        entry = data_store.create_entry("provtest@", datatype, saved_dataset.root)
        data_store.put(datatype(value), entry)  # Create the entry first
        data_store.put_provenance(provenance, entry)  # Save the provenance
        reloaded_provenance = data_store.get_provenance(entry)  # reload the provenance
        assert provenance == reloaded_provenance


def test_singletons():
    standard = set(["dirtree"])
    assert set(DataStore.singletons()) & standard == standard


@pytest.mark.skipif(
    condition=cpu_count() < 2, reason="Not enough cpus to run test with multiprocessing"
)
def test_delayed_download(
    delayed_mock_remote: MockRemote, simple_dataset_blueprint: TestDatasetBlueprint
):

    dataset_id = "delayed_download"
    dataset = simple_dataset_blueprint.make_dataset(delayed_mock_remote, dataset_id)
    entry = next(iter(dataset.rows())).entry("file1")

    delayed_mock_remote.clear_cache()

    worker = partial(
        delayed_download,
        entry,
    )
    with Pool(2) as p:
        try:
            no_offset, with_offset = p.map(worker, [0.0, 0.001])
        finally:
            p.close()  # Marks the pool as closed.
            p.join()  # Required to get the concurrency to show up in test coverage

    assert no_offset == "file1.txt"
    assert with_offset == "modified"


def delayed_download(entry: DataEntry, start_offset: float):
    # Set the downloads off at slightly different times
    time.sleep(start_offset)
    text_file = TextFile(entry.item)
    contents = text_file.contents
    if not start_offset:
        with open(text_file.fspath, "w") as f:
            f.write("modified")
    return contents
