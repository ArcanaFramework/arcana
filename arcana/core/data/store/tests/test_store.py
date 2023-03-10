from tempfile import mkdtemp
from pathlib import Path
import itertools
import operator as op
from functools import reduce
from fileformats.core import Field
from arcana.core.data.set.base import Dataset
from arcana.dirtree import DirTree
from arcana.core.data.store import DataStore
from arcana.core.utils.misc import path2varname
from arcana.core.utils.serialize import asdict


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
        entry_paths = sorted(e.path for e in row.entries)
        if isinstance(dataset.store, DirTree):
            expected = sorted(blueprint.files)
        else:
            expected = sorted(set(f.split(".")[0] for f in blueprint.files))
        assert entry_paths == expected


def test_get_fileset(dataset: Dataset):
    blueprint = dataset.__annotations__["blueprint"]
    source_files = {}
    for fg_name, exp_datatypes in blueprint.expected_datatypes.items():
        for exp in exp_datatypes:
            source_name = fg_name + path2varname(exp.datatype.mime_like)
            dataset.add_source(source_name, path=fg_name, datatype=exp.datatype)
            source_files[source_name] = set(exp.filenames)
    for row in dataset.rows(dataset.leaf_freq):
        for source_name, files in source_files.items():
            item = row[source_name].trim_paths()
            assert set(p.name for p in item.fspaths) == files


def test_post_fileset(dataset: Dataset):
    blueprint = dataset.__annotations__["blueprint"]

    def check_inserted():
        """Check that the inserted items are present in the dataset"""
        for deriv in blueprint.derivatives:  # name, freq, datatype, _
            for row in dataset.rows(deriv.row_frequency):
                cell = row.cell(deriv.name, allow_empty=False)
                item = cell.item
                if isinstance(dataset.store, DirTree):
                    assert item.fspath.relative_to(dataset.id)
                assert isinstance(item, deriv.datatype)
                assert item.hash_files() == all_checksums[deriv.name]

    all_checksums = {}
    with dataset.tree:
        for deriv in blueprint.derivatives:  # name, freq, datatype, files
            dataset.add_sink(
                name=deriv.name,
                datatype=deriv.datatype,
                row_frequency=deriv.row_frequency,
            )
            deriv_tmp_dir = Path(mkdtemp())
            fspaths = []
            for fname in deriv.filenames:
                fspaths.append(
                    blueprint.create_fsobject(
                        fname,
                        deriv_tmp_dir,
                        store=dataset.store,
                    )
                )
            test_file = deriv.datatype(fspaths)
            all_checksums[deriv.name] = test_file.hash_files()
            # Test inserting the new item into the store
            with dataset.tree:
                for row in dataset.rows(deriv.row_frequency):
                    row[deriv.name] = test_file
        check_inserted()  # Check that cached objects have been updated
    check_inserted()  # Check that objects can be recreated from store


def test_field_rountrip(dataset: Dataset):
    blueprint = dataset.__annotations__["blueprint"]

    def sort_key(bp):
        return bp.row_frequency

    for field_bp in blueprint.fields:
        dataset.add_sink(
            name=field_bp.name,
            datatype=field_bp.datatype,
            row_frequency=field_bp.row_frequency,
        )
    for freq, bps in itertools.groupby(
        sorted(blueprint.fields, key=sort_key), key=sort_key
    ):
        bps = list(bps)
        row_id = next(iter(dataset.row_ids(freq)))
        row = dataset.row(id=row_id, frequency=freq)
        for bp in bps:
            row[bp.name] = bp.value
        reloaded_row = dataset.row(id=row_id, frequency=freq)
        # Check all entries are loaded
        field_entries = [
            e for e in reloaded_row.entries if issubclass(e.datatype, Field)
        ]
        assert sorted(e.path for e in field_entries) == sorted(
            bp.name + "@" for bp in bps
        )
        for bp in bps:
            assert row[bp.name] == bp.datatype(bp.value)


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


def test_provenance_roundtrip(dataset: Dataset):
    provenance = {"a": 1, "b": [1, 2, 3], "c": {"x": True, "y": "foo", "z": "bar"}}
    data_store = dataset.store

    with data_store.connection:
        data_store.put_provenance(provenance)
        reloaded_provenance = data_store.get_provenance()
        assert provenance == reloaded_provenance


def test_singletons():
    standard = set(["dirtree"])
    assert set(DataStore.singletons()) & standard == standard
