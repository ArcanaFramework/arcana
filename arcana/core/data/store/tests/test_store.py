from tempfile import mkdtemp
from pathlib import Path
import operator as op
from functools import reduce
from arcana.core.data.set.base import Dataset
from arcana.dirtree import DirTree
from arcana.core.data.store import DataStore
from arcana.core.utils.misc import path2varname


def test_find_rows(dataset: Dataset):
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


def test_get_items(dataset: Dataset):
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


def test_put_items(dataset: Dataset):
    blueprint = dataset.__annotations__["blueprint"]

    def check_inserted():
        """Check that the inserted items are present in the dataset"""
        for deriv in blueprint.derivatives:  # name, freq, datatype, _
            for row in dataset.rows(deriv.row_frequency):
                item = row[deriv.name]
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
                fspaths.append(DirTree().create_test_fsobject(fname, deriv_tmp_dir))
            test_file = deriv.datatype(fspaths)
            all_checksums[deriv.name] = test_file.hash_files()
            # Test inserting the new item into the store
            with dataset.tree:
                for row in dataset.rows(deriv.row_frequency):
                    row[deriv.name] = test_file
        check_inserted()  # Check that cached objects have been updated
    check_inserted()  # Check that objects can be recreated from store


def test_singletons():
    standard = set(["dirtree"])
    assert set(DataStore.singletons()) & standard == standard
