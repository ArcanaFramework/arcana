import os
import os.path
from tempfile import mkdtemp
import hashlib
from pathlib import Path
import operator as op
from functools import reduce
from arcana.core.data.set import Dataset
from arcana.core.utils.testing.data import create_test_file


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
    for fg_name, datatypes in blueprint.expected_formats.items():
        for datatype, files in datatypes:
            source_name = fg_name + datatype.class_name()
            dataset.add_source(source_name, path=fg_name, datatype=datatype)
            source_files[source_name] = set(files)
    for row in dataset.rows(dataset.leaf_freq):
        for source_name, files in source_files.items():
            item = row[source_name]
            item.get()
            assert set(os.path.basename(p) for p in item.fs_paths) == files


def test_put_items(dataset: Dataset):
    blueprint = dataset.__annotations__["blueprint"]
    all_checksums = {}
    all_fs_paths = {}
    for name, freq, datatype, files in blueprint.derivatives:
        dataset.add_sink(name=name, datatype=datatype, row_frequency=freq)
        deriv_tmp_dir = Path(mkdtemp())
        # Create test files, calculate checksums and recorded expected paths
        # for inserted files
        all_checksums[name] = checksums = {}
        all_fs_paths[name] = fs_paths = []
        for fname in files:
            test_file = create_test_file(fname, deriv_tmp_dir)
            fhash = hashlib.md5()
            with open(deriv_tmp_dir / test_file, "rb") as f:
                fhash.update(f.read())
            try:
                rel_path = str(test_file.relative_to(files[0]))
            except ValueError:
                rel_path = ".".join(test_file.suffixes)[1:]
            checksums[rel_path] = fhash.hexdigest()
            fs_paths.append(deriv_tmp_dir / test_file.parts[0])
        # Test inserting the new item into the store
        for row in dataset.rows(freq):
            item = row[name]
            item.put(*fs_paths)

    def check_inserted():
        """Check that the inserted items are present in the dataset"""
        for name, freq, datatype, _ in blueprint.derivatives:
            for row in dataset.rows(freq):
                item = row[name]
                item.get_checksums()
                assert isinstance(item, datatype)
                assert item.checksums == all_checksums[name]
                item.get()
                assert all(p.exists() for p in item.fs_paths)

    check_inserted()  # Check that cached objects have been updated
    dataset.refresh()  # Clear object cache
    check_inserted()  # Check that objects can be recreated from store
