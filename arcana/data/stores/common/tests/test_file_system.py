import os
import os.path
from tempfile import mkdtemp
import hashlib
from pathlib import Path
import operator as op
from functools import reduce
from arcana.core.data.set import Dataset
from arcana.test.datasets import create_test_file



def test_find_nodes(dataset: Dataset):
    for freq in dataset.space:
        # For all non-zero bases in the frequency, multiply the dim lengths
        # together to get the combined number of nodes expected for that
        # frequency
        num_nodes = reduce(
            op.mul, (l for l, b in zip(dataset.blueprint.dim_lengths, freq) if b), 1)
        assert len(dataset.nodes(freq)) == num_nodes, (
            f"{freq} doesn't match {len(dataset.nodes(freq))} vs {num_nodes}")


def test_get_items(dataset: Dataset):
    source_files = {}
    for fg_name, formats in dataset.blueprint.expected_formats.items():
        for format, files in formats:
            source_name = fg_name + format.class_name()
            dataset.add_source(source_name, path=fg_name, format=format)
            source_files[source_name] = set(files)
    for node in dataset.nodes(dataset.leaf_freq):
        for source_name, files in source_files.items():
            item = node[source_name]
            item.get()
            assert set(os.path.basename(p) for p in item.fs_paths) == files


def test_put_items(dataset: Dataset):
    all_checksums = {}
    all_fs_paths = {}
    for name, freq, format, files in dataset.blueprint.derivatives:
        dataset.add_sink(name=name, format=format, frequency=freq)
        deriv_tmp_dir = Path(mkdtemp())
        # Create test files, calculate checkums and recorded expected paths
        # for inserted files
        all_checksums[name] = checksums = {}
        all_fs_paths[name] = fs_paths = []
        for fname in files:
            test_file = create_test_file(fname, deriv_tmp_dir)
            fhash = hashlib.md5()
            with open(deriv_tmp_dir / test_file, 'rb') as f:
                fhash.update(f.read())
            try:
                rel_path = str(test_file.relative_to(files[0]))
            except ValueError:
                rel_path = '.'.join(test_file.suffixes)[1:]
            checksums[rel_path] = fhash.hexdigest()
            fs_paths.append(deriv_tmp_dir / test_file.parts[0])
        # Test inserting the new item into the store
        for node in dataset.nodes(freq):
            item = node[name]
            item.put(*fs_paths)
    def check_inserted():
        """Check that the inserted items are present in the dataset"""
        for name, freq, format, _ in dataset.blueprint.derivatives:
            for node in dataset.nodes(freq):
                item = node[name]
                item.get_checksums()
                assert isinstance(item, format)
                assert item.checksums == all_checksums[name]
                item.get()
                assert all(p.exists() for p in item.fs_paths)
    check_inserted()  # Check that cached objects have been updated
    dataset.refresh()  # Clear object cache
    check_inserted()  # Check that objects can be recreated from store
