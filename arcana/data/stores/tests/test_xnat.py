import os
import os
from pwd import getpwuid
from grp import getgrgid
import os.path
import operator as op
import shutil
import logging
from pathlib import Path
import hashlib
from tempfile import mkdtemp
from functools import reduce
from arcana.data.dimensions.clinical import Clinical
from arcana.core.data.set import Dataset
from arcana.data.stores.xnat.tests.fixtures import create_test_file
from arcana.data.stores.tests.fixtures import create_test_file

# logger = logging.getLogger('arcana')
# logger.setLevel(logging.INFO)

def test_find_nodes(xnat_dataset):
    for freq in Clinical:
        # For all non-zero bases in the frequency, multiply the dim lengths
        # together to get the combined number of nodes expected for that
        # frequency
        num_nodes = reduce(
            op.mul,
            (l for l, b in zip(xnat_dataset.blueprint.dim_lengths, freq) if b),
            1)
        assert len(xnat_dataset.nodes(freq)) == num_nodes, (
            f"{freq} doesn't match {len(xnat_dataset.nodes(freq))}"
            f" vs {num_nodes}")


def test_get_items(xnat_dataset, caplog):
    expected_files = {}
    for scan_name, resources in xnat_dataset.blueprint.scans:
        for resource_name, datatype, files in resources:
            if datatype is not None:
                source_name = scan_name + resource_name
                xnat_dataset.add_source(source_name, path=scan_name,
                                        datatype=datatype)
                expected_files[source_name] = set(files)
    with caplog.at_level(logging.INFO, logger='arcana'):
        for node in xnat_dataset.nodes(Clinical.session):
            for source_name, files in expected_files.items():
                item = node[source_name]
                try:
                    item.get()
                except PermissionError:
                    def get_perms(f):
                        st = os.stat(f)
                        return (
                            getpwuid(st.st_uid).pw_name,
                            getgrgid(st.st_gid).gr_name,
                            oct(st.st_mode))
                    current_user = getpwuid(os.getuid()).pw_name
                    archive_dir = str(Path.home() / '.xnat4tests' / 'xnat_root' / 'archive' / xnat_dataset.id)
                    archive_perms = get_perms(archive_dir)
                    msg = f"Error accessing {item} as '{current_user}' when '{archive_dir}' has {archive_perms} permissions"
                    raise PermissionError(msg)
                if item.datatype.directory:
                    item_files = set(os.listdir(item.fs_path))
                else:
                    item_files = set(p.name for p in item.fs_paths)
                assert item_files == files
    assert f'{xnat_dataset.access_method} access' in caplog.text.lower()


def test_put_items(mutable_xnat_dataset: Dataset, caplog):
    all_checksums = {}
    tmp_dir = Path(mkdtemp())
    for name, freq, datatype, files in mutable_xnat_dataset.blueprint.to_insert:
        mutable_xnat_dataset.add_sink(name=name, datatype=datatype,
                                        frequency=freq)
        deriv_tmp_dir = tmp_dir / name
        # Create test files, calculate checkums and recorded expected paths
        # for inserted files
        all_checksums[name] = checksums = {}
        fs_paths = []        
        for fname in files:
            test_file = create_test_file(fname, deriv_tmp_dir)
            fhash = hashlib.md5()
            with open(deriv_tmp_dir / test_file, 'rb') as f:
                fhash.update(f.read())
            try:
                rel_path = str(test_file.relative_to(files[0]))
            except ValueError:
                rel_path = '.'.join(test_file.suffixes)                
            checksums[rel_path] = fhash.hexdigest()
            fs_paths.append(deriv_tmp_dir / test_file.parts[0])
        # Insert into first node of that frequency in xnat_dataset
        node = next(iter(mutable_xnat_dataset.nodes(freq)))
        item = node[name]
        with caplog.at_level(logging.INFO, logger='arcana'):
            item.put(*datatype.assort_files(fs_paths))
        assert f'{mutable_xnat_dataset.access_method} access' in caplog.text.lower()
    def check_inserted():
        for name, freq, datatype, _ in mutable_xnat_dataset.blueprint.to_insert:
            node = next(iter(mutable_xnat_dataset.nodes(freq)))
            item = node[name]
            item.get_checksums(force_calculate=(
                mutable_xnat_dataset.access_method == 'direct'))
            assert item.datatype == datatype
            assert item.checksums == all_checksums[name]
            item.get()
            assert all(p.exists() for p in item.fs_paths)
    if mutable_xnat_dataset.access_method == 'api':
        check_inserted()
        # Check read from cached files
        mutable_xnat_dataset.refresh()
        # Note that we can't check the direct access put by this method since
        # it isn't registered with the XNAT database and therefore isn't
        # found by `find_items`. In real life this is handled by the output
        # handlers of the container service
        check_inserted()
        # Check downloaded by deleting the cache dir
        shutil.rmtree(mutable_xnat_dataset.store.cache_dir / 'projects'
                    / mutable_xnat_dataset.id)
        mutable_xnat_dataset.refresh()
        check_inserted()  
