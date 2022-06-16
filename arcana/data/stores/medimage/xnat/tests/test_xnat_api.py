import os
import sys
import os.path
import operator as op
import shutil
import logging
from pathlib import Path
import hashlib
from tempfile import mkdtemp
from functools import reduce
from arcana.data.spaces.medimage import Clinical
from arcana.core.data.set import Dataset
from arcana.test.datasets import create_test_file

if sys.platform == 'win32':

    def get_perms(f):
        return 'WINDOWS-UNKNOWN'

else:
    from pwd import getpwuid
    from grp import getgrgid

    def get_perms(f):
        st = os.stat(f)
        return (
            getpwuid(st.st_uid).pw_name,
            getgrgid(st.st_gid).gr_name,
            oct(st.st_mode))


# logger = logging.getLogger('arcana')
# logger.setLevel(logging.INFO)

def test_find_rows(xnat_dataset):
    for freq in Clinical:
        # For all non-zero bases in the frequency, multiply the dim lengths
        # together to get the combined number of rows expected for that
        # frequency
        num_rows = reduce(
            op.mul,
            (l for l, b in zip(xnat_dataset.blueprint.dim_lengths, freq) if b),
            1)
        assert len(xnat_dataset.rows(freq)) == num_rows, (
            f"{freq} doesn't match {len(xnat_dataset.rows(freq))}"
            f" vs {num_rows}")


def test_get_items(xnat_dataset, caplog):
    expected_files = {}
    for scan in xnat_dataset.blueprint.scans:
        for resource in scan.resources:
            if resource.format is not None:
                source_name = scan.name + resource.name
                xnat_dataset.add_source(source_name, path=scan.name,
                                        format=resource.format)
                expected_files[source_name] = set(resource.filenames)
    with caplog.at_level(logging.INFO, logger='arcana'):
        for row in xnat_dataset.rows(Clinical.session):
            for source_name, files in expected_files.items():
                item = row[source_name]
                try:
                    item.get()
                except PermissionError:
                    archive_dir = str(Path.home() / '.xnat4tests' / 'xnat_root' / 'archive' / xnat_dataset.id)
                    archive_perms = get_perms(archive_dir)
                    current_user = os.getlogin()
                    msg = f"Error accessing {item} as '{current_user}' when '{archive_dir}' has {archive_perms} permissions"
                    raise PermissionError(msg)
                if item.is_dir:
                    item_files = set(os.listdir(item.fs_path))
                else:
                    item_files = set(p.name for p in item.fs_paths)
                assert item_files == files
    method_str = 'direct' if xnat_dataset.access_method == 'cs' else 'api'
    assert f'{method_str} access' in caplog.text.lower()


def test_put_items(mutable_xnat_dataset: Dataset, caplog):
    all_checksums = {}
    tmp_dir = Path(mkdtemp())
    for deriv in mutable_xnat_dataset.blueprint.derivatives:
        mutable_xnat_dataset.add_sink(name=deriv.name, format=deriv.format,
                                      frequency=deriv.frequency)
        deriv_tmp_dir = tmp_dir / deriv.name
        # Create test files, calculate checkums and recorded expected paths
        # for inserted files
        all_checksums[deriv.name] = checksums = {}
        fs_paths = []        
        for fname in deriv.filenames:
            test_file = create_test_file(fname, deriv_tmp_dir)
            fhash = hashlib.md5()
            with open(deriv_tmp_dir / test_file, 'rb') as f:
                fhash.update(f.read())
            try:
                rel_path = str(test_file.relative_to(Path(deriv.filenames[0])))
            except ValueError:
                rel_path = '.'.join(test_file.suffixes)[1:]
            checksums[rel_path] = fhash.hexdigest()
            fs_paths.append(deriv_tmp_dir / test_file.parts[0])
        # Insert into first row of that frequency in xnat_dataset
        row = next(iter(mutable_xnat_dataset.rows(deriv.frequency)))
        item = row[deriv.name]
        with caplog.at_level(logging.INFO, logger='arcana'):
            item.put(*fs_paths)
        method_str = 'direct' if mutable_xnat_dataset.access_method == 'cs' else 'api'
        assert f'{method_str} access' in caplog.text.lower()
    def check_inserted():
        for deriv in mutable_xnat_dataset.blueprint.derivatives:
            row = next(iter(mutable_xnat_dataset.rows(deriv.frequency)))
            item = row[deriv.name]
            item.get_checksums(force_calculate=(mutable_xnat_dataset.access_method == 'cs'))
            assert isinstance(item, deriv.format)
            assert item.checksums == all_checksums[deriv.name]
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
