import os
import os.path
from datetime import datetime
import operator as op
from dataclasses import dataclass
import shutil
from pathlib import Path
import random
import hashlib
import time
import contextlib
from tempfile import mkdtemp
from itertools import product
from collections import defaultdict
from functools import reduce
from copy import copy
import pytest
import docker
import xnat
from arcana2.repositories import Xnat
from arcana2.core.utils import set_cwd
from arcana2.dataspaces.clinical import Clinical
from arcana2.core.data.enum import DataSpace
from arcana2.core.data.set import Dataset
from arcana2.core.data.datatype import FileFormat
from arcana2.datatypes.general import text, directory, json
from arcana2.datatypes.neuroimaging import niftix_gz, nifti_gz, dicom, nifti


def test_find_nodes(dataset):
    for freq in Clinical:
        # For all non-zero bases in the frequency, multiply the dim lengths
        # together to get the combined number of nodes expected for that
        # frequency
        num_nodes = reduce(
            op.mul,
            (l for l, b in zip(dataset.blueprint.dim_lengths, freq) if b),
            1)
        assert len(dataset.nodes(freq)) == num_nodes, (
            f"{freq} doesn't match {len(dataset.nodes(freq))} vs {num_nodes}")


def test_get_items(dataset):
    expected_files = {}
    for scan_name, resources in dataset.blueprint.scans:
        for resource_name, datatype, files in resources:
            if datatype is not None:
                source_name = scan_name + resource_name
                dataset.add_source(source_name, scan_name, datatype)
                expected_files[source_name] = set(files)
    for node in dataset.nodes(Clinical.session):
        for source_name, files in expected_files.items():
            item = node[source_name]
            item.get()
            if item.datatype.directory:
                item_files = set(os.listdir(item.fs_path))
            else:
                item_files = set(os.path.basename(p) for p in item.fs_paths)
            assert item_files == files


def test_put_items(mutable_dataset: Dataset):
    all_checksums = {}
    tmp_dir = Path(mkdtemp())
    for name, freq, datatype, files in mutable_dataset.blueprint.to_insert:
        mutable_dataset.add_sink(name=name, format=datatype, frequency=freq)
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
        # Insert node into dataset
        for node in mutable_dataset.nodes(freq):
            item = node[name]
            item.put(*datatype.assort_files(fs_paths))
    def check_inserted():
        for name, freq, datatype, _ in mutable_dataset.blueprint.to_insert:
            for node in mutable_dataset.nodes(freq):
                item = node[name]
                item.get_checksums()
                assert item.datatype == datatype
                assert item.checksums == all_checksums[name]
                item.get()
                assert all(p.exists() for p in item.fs_paths)
    check_inserted()
    # Check read from cached files
    mutable_dataset.refresh()
    orig_server = mutable_dataset.repository.server
    check_inserted()
    # Check downloaded
    mutable_dataset.repository.server = orig_server
    mutable_dataset.refresh()
    shutil.rmtree(mutable_dataset.repository.cache_dir / 'projects'
                  / mutable_dataset.name)
    check_inserted()  


# -----------------------
# Test dataset structures
# -----------------------

@dataclass
class TestDatasetBlueprint():

    dim_lengths: list[int]
    scans: list[tuple[str, list[tuple[str, FileFormat, list[str]]]]]
    id_inference: dict[DataSpace, str]
    to_insert: list[str, tuple[DataSpace, FileFormat, list[str]]]  # files to insert as derivatives


TEST_DATASET_BLUEPRINTS = {
    'basic': TestDatasetBlueprint(  # dataset name
        [1, 1, 3],  # number of timepoints, groups and members respectively
        [('scan1',  # scan type (ID is index)
          [('text', # resource name
            text,  # Data format
            ['file.txt'])]),  # name files to place within resource
         ('scan2',
          [('niftix_gz',
            niftix_gz,
            ['file.nii.gz', 'file.json'])]),
         ('scan3',
          [('directory',
            directory,
            ['doubledir', 'dir', 'file.dat'])]),
         ('scan4',
          [('DICOM', dicom, ['file1.dcm', 'file2.dcm', 'file3.dcm']),
           ('NIFTI', nifti_gz, ['file1.nii.gz']),
           ('BIDS', None, ['file1.json']),
           ('SNAPSHOT', None, ['file1.png'])])],
        {},
        [('deriv1', Clinical.timepoint, text, ['file.txt']),
         ('deriv2', Clinical.subject, niftix_gz, ['file.nii.gz', 'file.json']),
         ('deriv3', Clinical.batch, directory, ['dir']),
         ]),  # id_inference dict
    'multi': TestDatasetBlueprint(  # dataset name
        [2, 2, 2],  # number of timepoints, groups and members respectively
        [
            ('scan1',
             [('TEXT',  # resource name
               text, 
               ['file.txt'])])],
        {Clinical.subject: r'group(?P<group>\d+)member(?P<member>\d+)',
         Clinical.session: r'timepoint(?P<timepoint>\d+).*'},  # id_inference dict
        [
         ('deriv1', Clinical.session, text, ['file.txt']),
         ('deriv2', Clinical.subject, niftix_gz, ['file.nii.gz', 'file.json']),
         ('deriv3', Clinical.timepoint, directory, ['doubledir']),
         ('deriv4', Clinical.member, text, ['file.txt']),
         ('deriv5', Clinical.dataset, text, ['file.txt']),
         ('deriv6', Clinical.batch, text, ['file.txt']),
         ('deriv7', Clinical.matchedpoint, text, ['file.txt']),
         ('deriv8', Clinical.group, text, ['file.txt']),
         ])}

GOOD_DATASETS = ['basic', 'multi']
MUTABLE_DATASETS = ['basic', 'multi']

# ------------------------------------
# Pytest fixtures and helper functions
# ------------------------------------

DOCKER_BUILD_DIR = Path(__file__).parent / 'xnat-docker'
DOCKER_IMAGE = 'arcana-xnat-debug'
DOCKER_HOST = 'localhost'
DOCKER_XNAT_PORT = '8989'
DOCKER_XNAT_URI = f'http://{DOCKER_HOST}:{DOCKER_XNAT_PORT}'
DOCKER_XNAT_USER = 'admin'
DOCKER_XNAT_PASSWORD = 'admin'
CONNECTION_ATTEMPTS = 20
CONNECTION_ATTEMPT_SLEEP = 5
PUT_SUFFIX = '_put'


@pytest.fixture(params=GOOD_DATASETS, scope='module')
def dataset(repository, request):
    return access_dataset(repository, request.param)


@pytest.fixture(params=MUTABLE_DATASETS, scope='function')
def mutable_dataset(repository, request):
    dataset_name = request.param
    test_suffix = 'MUTABLE' + str(hex(random.getrandbits(32)))[2:]
    # Need to create a new dataset per function so it can be safely modified
    # by the test without messing up other tests.
    create_dataset_in_repo(dataset_name, repository.run_prefix,
                           test_suffix=test_suffix)
    return access_dataset(repository, request.param, test_suffix)


@pytest.fixture(scope='module')
def xnat_archive_dir():
    return Path(__file__).parent / 'xnat_archive_dir'


@pytest.fixture(scope='module')
def repository(xnat_archive_dir):

    dc = docker.from_env()

    try:
        image = dc.images.get(DOCKER_IMAGE)
    except docker.errors.ImageNotFound:
        image, build_logs = dc.images.build(path=str(DOCKER_BUILD_DIR),
                                            tag=DOCKER_IMAGE)
        build_logs = list(build_logs)
        if build_logs[-1]['stream'] != f'Successfully tagged {DOCKER_IMAGE}:latest\n':
            raise Exception("Could not build debug XNAT image:\n"
                            ''.join(l['stream'] for l in build_logs))
    
    try:
        container = dc.containers.get(DOCKER_IMAGE)
    except docker.errors.NotFound:
        container = dc.containers.run(
            image.tags[0], detach=True, ports={'8080/tcp': DOCKER_XNAT_PORT},
            remove=True, name=DOCKER_IMAGE,
            volumes={xnat_archive_dir: {'bind': '/data/xnat/archive',
                                        'mode': 'rw'}})
        run_prefix = ''
    else:
        # Set a prefix for all the created projects based on the current time
        # so that they don't clash with datasets generated for previous test
        # runs that haven't been cleaned properly
        run_prefix = datetime.strftime(datetime.now(), '%Y%m%d%H%M%S')

    # Create all datasets that can be reused and don't raise errors
    for dataset_name in GOOD_DATASETS:
        create_dataset_in_repo(dataset_name, run_prefix)
    
    repository = Xnat(
        server=DOCKER_XNAT_URI,
        user=DOCKER_XNAT_USER,
        password=DOCKER_XNAT_PASSWORD,
        cache_dir=mkdtemp())

    # Stash a project prefix in the repository object
    repository.run_prefix = run_prefix

    yield repository

    # If container was run by this fixture (instead of already running) stop
    # it afterwards
    if not run_prefix:
        container.stop()


def access_dataset(repository, name, test_suffix=''):
    blueprint = TEST_DATASET_BLUEPRINTS[name]
    proj_name = repository.run_prefix + name + test_suffix
    dataset = repository.dataset(proj_name, id_inference=blueprint.id_inference)
    # Stash the args used to create the dataset in attributes so they can be
    # used by tests
    dataset.blueprint = blueprint
    return dataset


def create_dataset_in_repo(dataset_name, run_prefix, test_suffix=''):
    """
    Creates dataset for each entry in dataset_structures
    """

    blueprint  =  TEST_DATASET_BLUEPRINTS[dataset_name]
    dataset_name = run_prefix + dataset_name + test_suffix

    with connect() as login:
        login.put(f'/data/archive/projects/{dataset_name}')
    
    with connect() as login:
        xproject = login.projects[dataset_name]
        xclasses = login.classes
        for id_tple in product(*(list(range(d))
                                 for d in blueprint.dim_lengths)):
            ids = dict(zip(Clinical.basis(), id_tple))
            # Create subject
            subject_label = ''.join(
                f'{b}{ids[b]}' for b in Clinical.subject.nonzero_basis())
            xsubject = xclasses.SubjectData(label=subject_label,
                                            parent=xproject)
            # Create session
            session_label = ''.join(
                f'{b}{ids[b]}' for b in Clinical.session.nonzero_basis())
            xsession = xclasses.MrSessionData(label=session_label,
                                            parent=xsubject)
            
            for i, (sname, resources) in enumerate(blueprint.scans, start=1):
                # Create scan
                xscan = xclasses.MrScanData(id=i, type=sname,
                                            parent=xsession)
                for rname, _, fnames in resources:

                    tmp_dir = Path(mkdtemp())
                    # Create the resource
                    xresource = xscan.create_resource(rname)
                    # Create the dummy files
                    for fname in fnames:
                        fpath = create_test_file(fname, tmp_dir)
                        xresource.upload(str(tmp_dir / fpath), str(fpath))


def create_test_file(fname, tmp_dir):
    fpath = Path(fname)
    os.makedirs(tmp_dir, exist_ok=True)
    # Make double dir
    if fname.startswith('doubledir'):
        os.mkdir(tmp_dir / fpath)
        fname = 'dir'
        fpath /= fname
    if fname.startswith('dir'):
        os.mkdir(tmp_dir / fpath)
        fname = 'test.txt'
        fpath /= fname
    with open(tmp_dir / fpath, 'w') as f:
        f.write(f'test {fname}')
    return fpath


@contextlib.contextmanager
def connect():
    # Need to give time for XNAT to get itself ready after it has
    # started so we try multiple times until giving up trying to connect
    attempts = 0
    for _ in range(1, CONNECTION_ATTEMPTS + 1):
        try:
            login = xnat.connect(server=DOCKER_XNAT_URI, user=DOCKER_XNAT_USER,
                                 password=DOCKER_XNAT_PASSWORD)
        except xnat.exceptions.XNATError:
            if attempts == CONNECTION_ATTEMPTS:
                raise
            else:
                time.sleep(CONNECTION_ATTEMPT_SLEEP)
        else:
            break
    try:
        yield login
    finally:
        login.disconnect()
