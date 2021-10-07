import os
import os.path
from datetime import datetime
import operator as op
from pathlib import Path
import time
import contextlib
from tempfile import mkdtemp
from itertools import product
from functools import reduce
from copy import copy
import pytest
import docker
import xnat
from arcana2.repositories import Xnat
from arcana2.core.utils import set_cwd
from arcana2.dimensions.clinical import Clinical
from arcana2.data_formats.general import text, directory, json
from arcana2.data_formats.neuroimaging import niftix_gz, nifti_gz, dicom, nifti


def test_find_nodes(dataset):
    for freq in Clinical:
        # For all non-zero bases in the frequency, multiply the dim lengths
        # together to get the combined number of nodes expected for that
        # frequency
        num_nodes = reduce(
            op.mul, (l for l, b in zip(dataset.dim_lengths, freq) if b), 1)
        assert len(dataset.nodes(freq)) == num_nodes, (
            f"{freq} doesn't match {len(dataset.nodes(freq))} vs {num_nodes}")


def test_find_items(dataset):
    expected_files = {}
    for scan_name, resources in dataset.scans:
        for resource_name, data_format, files in resources:
            if data_format is not None:
                source_name = scan_name + resource_name
                dataset.add_source(source_name, scan_name, data_format)
                expected_files[source_name] = set(files)
    for node in dataset.nodes(Clinical.session):
        for source_name, files in expected_files.items():
            item = node[source_name]
            item.get()
            if item.data_format.directory:
                item_files = set(os.listdir(item.local_cache))
            else:
                item_files = set(os.path.basename(p) for p in item.cache_paths)
            assert item_files == files


# -----------------------
# Test dataset structures
# -----------------------

TEST_DATASETS = {
    'basic': (  # dataset name
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
        {}),  # id_inference dict
    'multi': (  # dataset name
        [2, 3, 4],  # number of timepoints, groups and members respectively
        [
            ('scan1',
             [('TEXT',  # resource name
               text, 
               ['file.txt'])])],
        {Clinical.subject: r'group(?P<group>\d+)member(?P<member>\d+)',
         Clinical.session: r'timepoint(?P<timepoint>\d+).*'}),  # id_inference dict
    }

GOOD_DATASETS = ['basic', 'multi']


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


@pytest.fixture(scope='module')
def repository():

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
        container = dc.containers.run(image.tags[0], detach=True,
                                      ports={'8080/tcp': DOCKER_XNAT_PORT},
                                      remove=True, name=DOCKER_IMAGE)
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
    dim_lengths, scans, id_inference = TEST_DATASETS[name]
    proj_name = repository.run_prefix + name + test_suffix
    dataset = repository.dataset(proj_name, id_inference=id_inference)
    # Stash the args used to create the dataset in attributes so they can be
    # used by tests
    dataset.dim_lengths = dim_lengths
    dataset.scans = scans
    return dataset


def create_dataset_in_repo(dataset_name, run_prefix, test_suffix=''):
    """
    Creates dataset for each entry in dataset_structures
    """

    dim_lengths, scans, _  =  TEST_DATASETS[dataset_name]
    dataset_name = run_prefix + dataset_name + test_suffix

    with connect() as login:
        login.put(f'/data/archive/projects/{dataset_name}')
    
    with connect() as login:
        xproject = login.projects[dataset_name]
        xclasses = login.classes
        for id_tple in product(*(list(range(d)) for d in dim_lengths)):
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
            
            for i, (sname, resources) in enumerate(scans, start=1):
                # Create scan
                xscan = xclasses.MrScanData(id=i, type=sname,
                                            parent=xsession)
                for rname, _, fnames in resources:

                    temp_dir = Path(mkdtemp())
                    # Create the resource
                    xresource = xscan.create_resource(rname)
                    # Create the dummy files
                    for fname in fnames:
                        fpath = Path(fname)
                        # Make double
                        if fname.startswith('doubledir'):
                            os.mkdir(temp_dir / fpath)
                            fname = 'dir'
                            fpath /= fname
                        if fname.startswith('dir'):
                            os.mkdir(temp_dir / fpath)
                            fname = 'test.txt'
                            fpath /= fname
                        with open(temp_dir / fpath, 'w') as f:
                            f.write(f'test {fname}')
                        xresource.upload(str(temp_dir / fpath), str(fpath))


@contextlib.contextmanager
def connect():
    # Need to give time for XNAT to get itself ready after it has
    # started so we try multiple times until giving up
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
