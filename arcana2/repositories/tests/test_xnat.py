import os
import os.path
from datetime import datetime
import operator as op
from pathlib import Path
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


def test_construct_tree(dataset):
    for freq in Clinical:
        # For all non-zero bases in the frequency, multiply the dim lengths
        # together to get the combined number of nodes expected for that
        # frequency
        num_nodes = reduce(
            op.mul, (l for l, b in zip(dataset.dim_lengths, freq) if b), 1)
        assert len(dataset.nodes(freq)) == num_nodes, (
            f"{freq} doesn't match {len(dataset.nodes(freq))} vs {num_nodes}")


# -----------------------
# Test dataset structures
# -----------------------

TEST_DATASETS = {
    'basic': (  # dataset name
        [1, 1, 3],  # number of timepoints, groups and members respectively
        [('scan1', [ # scan type (ID is index)
            ('TEXT',  # resource name
            ['file.txt'])]), # files within the resource
         ('scan2', [
            ('NIFTIX_GZ',
            ['file.nii.gz', 'file.json'])]),
         ('scan3', [
            ('FREESURFER',
            ['doubledir', 'dir', 'file.dat'])]),
         ('scan4', [
            ('DICOM',
            ['file1.dcm', 'file2.dcm', 'file3.dcm']),
            ('NIFTI',
            ['file1.nii.gz']),
            ('BIDS',
            ['file1.json']),
            ('SNAPSHOT', ['file1.png'])])],
        {}),  # id_inference dict
    }


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
DEFAULT_XNAT_TIMEOUT = 10000

@pytest.fixture(params=TEST_DATASETS.keys())
def dataset(repository, request):
    return access_dataset(repository, request.param)


@pytest.fixture(params=['basic'])
def dataset_for_put(repository, request):
    """Datasets used for testing put methods"""
    return access_dataset(repository, request.param, test_suffix='_put')


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
        
    create_datasets(TEST_DATASETS, run_prefix)
    
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


def create_datasets(dataset_structures, run_prefix, test_suffix=''):
    """
    Creates dataset for each entry in dataset_structures
    """

    for dataset_name, (dim_lengths, scans, _) in dataset_structures.items():

        dataset_name = run_prefix + dataset_name + test_suffix

        with connect() as login:
            login.put(f'/data/archive/projects/{dataset_name}')
        # Need to force refresh of connection to refresh project list
        
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
                    for rname, fnames in resources:

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
    login = xnat.connect(server=DOCKER_XNAT_URI, user=DOCKER_XNAT_USER,
                         password=DOCKER_XNAT_PASSWORD,
                         default_timeout=DEFAULT_XNAT_TIMEOUT)
    try:
        yield login
    finally:
        login.disconnect()
