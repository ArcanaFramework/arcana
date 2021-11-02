import os
from datetime import datetime
from dataclasses import dataclass
import shutil
from pathlib import Path
import random
import time
import contextlib
from tempfile import mkdtemp
from itertools import product
import pytest
import docker
import xnat
from arcana2.data.repositories import Xnat
from arcana2.data.spaces.clinical import Clinical
from arcana2.core.data.space import DataSpace
from arcana2.core.data.type import FileFormat
from arcana2.data.types.general import text, directory
from arcana2.data.types.neuroimaging import niftix_gz, nifti_gz, dicom
from arcana2.core.data.tests.fixtures import create_test_file


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
         ]),
    'simple': TestDatasetBlueprint(  # dataset name
        [1, 1, 1],  # number of timepoints, groups and members respectively
        [('scan1',  # scan type (ID is index)
          [('text', # resource name
            text,  # Data format
            ['file1.txt'])]),  # name files to place within resource
         ('scan2',
          [('text',
            text,
            ['file2.txt'])])],
        {},
        [('deriv1', Clinical.session, text, ['file.txt'])]),  # id_inference dict
    'basic': TestDatasetBlueprint(
        [1, 1, 2],
        [('scan1',
          [('text', text, ['file1.txt'])]),
         ('scan2',
          [('text', text, ['file2.txt'])])],
        {}, [])}

GOOD_DATASETS = ['basic.api', 'multi.api', 'basic.direct', 'multi.direct']
MUTABLE_DATASETS = ['basic.api', 'multi.api', 'basic.direct', 'multi.direct']

# ------------------------------------
# Pytest fixtures and helper functions
# ------------------------------------

DOCKER_BUILD_DIR = Path(__file__).parent / 'docker-build'
DOCKER_XNAT_ARCHIVE_DIR = Path(__file__).parent / 'xnat_archive_dir'
DOCKER_IMAGE = 'arcana-xnat'
DOCKER_HOST = 'localhost'
DOCKER_XNAT_PORT = '8989'
DOCKER_REGISTRY_IMAGE = 'registry'
DOCKER_REGISTRY_CONTAINER = 'arcana-docker-registry'
DOCKER_NETWORK_NAME = 'arcana'
DOCKER_REGISTRY_PORT = '5959'
DOCKER_XNAT_URI = f'http://{DOCKER_HOST}:{DOCKER_XNAT_PORT}'
DOCKER_XNAT_USER = 'admin'
DOCKER_XNAT_PASSWORD = 'admin'
CONNECTION_ATTEMPTS = 20
CONNECTION_ATTEMPT_SLEEP = 5


@pytest.fixture(params=GOOD_DATASETS, scope='session')
def xnat_dataset(xnat_repository, xnat_archive_dir, request):
    dataset_name, access_method = request.param.split('.')
    with connect() as login:
        if project_name(dataset_name,
                        xnat_repository.run_prefix) not in login.projects:
            create_dataset_in_repo(dataset_name, xnat_repository.run_prefix)    
    return access_dataset(xnat_repository, dataset_name, access_method,
                          xnat_archive_dir)    


@pytest.fixture(params=MUTABLE_DATASETS, scope='function')
def mutable_xnat_dataset(xnat_repository, xnat_archive_dir, request):
    return make_mutable_dataset(xnat_repository, xnat_archive_dir, request.param)


@pytest.fixture(scope='session')
def xnat_archive_dir():
    return DOCKER_XNAT_ARCHIVE_DIR


@pytest.fixture(scope='session')
def xnat_repository(xnat_archive_dir, run_prefix, xnat_docker_network):

    container, already_running = start_xnat_repository(xnat_archive_dir,
                                                       xnat_docker_network)

    repository = Xnat(
        server=DOCKER_XNAT_URI,
        user=DOCKER_XNAT_USER,
        password=DOCKER_XNAT_PASSWORD,
        cache_dir=mkdtemp())

    # Stash a project prefix in the repository object
    repository.run_prefix = run_prefix if already_running else None

    yield repository

    # If container was run by this fixture (instead of already running) stop
    # it afterwards
    if not already_running:
        container.stop()


def start_xnat_repository(xnat_archive_dir=DOCKER_XNAT_ARCHIVE_DIR,
                          xnat_docker_network=None):
    if xnat_docker_network is None:
        xnat_docker_network = get_xnat_docker_network()

    dc = docker.from_env()

    try:
        image = dc.images.get(DOCKER_IMAGE)
    except docker.errors.ImageNotFound:
        image, _ = dc.images.build(path=str(DOCKER_BUILD_DIR), tag=DOCKER_IMAGE)
    
    try:
        container = dc.containers.get(DOCKER_IMAGE)
    except docker.errors.NotFound:
        # Clear the XNAT archive dir
        shutil.rmtree(xnat_archive_dir, ignore_errors=True)
        os.mkdir(xnat_archive_dir)
        container = dc.containers.run(
            image.tags[0], detach=True, ports={
                '80/tcp': DOCKER_XNAT_PORT},
            remove=True, name=DOCKER_IMAGE,
            # Expose the XNAT archive dir outside of the XNAT docker container
            # to simulate what the XNAT container service exposes to running
            # pipelines, and the Docker socket for the container service to
            # to use
            network=xnat_docker_network.id,
            volumes={str(xnat_archive_dir): {'bind': '/data/xnat/archive',
                                             'mode': 'rw'},
                     '/var/run/docker.sock': {'bind': '/var/run/docker.sock',
                                              'mode': 'rw'}})
        already_running = False
    else:
        already_running = True
    return container, already_running



@pytest.fixture(scope='session')
def run_prefix():
    "A datetime string used to avoid stale data left over from previous tests"
    return datetime.strftime(datetime.now(), '%Y%m%d%H%M%S')


@pytest.fixture(scope='session')
def xnat_respository_uri(xnat_repository):
    return xnat_repository.server


@pytest.fixture(scope='session')
def xnat_container_registry(xnat_repository, xnat_docker_network):
    "Stand up a Docker registry to use with the container service"

    container, already_running = start_xnat_container_registry(xnat_docker_network)

    uri = f'localhost:{DOCKER_REGISTRY_PORT}'

    # Set it to the default registry in the XNAT repository
    with connect(xnat_repository.server) as login:
        login.post('/xapi/docker/hubs/1', json={
            "name": "testregistry",
            "url": f"https://{DOCKER_REGISTRY_CONTAINER}:5000"})

    yield uri

    if not already_running:
        container.stop()

def start_xnat_container_registry(xnat_docker_network=None):
    if xnat_docker_network is None:
        xnat_docker_network = get_xnat_docker_network()
    dc = docker.from_env()
    try:
        image = dc.images.get(DOCKER_REGISTRY_IMAGE)
    except docker.errors.ImageNotFound:
        image = dc.images.pull(DOCKER_REGISTRY_IMAGE)

    try:
        container = dc.containers.get(DOCKER_REGISTRY_CONTAINER)
    except docker.errors.NotFound:
        container = dc.containers.run(
            image.tags[0], detach=True,
            ports={'5000/tcp': DOCKER_REGISTRY_PORT},
            network=xnat_docker_network.id,
            remove=True, name=DOCKER_REGISTRY_CONTAINER)
        already_running = False
    else:
        already_running = True
    return container, already_running

@pytest.fixture(scope='session')
def xnat_docker_network():
    return get_xnat_docker_network()

def get_xnat_docker_network():
    dc = docker.from_env()
    try:
        network = dc.networks.get(DOCKER_NETWORK_NAME)
    except docker.errors.NotFound:
        network = dc.networks.create(DOCKER_NETWORK_NAME)
    return network


def make_mutable_dataset(xnat_repository, xnat_archive_dir, test_name):
    dataset_name, access_method = test_name.split('.')
    test_suffix = 'mutable' + access_method + str(hex(random.getrandbits(16)))[2:]
    # Need to create a new dataset per function so it can be safely modified
    # by the test without messing up other tests.
    create_dataset_in_repo(dataset_name, xnat_repository.run_prefix,
                           test_suffix=test_suffix)
    return access_dataset(xnat_repository, dataset_name, access_method,
                          xnat_archive_dir, test_suffix)


def project_name(dataset_name, run_prefix=None, test_suffix=''):
    return (run_prefix if run_prefix else '') + dataset_name + test_suffix


def access_dataset(repository, dataset_name, access_method, xnat_archive_dir,
                   test_suffix=''):
    blueprint = TEST_DATASET_BLUEPRINTS[dataset_name]
    proj_name = project_name(dataset_name, repository.run_prefix, test_suffix)
    if access_method == 'direct':
        proj_dir = xnat_archive_dir / proj_name / 'arc001'
        mounts = {(Clinical.dataset, None): proj_dir}
        for sess_label in proj_dir.iterdir():
            mounts[(Clinical.session, sess_label.name)] = proj_dir / sess_label
    elif access_method == 'api':
        mounts = {}
    else:
        assert False    
    dataset = repository.dataset(proj_name,
                                 id_inference=blueprint.id_inference,
                                 access_args={'mounts': mounts})
    # Stash the args used to create the dataset in attributes so they can be
    # used by tests
    dataset.blueprint = blueprint
    dataset.access_method = access_method
    return dataset


def create_dataset_in_repo(dataset_name, run_prefix, test_suffix=''):
    """
    Creates dataset for each entry in dataset_structures
    """
    blueprint  =  TEST_DATASET_BLUEPRINTS[dataset_name]
    proj_name = project_name(dataset_name, run_prefix, test_suffix)

    with connect() as login:
        login.put(f'/data/archive/projects/{proj_name}')
    
    with connect() as login:
        xproject = login.projects[proj_name]
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


@contextlib.contextmanager
def connect(server=DOCKER_XNAT_URI, user=DOCKER_XNAT_USER,
            password=DOCKER_XNAT_PASSWORD):
    # Need to give time for XNAT to get itself ready after it has
    # started so we try multiple times until giving up trying to connect
    attempts = 0
    for _ in range(1, CONNECTION_ATTEMPTS + 1):
        try:
            login = xnat.connect(server, user=user, password=password)
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
