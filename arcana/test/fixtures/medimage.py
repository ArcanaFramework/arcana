from datetime import datetime
from dataclasses import dataclass
from pathlib import Path
import random
from tempfile import mkdtemp
from itertools import product
import typing as ty
import pytest
import docker
import xnat4tests
from arcana.data.stores.medimage.xnat.api import Xnat
from arcana.data.stores.medimage.xnat.cs import XnatViaCS
from arcana.data.spaces.medimage import Clinical
from arcana.core.data.space import DataSpace
from arcana.data.formats.common import Text, Directory
from arcana.data.formats.medimage import NiftiGzX, NiftiGz, Dicom
from arcana.test.datasets import create_test_file


@pytest.fixture
def nifti_sample_dir():
    return Path(__file__).parent.parent.parent.parent / 'test-data'/ 'nifti'


# -----------------------
# Test dataset structures
# -----------------------

@dataclass
class TestDatasetBlueprint():

    dim_lengths: ty.List[int]
    scans: ty.List[ty.Tuple[str, ty.List[ty.Tuple[str, type, ty.List[str]]]]]
    id_inference: ty.Dict[DataSpace, str]
    to_insert: ty.List[ty.Tuple[str, ty.Tuple[DataSpace, type, ty.List[str]]]]  # files to insert as derivatives


TEST_DATASET_BLUEPRINTS = {
    'basic': TestDatasetBlueprint(  # dataset name
        [1, 1, 3],  # number of timepoints, groups and members respectively
        [('scan1',  # scan type (ID is index)
          [('Text', # resource name
            Text,  # Data format
            ['file.txt'])]),  # name files to place within resource
         ('scan2',
          [('NiftiGzX',
            NiftiGzX,
            ['file.nii.gz', 'file.json'])]),
         ('scan3',
          [('Directory',
            Directory,
            ['doubledir', 'dir', 'file.dat'])]),
         ('scan4',
          [('DICOM', Dicom, ['file1.dcm', 'file2.dcm', 'file3.dcm']),
           ('NIFTI', NiftiGz, ['file1.nii.gz']),
           ('BIDS', None, ['file1.json']),
           ('SNAPSHOT', None, ['file1.png'])])],
        [],
        [('deriv1', Clinical.timepoint, Text, ['file.txt']),
         ('deriv2', Clinical.subject, NiftiGzX, ['file.nii.gz', 'file.json']),
         ('deriv3', Clinical.batch, Directory, ['dir']),
         ('deriv4', Clinical.dataset, Text, ['file.txt']),
         ]),  # id_inference dict
    'multi': TestDatasetBlueprint(  # dataset name
        [2, 2, 2],  # number of timepoints, groups and members respectively
        [
            ('scan1',
             [('Text',  # resource name
               Text, 
               ['file.txt'])])],
        [('subject', r'group(?P<group>\d+)member(?P<member>\d+)'),
         ('session', r'timepoint(?P<timepoint>\d+).*')],  # id_inference dict
        [
         ('deriv1', Clinical.session, Text, ['file.txt']),
         ('deriv2', Clinical.subject, NiftiGzX, ['file.nii.gz', 'file.json']),
         ('deriv3', Clinical.timepoint, Directory, ['doubledir']),
         ('deriv4', Clinical.member, Text, ['file.txt']),
         ('deriv5', Clinical.dataset, Text, ['file.txt']),
         ('deriv6', Clinical.batch, Text, ['file.txt']),
         ('deriv7', Clinical.matchedpoint, Text, ['file.txt']),
         ('deriv8', Clinical.group, Text, ['file.txt']),
         ]),
    'concatenate_test': TestDatasetBlueprint(
        [1, 1, 2],
        [('scan1',
          [('Text', Text, ['file1.txt'])]),
         ('scan2',
          [('Text', Text, ['file2.txt'])])],
        {}, [])}

GOOD_DATASETS = ['basic.api', 'multi.api', 'basic.direct', 'multi.direct']
MUTABLE_DATASETS = ['basic.api', 'multi.api', 'basic.direct', 'multi.direct']

# ------------------------------------
# Pytest fixtures and helper functions
# ------------------------------------


@pytest.fixture(params=GOOD_DATASETS, scope='session')
def xnat_dataset(xnat_repository, xnat_archive_dir, request):
    dataset_name, access_method = request.param.split('.')
    with xnat4tests.connect() as login:
        if project_name(dataset_name,
                        xnat_repository.run_prefix) not in login.projects:
            create_dataset_data_in_repo(dataset_name, xnat_repository.run_prefix)    
    return access_dataset(xnat_repository, dataset_name, access_method,
                          xnat_archive_dir)    


@pytest.fixture(params=MUTABLE_DATASETS, scope='function')
def mutable_xnat_dataset(xnat_repository, xnat_archive_dir, request):
    return make_mutable_dataset(xnat_repository, xnat_archive_dir, request.param)


@pytest.fixture(scope='session')
def xnat_root_dir():
    return xnat4tests.config.XNAT_ROOT_DIR


@pytest.fixture(scope='session')
def xnat_archive_dir(xnat_root_dir):
    return xnat_root_dir / 'archive'


@pytest.fixture(scope='session')
def xnat_repository(run_prefix):

    xnat4tests.launch_xnat()

    repository = Xnat(
        server=xnat4tests.config.XNAT_URI,
        user=xnat4tests.config.XNAT_USER,
        password=xnat4tests.config.XNAT_PASSWORD,
        cache_dir=mkdtemp())

    # Stash a project prefix in the repository object
    repository.run_prefix = run_prefix

    yield repository


@pytest.fixture(scope='session')
def run_prefix():
    "A datetime string used to avoid stale data left over from previous tests"
    return datetime.strftime(datetime.now(), '%Y%m%d%H%M%S')


@pytest.fixture(scope='session')
def xnat_respository_uri(xnat_repository):
    return xnat_repository.server


@pytest.fixture(scope='session')
def xnat_container_registry():
    "Stand up a Docker registry to use with the container service"

    xnat4tests.launch_docker_registry()

    return 'localhost'  # {DOCKER_REGISTRY_PORT}  # Needs to be on 80 to avoid bug with ':' in URI 


def make_mutable_dataset(xnat_repository, xnat_archive_dir, test_name):
    dataset_name, access_method = test_name.split('.')
    test_suffix = 'mutable' + access_method + str(hex(random.getrandbits(16)))[2:]
    # Need to create a new dataset per function so it can be safely modified
    # by the test without messing up other tests.
    create_dataset_data_in_repo(dataset_name, xnat_repository.run_prefix,
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
        # Create a new repository access object that accesses data directly
        # via the XNAT archive directory, like 
        proj_dir = xnat_archive_dir / proj_name / 'arc001'
        repository = XnatViaCS(
            server=repository.server,
            user=repository.user,
            password=repository.password,
            cache_dir=repository.cache_dir,
            frequency=Clinical.dataset,
            input_mount=proj_dir,
            output_mount=Path(mkdtemp()))
    elif access_method != 'api':
        assert False
    
    dataset = repository.new_dataset(proj_name,
                                     id_inference=blueprint.id_inference)
    # Stash the args used to create the dataset in attributes so they can be
    # used by tests
    dataset.blueprint = blueprint
    dataset.access_method = access_method
    return dataset


def create_dataset_data_in_repo(dataset_name, run_prefix='', test_suffix=''):
    """
    Creates dataset for each entry in dataset_structures
    """
    blueprint  =  TEST_DATASET_BLUEPRINTS[dataset_name]
    proj_name = project_name(dataset_name, run_prefix, test_suffix)

    with xnat4tests.connect() as login:
        login.put(f'/data/archive/projects/{proj_name}')
    
    with xnat4tests.connect() as login:
        xproject = login.projects[proj_name]
        xclasses = login.classes
        for id_tple in product(*(list(range(d))
                                 for d in blueprint.dim_lengths)):
            ids = dict(zip(Clinical.axes(), id_tple))
            # Create subject
            subject_label = ''.join(
                f'{b}{ids[b]}' for b in Clinical.subject.span())
            xsubject = xclasses.SubjectData(label=subject_label,
                                            parent=xproject)
            # Create session
            session_label = ''.join(
                f'{b}{ids[b]}' for b in Clinical.session.span())
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
