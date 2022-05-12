from datetime import datetime
from dataclasses import dataclass
from pathlib import Path
import random
from tempfile import mkdtemp
from itertools import product
import typing as ty
from numpy import real
import pytest
import docker
import xnat4tests
from arcana.data.stores.common import FileSystem
from arcana.data.stores.medimage.xnat.api import Xnat
from arcana.data.stores.medimage.xnat.cs import XnatViaCS
from arcana.data.spaces.medimage import Clinical
from arcana.core.data.space import DataSpace
from arcana.data.formats.common import Text, Directory
from arcana.data.formats.medimage import NiftiGzX, NiftiGz, Dicom
from arcana.test.datasets import create_test_file


@pytest.fixture(scope='session')
def nifti_sample_dir():
    return Path(__file__).parent.parent.parent.parent / 'test-data'/ 'nifti'


TEST_DICOM_DATASET_DIR = Path(__file__).parent / 'test-dataset'

@pytest.fixture(scope='session')
def dicom_dataset(test_dicom_dataset_dir):
    return FileSystem().dataset(
        test_dicom_dataset_dir,
        hierarchy=['session'])


@pytest.fixture(scope='session')
def test_dicom_dataset_dir():
    return TEST_DICOM_DATASET_DIR


# -----------------------
# Test dataset structures
# -----------------------


@dataclass
class ResourceBlueprint():

    name: str
    format: type
    filenames: ty.List[str]


@dataclass
class ScanBlueprint():

    name: str
    resources: ty.List[ResourceBlueprint]

@dataclass
class DerivBlueprint():

    name: str
    frequency: Clinical
    format: type
    filenames: ty.List[str]

@dataclass
class TestXnatDatasetBlueprint():

    dim_lengths: ty.List[int]
    scans: ty.List[ScanBlueprint]
    id_inference: ty.Dict[str, str]
    derivatives: ty.List[DerivBlueprint]  # files to insert as derivatives



TEST_XNAT_DATASET_BLUEPRINTS = {
    'basic': TestXnatDatasetBlueprint(  # dataset name
        [1, 1, 3],  # number of timepoints, groups and members respectively
        [ScanBlueprint('scan1',  # scan type (ID is index)
          [ResourceBlueprint(
              'Text', # resource name
              Text,  # Data format
              ['file.txt'])]),  # name files to place within resource
         ScanBlueprint('scan2',
          [ResourceBlueprint(
              'NiftiGzX',
              NiftiGzX,
              ['file.nii.gz', 'file.json'])]),
         ScanBlueprint('scan3',
          [ResourceBlueprint(
              'Directory',
              Directory,
              ['doubledir', 'dir', 'file.dat'])]),
         ScanBlueprint('scan4',
          [ResourceBlueprint('DICOM', Dicom, ['file1.dcm', 'file2.dcm', 'file3.dcm']),
           ResourceBlueprint('NIFTI', NiftiGz, ['file1.nii.gz']),
           ResourceBlueprint('BIDS', None, ['file1.json']),
           ResourceBlueprint('SNAPSHOT', None, ['file1.png'])])],
        [],
        [DerivBlueprint('deriv1', Clinical.timepoint, Text, ['file.txt']),
         DerivBlueprint('deriv2', Clinical.subject, NiftiGzX, ['file.nii.gz', 'file.json']),
         DerivBlueprint('deriv3', Clinical.batch, Directory, ['dir']),
         DerivBlueprint('deriv4', Clinical.dataset, Text, ['file.txt']),
         ]),  # id_inference dict
    'multi': TestXnatDatasetBlueprint(  # dataset name
        [2, 2, 2],  # number of timepoints, groups and members respectively
        [ScanBlueprint('scan1', [ResourceBlueprint('Text', Text, ['file.txt'])])],
        [('subject', r'group(?P<group>\d+)member(?P<member>\d+)'),
         ('session', r'timepoint(?P<timepoint>\d+).*')],  # id_inference dict
        [
         DerivBlueprint('deriv1', Clinical.session, Text, ['file.txt']),
         DerivBlueprint('deriv2', Clinical.subject, NiftiGzX, ['file.nii.gz', 'file.json']),
         DerivBlueprint('deriv3', Clinical.timepoint, Directory, ['doubledir']),
         DerivBlueprint('deriv4', Clinical.member, Text, ['file.txt']),
         DerivBlueprint('deriv5', Clinical.dataset, Text, ['file.txt']),
         DerivBlueprint('deriv6', Clinical.batch, Text, ['file.txt']),
         DerivBlueprint('deriv7', Clinical.matchedpoint, Text, ['file.txt']),
         DerivBlueprint('deriv8', Clinical.group, Text, ['file.txt']),
         ]),
    'concatenate_test': TestXnatDatasetBlueprint(
        [1, 1, 2],
        [
            ScanBlueprint(
                'scan1',
                [ResourceBlueprint('Text', Text, ['file1.txt'])]),
            ScanBlueprint(
                'scan2',
                [ResourceBlueprint('Text', Text, ['file2.txt'])])],
        {},
        [DerivBlueprint('concatenated', Clinical.session, Text, ['concatenated.txt'])])}

GOOD_DATASETS = ['basic.api', 'multi.api', 'basic.cs', 'multi.cs']
MUTABLE_DATASETS = ['basic.api', 'multi.api', 'basic.cs', 'multi.cs']

# ------------------------------------
# Pytest fixtures and helper functions
# ------------------------------------


@pytest.fixture(params=GOOD_DATASETS, scope='session')
def xnat_dataset(xnat_repository, xnat_archive_dir, request):
    dataset_name, access_method = request.param.split('.')
    blueprint = TEST_XNAT_DATASET_BLUEPRINTS[dataset_name]
    with xnat4tests.connect() as login:
        if project_name(dataset_name,
                        xnat_repository.run_prefix) not in login.projects:
            create_dataset_data_in_repo(dataset_name, blueprint, xnat_repository.run_prefix)    
    return access_dataset(dataset_name=dataset_name,
                          blueprint=blueprint,
                          xnat_repository=xnat_repository,
                          xnat_archive_dir=xnat_archive_dir,
                          access_method=access_method)    


@pytest.fixture(params=MUTABLE_DATASETS, scope='function')
def mutable_xnat_dataset(xnat_repository, xnat_archive_dir, request):
    dataset_name, access_method = request.param.split('.')
    blueprint = TEST_XNAT_DATASET_BLUEPRINTS[dataset_name]
    return make_mutable_dataset(dataset_name=dataset_name,
                                blueprint=blueprint,
                                xnat_repository=xnat_repository,
                                xnat_archive_dir=xnat_archive_dir,
                                access_method=access_method)


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


def make_mutable_dataset(dataset_name: str, blueprint: TestXnatDatasetBlueprint, xnat_repository: Xnat,
                         xnat_archive_dir: Path, access_method: str, source_data: Path=None):
    """Create a dataset (project) in the test XNAT repository
    """
    test_suffix = 'mutable' + access_method + str(hex(random.getrandbits(16)))[2:]
    # Need to create a new dataset per function so it can be safely modified
    # by the test without messing up other tests.
    create_dataset_data_in_repo(dataset_name=dataset_name,
                                blueprint=blueprint,
                                run_prefix=xnat_repository.run_prefix,
                                test_suffix=test_suffix,
                                source_data=source_data)
    return access_dataset(xnat_repository=xnat_repository,
                          dataset_name=dataset_name,
                          blueprint=blueprint,
                          access_method=access_method,
                          xnat_archive_dir=xnat_archive_dir,
                          test_suffix=test_suffix)


def project_name(dataset_name: str, run_prefix: str=None, test_suffix: str=''):
    return (run_prefix if run_prefix else '') + dataset_name + test_suffix


def access_dataset(xnat_repository: Xnat, dataset_name: str, blueprint: TestXnatDatasetBlueprint,
                   access_method: str, xnat_archive_dir: Path, test_suffix: str=''):
    proj_name = project_name(dataset_name, xnat_repository.run_prefix, test_suffix)
    if access_method == 'cs':
        # Create a new repository access object that accesses data directly
        # via the XNAT archive directory, like 
        proj_dir = xnat_archive_dir / proj_name / 'arc001'
        xnat_repository = XnatViaCS(
            server=xnat_repository.server,
            user=xnat_repository.user,
            password=xnat_repository.password,
            cache_dir=xnat_repository.cache_dir,
            frequency=Clinical.dataset,
            input_mount=proj_dir,
            output_mount=Path(mkdtemp()))
    elif access_method != 'api':
        assert False
    
    dataset = xnat_repository.new_dataset(proj_name, id_inference=blueprint.id_inference)
    # Stash the args used to create the dataset in attributes so they can be
    # used by tests
    dataset.blueprint = blueprint
    dataset.access_method = access_method
    return dataset


def create_dataset_data_in_repo(dataset_name: str, blueprint: TestXnatDatasetBlueprint,
                                run_prefix: str='', test_suffix: str='', source_data: Path=None):
    """
    Creates dataset for each entry in dataset_structures
    """
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
            
            for i, scan in enumerate(blueprint.scans, start=1):
                # Create scan
                xscan = xclasses.MrScanData(id=i, type=scan.name,
                                            parent=xsession)
                for resource in scan.resources:

                    tmp_dir = Path(mkdtemp())
                    # Create the resource
                    xresource = xscan.create_resource(resource.name)
                    # Create the dummy files
                    for fname in resource.filenames:
                        if source_data is not None:
                            fpath = source_data.joinpath(*fname.split('/'))
                            target_fpath = fpath.name
                        else:
                            fpath = create_test_file(fname, tmp_dir)
                            target_fpath = str(fpath)
                        xresource.upload(str(tmp_dir / fpath), target_fpath)
