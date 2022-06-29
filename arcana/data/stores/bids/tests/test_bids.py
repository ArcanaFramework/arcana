import os
import stat
import typing as ty
import json
from pathlib import Path
import nibabel as nb
import numpy.random
import shutil
from dataclasses import dataclass
import pytest
import docker
from arcana import __version__
from arcana.data.formats import NiftiX
from arcana.data.stores.bids import BidsDataset
from arcana.tasks.bids.app import bids_app, Input, Output
from arcana.data.formats.common import Text, Directory
from arcana.data.formats.medimage import NiftiGzX, NiftiGzXFslgrad


MOCK_BIDS_APP_NAME = 'mockapp'
MOCK_README = 'A dummy readme\n' * 100
MOCK_AUTHORS = ['Dumm Y. Author',
                'Another D. Author']


def test_bids_roundtrip(bids_validator_docker, bids_success_str, work_dir):

    path = work_dir / 'bids-dataset'
    name = 'bids-dataset'

    shutil.rmtree(path, ignore_errors=True)
    dataset = BidsDataset.create(path, name,
                                 subject_ids=[str(i) for i in range(1, 4)],
                                 session_ids=[str(i) for i in range(1, 3)],
                                 readme=MOCK_README,
                                 authors=MOCK_AUTHORS)

    dataset.add_generator_metadata(
        name='arcana', version=__version__,
        description='Dataset was created programmatically from scratch',
        code_url='http://arcana.readthedocs.io')

    dataset.save_metadata()

    dataset.add_sink('t1w', format=NiftiX, path='anat/T1w')

    dummy_nifti = work_dir / 't1w.nii'
    # dummy_nifti_gz = dummy_nifti + '.gz'
    dummy_json = work_dir / 't1w.json'

    N = 10 ** 6

    # Create a random Nifti file to satisfy BIDS parsers
    hdr = nb.Nifti1Header()
    hdr.set_data_shape((10, 10, 10))
    hdr.set_zooms((1., 1., 1.))  # set voxel size
    hdr.set_xyzt_units(2)  # millimeters
    hdr.set_qform(numpy.diag([1,2,3,1]))
    nb.save(nb.Nifti1Image(
        numpy.random.randint(0, 1, size=[10, 10, 10]), hdr.get_best_affine(),
        header=hdr), dummy_nifti)

    with open(dummy_json, 'w') as f:
        json.dump({'test': 'json-file'}, f)

    for row in dataset.rows(row_frequency='session'):
        item = row['t1w']
        item.put(dummy_nifti, dummy_json)

    # Full dataset validation using dockerized validator
    dc = docker.from_env()
    dc.images.pull(bids_validator_docker)
    result = dc.containers.run(bids_validator_docker, '/data',
                               volumes=[f'{path}:/data:ro'],
                               remove=True, stderr=True).decode('utf-8')
    assert bids_success_str in result
    
    reloaded = BidsDataset.load(path)
    reloaded.add_sink('t1w', format=NiftiX, path='anat/T1w')

    assert dataset == reloaded


@dataclass
class JsonEditBlueprint():

    path_re: str  # regular expression for the path
    edit_str: str  # jq script
    orig_dict: dict  # Test JSON dictionary to be edited
    edited_dict: dict  # Dictionary after the edits
    # file_paths: ty.List[str]  # 


JSON_EDIT_TESTS = {
    'basic': JsonEditBlueprint(
        path_re='anat/T.*w',
        edit_str='.a.b += 4',
        orig_dict={'a': {
                   'b': 1.0}},
        edited_dict={'a': {
                   'b': 5.0}}),
    'multiple': JsonEditBlueprint(
        path_re='anat/T.*w',
        edit_str='.a.b += 4 | .a.c[] *= 2',
        orig_dict={'a': {
                   'b': 1.0,
                   'c': [2, 4, 6]}},
        edited_dict={'a': {
                   'b': 5.0,
                   'c': [4, 8, 12]}}),
}


@pytest.fixture(params=JSON_EDIT_TESTS)
def json_edit_blueprint(request):
    return JSON_EDIT_TESTS[request.param]


def test_bids_json_edit(json_edit_blueprint, work_dir):

    bp = json_edit_blueprint  # shorten name
    
    path = work_dir / 'bids-dataset'
    name = 'bids-dataset'

    shutil.rmtree(path, ignore_errors=True)
    dataset = BidsDataset.create(
        path,
        name,
        subject_ids=['1'],
        session_ids=['1'],
        readme=MOCK_README,
        authors=MOCK_AUTHORS,
        json_edits=[(bp.path_re, bp.edit_str)])

    dataset.add_generator_metadata(
        name='arcana', version=__version__,
        description='Dataset was created programmatically from scratch',
        code_url='http://arcana.readthedocs.io')

    dataset.save_metadata()

    dataset.add_sink('t1w', format=NiftiX, path='anat/T1w')

    nifti_fs_path = work_dir / 't1w.nii'
    # dummy_nifti_gz = dummy_nifti + '.gz'
    json_fs_path = work_dir / 't1w.json'

    N = 10 ** 6

    # Create a random Nifti file to satisfy BIDS parsers
    hdr = nb.Nifti1Header()
    hdr.set_data_shape((10, 10, 10))
    hdr.set_zooms((1., 1., 1.))  # set voxel size
    hdr.set_xyzt_units(2)  # millimeters
    hdr.set_qform(numpy.diag([1, 2, 3, 1]))
    nb.save(nb.Nifti1Image(
        numpy.random.randint(0, 1, size=[10, 10, 10]), hdr.get_best_affine(),
        header=hdr), nifti_fs_path)

    with open(json_fs_path, 'w') as f:
        json.dump(bp.orig_dict, f)

    # Get single item in dataset
    item = dataset['t1w'][('ses-1', 'sub-1')]

    # Put file paths in item
    item.put(nifti_fs_path, json_fs_path)

    with open(item.side_car('json')) as f:
        saved_dict = json.load(f)

    assert saved_dict == bp.edited_dict
    

BIDS_INPUTS = [Input('anat/T1w', NiftiGzX),
               Input('anat/T2w', NiftiGzX),
               Input('dwi/dwi', NiftiGzXFslgrad)]
BIDS_OUTPUTS = [Output('whole_dir', Directory),  # whole derivative directory
                Output('a_file', Text, 'file1'),
                Output('another_file', Text, 'file2')]


def test_run_bids_app_docker(bids_validator_app_image: str, nifti_sample_dir: Path, work_dir: Path):

    kwargs = {}



    bids_dir = work_dir / 'bids'

    shutil.rmtree(bids_dir, ignore_errors=True)

    task = bids_app(
        name=MOCK_BIDS_APP_NAME,
        container_image=bids_validator_app_image,
        executable='/launch.sh',  # Extracted using `docker_image_executable(docker_image)`
        inputs=BIDS_INPUTS,
        outputs=BIDS_OUTPUTS,
        dataset=bids_dir)

    for inpt in BIDS_INPUTS:
        kwargs[inpt.name] = nifti_sample_dir.joinpath(*inpt.path.split('/')).with_suffix('.' + inpt.format.ext)

    result = task(plugin='serial', **kwargs)

    for output in BIDS_OUTPUTS:
        assert Path(getattr(result.output, output.name)).exists()


def test_run_bids_app_naked(mock_bids_app_script: str, nifti_sample_dir: Path, work_dir: Path):

    kwargs = {}
    # INPUTS = [Input('anat/T1w', NiftiGzX),
    #           Input('anat/T2w', NiftiGzX),
    #           Input('dwi/dwi', NiftiGzXFslgrad)]
    # OUTPUTS = [Output('', Directory),  # whole derivative directory
    #            Output('file1', Text),
    #            Output('file2', Text)]

    # Build mock BIDS app image

    # Create executable that runs validator then produces some mock output
    # files
    launch_sh = work_dir / 'launch.sh'

    bids_app_output_dir = work_dir / 'output'

    # We don't need to run the full validation in this case as it is already tested by test_run_bids_app_docker
    # so we use the simpler test script.
    with open(launch_sh, 'w') as f:
        f.write(mock_bids_app_script)

    os.chmod(launch_sh, stat.S_IRWXU)

    task = bids_app(
        name=MOCK_BIDS_APP_NAME,
        executable=launch_sh,  # Extracted using `docker_image_executable(docker_image)`
        inputs=BIDS_INPUTS,
        outputs=BIDS_OUTPUTS,
        app_output_dir=bids_app_output_dir)

    for inpt in BIDS_INPUTS:
        kwargs[inpt.name] = nifti_sample_dir.joinpath(*inpt.path.split('/')).with_suffix('.' + inpt.format.ext)

    bids_dir = work_dir / 'bids'

    shutil.rmtree(bids_dir, ignore_errors=True)

    result = task(plugin='serial', **kwargs)

    for output in BIDS_OUTPUTS:
        assert Path(getattr(result.output, output.name)).exists()
