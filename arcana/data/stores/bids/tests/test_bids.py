import os
import stat
import json
import tempfile
from pathlib import Path
import nibabel as nb
import numpy.random
import shutil
import docker
from arcana import __version__
from arcana.data.formats import NiftiX
from arcana.data.stores.bids import BidsDataset
from arcana.tasks.bids import BidsApp
from arcana.data.formats.common import Text, Directory
from arcana.data.formats.medimage import NiftiGzX, NiftiGzXFslgrad


BIDS_VALIDATOR_DOCKER = 'bids/validator'
SUCCESS_STR = 'This dataset appears to be BIDS compatible'
MOCK_BIDS_APP_IMAGE = 'arcana-mock-bids-app'
MOCK_BIDS_APP_NAME = 'mockapp'
MOCK_README = 'A dummy readme\n' * 100
MOCK_AUTHORS = ['Dumm Y. Author',
                'Another D. Author']

def test_bids_roundtrip(work_dir):

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

    for node in dataset.nodes(frequency='session'):
        item = node['t1w']
        item.put(dummy_nifti, dummy_json)

    # Full dataset validation using dockerized validator
    dc = docker.from_env()
    dc.images.pull(BIDS_VALIDATOR_DOCKER)
    result = dc.containers.run(BIDS_VALIDATOR_DOCKER, '/data',
                               volumes=[f'{path}:/data:ro'],
                               remove=True, stderr=True).decode('utf-8')
    assert SUCCESS_STR in result
    
    reloaded = BidsDataset.load(path)
    reloaded.add_sink('t1w', format=NiftiX, path='anat/T1w')

    assert dataset == reloaded


def test_run_bids_app_docker(nifti_sample_dir: Path, work_dir: Path):

    kwargs = {}
    INPUTS = [('T1w', NiftiGzX, 'anat/T1w'),
              ('T2w', NiftiGzX, 'anat/T2w'),
              ('dwi', NiftiGzXFslgrad, 'dwi/dwi'),
            #   ('bold', NiftiGzX, 'func/task-REST_bold')
              ]
    OUTPUTS = [('whole_dir', Directory, None),
               ('out1', Text, f'file1'),
               ('out2', Text, f'file2')]

    dc = docker.from_env()

    dc.images.pull(BIDS_VALIDATOR_DOCKER)

    # Build mock BIDS app image
    build_dir = Path(tempfile.mkdtemp())

    # Create executable that runs validator then produces some mock output
    # files
    launch_sh = build_dir / 'launch.sh'
    with open(launch_sh, 'w') as f:
        f.write(f"""#!/bin/sh
BIDS_DATASET=$1
OUTPUTS_DIR=$2
SUBJ_ID=$5
# Run BIDS validator to check whether BIDS dataset is created properly
output=$(/usr/local/bin/bids-validator "$BIDS_DATASET")
if [[ "$output" != *"{SUCCESS_STR}"* ]]; then
    echo "BIDS validation was not successful, exiting:\n "
    echo $output
    exit 1;
fi
# Write mock output files to 'derivatives' Directory
mkdir -p $OUTPUTS_DIR
echo 'file1' > $OUTPUTS_DIR/sub-${{SUBJ_ID}}_file1.txt
echo 'file2' > $OUTPUTS_DIR/sub-${{SUBJ_ID}}_file2.txt
""")

    with open(build_dir / 'Dockerfile', 'w') as f:
        f.write(f"""FROM {BIDS_VALIDATOR_DOCKER}:latest
ADD ./launch.sh /launch.sh
RUN chmod +x /launch.sh
ENTRYPOINT ["/launch.sh"]""")
    
    dc.images.build(path=str(build_dir), tag=MOCK_BIDS_APP_IMAGE)

    task_interface = BidsApp(
        app_name=MOCK_BIDS_APP_NAME,
        image=MOCK_BIDS_APP_IMAGE,
        executable='/launch.sh',  # Extracted using `docker_image_executable(docker_image)`
        inputs=INPUTS,
        outputs=OUTPUTS)

    for inpt, dtype, _ in INPUTS:
        esc_inpt = inpt
        kwargs[esc_inpt] = nifti_sample_dir / (esc_inpt  + '.' + dtype.ext)

    bids_dir = work_dir / 'bids'

    shutil.rmtree(bids_dir, ignore_errors=True)

    task = task_interface(dataset=bids_dir, virtualisation='docker')
    result = task(plugin='serial', **kwargs)

    for output, dtype, _ in OUTPUTS:
        assert Path(getattr(result.output, output)).exists()


def test_run_bids_app_naked(nifti_sample_dir: Path, work_dir: Path):

    kwargs = {}
    INPUTS = [('T1w', NiftiGzX, 'anat/T1w'),
              ('T2w', NiftiGzX, 'anat/T2w'),
              ('dwi', NiftiGzXFslgrad, 'dwi/dwi'),
            #   ('bold', NiftiGzX, 'func/task-REST_bold')
              ]
    OUTPUTS = [('whole_dir', Directory, None),
               ('out1', Text, f'file1'),
               ('out2', Text, f'file2')]

    dc = docker.from_env()

    dc.images.pull(BIDS_VALIDATOR_DOCKER)

    # Build mock BIDS app image
    build_dir = Path(tempfile.mkdtemp())

    # Create executable that runs validator then produces some mock output
    # files
    launch_sh = build_dir / 'launch.sh'

    # Generate tests to see if input files have been created properly
    file_tests = ''
    for _, dtype, path in INPUTS:
        subdir, suffix = path.split('/')
        file_tests += f"""
        if [ ! -f "$BIDS_DATASET/sub-${{SUBJ_ID}}/{subdir}/sub-${{SUBJ_ID}}_{suffix}.{dtype.ext}" ]; then
            echo "Did not find {suffix} file at $BIDS_DATASET/sub-${{SUBJ_ID}}/{subdir}/sub-${{SUBJ_ID}}_{suffix}.{dtype.ext}"
            exit 1;
        fi
        """
    
    with open(launch_sh, 'w') as f:
        f.write(f"""#!/bin/sh
BIDS_DATASET=$1
OUTPUTS_DIR=$2
SUBJ_ID=$5
{file_tests}
# Write mock output files to 'derivatives' Directory
mkdir -p $OUTPUTS_DIR
echo 'file1' > $OUTPUTS_DIR/sub-${{SUBJ_ID}}_file1.txt
echo 'file2' > $OUTPUTS_DIR/sub-${{SUBJ_ID}}_file2.txt
""")

    os.chmod(launch_sh, stat.S_IRWXU)

    task_interface = BidsApp(
        app_name=MOCK_BIDS_APP_NAME,
        image=MOCK_BIDS_APP_IMAGE,
        executable=launch_sh,  # Extracted using `docker_image_executable(docker_image)`
        inputs=INPUTS,
        outputs=OUTPUTS)

    for inpt, dtype, _ in INPUTS:
        esc_inpt = inpt
        kwargs[esc_inpt] = nifti_sample_dir / (esc_inpt  + '.' + dtype.ext)

    bids_dir = work_dir / 'bids'

    shutil.rmtree(bids_dir, ignore_errors=True)

    task = task_interface(dataset=bids_dir, virtualisation=None)
    result = task(plugin='serial', **kwargs)

    for output, dtype, _ in OUTPUTS:
        assert Path(getattr(result.output, output)).exists()
