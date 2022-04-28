import pytest
import docker


BIDS_VALIDATOR_DOCKER = 'bids/validator'
SUCCESS_STR = 'This dataset appears to be BIDS compatible'
MOCK_BIDS_APP_IMAGE = 'arcana-mock-bids-app'


MOCK_BIDS_APP_SCRIPT = f"""#!/bin/sh
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
"""


@pytest.fixture(scope='session')
def bids_success_str():
    return SUCCESS_STR


@pytest.fixture(scope='session')
def bids_validator_docker():
    dc = docker.from_env()
    dc.images.pull(BIDS_VALIDATOR_DOCKER)
    return BIDS_VALIDATOR_DOCKER


@pytest.fixture(scope='session')
def mock_bids_app_image(bids_validator_docker, build_cache_dir):
    dc = docker.from_env()

    # Create executable that runs validator then produces some mock output
    # files
    build_dir = build_cache_dir / 'mock_bids_app_image'
    build_dir.mkdir()
    launch_sh = build_dir / 'launch.sh'
    with open(launch_sh, 'w') as f:
        f.write(MOCK_BIDS_APP_SCRIPT)

    # Build mock BIDS app image
    with open(build_dir / 'Dockerfile', 'w') as f:
        f.write(f"""FROM {bids_validator_docker}:latest
ADD ./launch.sh /launch.sh
RUN chmod +x /launch.sh
ENTRYPOINT ["/launch.sh"]""")
    
    dc.images.build(path=str(build_dir), tag=MOCK_BIDS_APP_IMAGE)

    return MOCK_BIDS_APP_IMAGE


@pytest.fixture(scope='session')
def bids_command_spec():
    return {
        'name': 'bids-app-test',
        'workflow': 'arcana.tasks.bids.app:bids_app',
        'inputs': [
            {
                'name': 'first-file',
                'format': 'common:Text',
                'pydra_field': 'in_file1',
                'frequency': 'session'
            },
            {
                'name': 'second-file',
                'format': 'common:Text',
                'pydra_field': 'in_file2',
                'frequency': 'session'
            },
        ],
        'outputs': [
            {
                'path': 'concatenated',
                'format': 'common:Text',
                'pydra_field': 'out_file'
            }
        ],
        'parameters': [
            {
                'name': 'number-of-duplicates',
                'pydra_field': 'duplicates',
                'required': True
            }
        ],
        'description': "A pipeline to test wrapping of BIDS apps",
        'version': '0.1',
        'frequency': 'session',
        'info_url': None}
