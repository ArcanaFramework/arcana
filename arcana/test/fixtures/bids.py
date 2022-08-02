import os
import pytest
import docker
from arcana.data.formats.medimage import NiftiGzX
from arcana.core.deploy.build import DEFAULT_BASE_IMAGE


BIDS_VALIDATOR_DOCKER = "bids/validator:latest"
SUCCESS_STR = "This dataset appears to be BIDS compatible"
MOCK_BIDS_APP_IMAGE = "arcana-mock-bids-app"
BIDS_VALIDATOR_APP_IMAGE = "arcana-bids-validator-app"


@pytest.fixture(scope="session")
def bids_validator_app_script():
    return f"""#!/bin/sh
# Echo inputs to get rid of any quotes
BIDS_DATASET=$(echo $1)
OUTPUTS_DIR=$(echo $2)
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


# FIXME: should be converted to python script to be Windows compatible
@pytest.fixture(scope="session")
def mock_bids_app_script():
    file_tests = ""
    for inpt_path, format in [
        ("anat/T1w", NiftiGzX),
        ("anat/T2w", NiftiGzX),
        ("dwi/dwi", NiftiGzX),
    ]:
        subdir, suffix = inpt_path.split("/")
        file_tests += f"""
        if [ ! -f "$BIDS_DATASET/sub-${{SUBJ_ID}}/{subdir}/sub-${{SUBJ_ID}}_{suffix}.{format.ext}" ]; then
            echo "Did not find {suffix} file at $BIDS_DATASET/sub-${{SUBJ_ID}}/{subdir}/sub-${{SUBJ_ID}}_{suffix}.{format.ext}"
            exit 1;
        fi
        """

    return f"""#!/bin/sh
BIDS_DATASET=$1
OUTPUTS_DIR=$2
SUBJ_ID=$5
{file_tests}
# Write mock output files to 'derivatives' Directory
mkdir -p $OUTPUTS_DIR
echo 'file1' > $OUTPUTS_DIR/sub-${{SUBJ_ID}}_file1.txt
echo 'file2' > $OUTPUTS_DIR/sub-${{SUBJ_ID}}_file2.txt
"""


@pytest.fixture(scope="session")
def mock_bids_app_executable(build_cache_dir, mock_bids_app_script):
    # Create executable that runs validator then produces some mock output
    # files
    script_path = build_cache_dir / "mock-bids-app-executable.sh"
    with open(script_path, "w") as f:
        f.write(mock_bids_app_script)
    os.chmod(script_path, 0o777)
    return script_path


@pytest.fixture(scope="session")
def bids_success_str():
    return SUCCESS_STR


@pytest.fixture(scope="session")
def bids_validator_docker():
    dc = docker.from_env()
    dc.images.pull(BIDS_VALIDATOR_DOCKER)
    return BIDS_VALIDATOR_DOCKER


@pytest.fixture(scope="session")
def bids_validator_app_image(
    bids_validator_app_script, bids_validator_docker, build_cache_dir
):
    return build_app_image(
        BIDS_VALIDATOR_APP_IMAGE,
        bids_validator_app_script,
        build_cache_dir,
        base_image=bids_validator_docker,
    )


@pytest.fixture(scope="session")
def mock_bids_app_image(mock_bids_app_script, build_cache_dir):
    return build_app_image(
        MOCK_BIDS_APP_IMAGE,
        mock_bids_app_script,
        build_cache_dir,
        base_image=DEFAULT_BASE_IMAGE,
    )


def build_app_image(tag_name, script, build_cache_dir, base_image):
    dc = docker.from_env()

    # Create executable that runs validator then produces some mock output
    # files
    build_dir = build_cache_dir / tag_name.replace(":", "__i__")
    build_dir.mkdir()
    launch_sh = build_dir / "launch.sh"
    with open(launch_sh, "w") as f:
        f.write(script)

    # Build mock BIDS app image
    with open(build_dir / "Dockerfile", "w") as f:
        f.write(
            f"""FROM {base_image}
ADD ./launch.sh /launch.sh
RUN chmod +x /launch.sh
ENTRYPOINT ["/launch.sh"]"""
        )

    dc.images.build(path=str(build_dir), tag=tag_name)

    return tag_name


@pytest.fixture(scope="session")
def bids_command_spec(mock_bids_app_executable):
    inputs = [
        {"name": "T1w", "path": "anat/T1w", "format": "medimage:NiftiGzX"},
        {"name": "T2w", "path": "anat/T2w", "format": "medimage:NiftiGzX"},
        {"name": "DWI", "path": "dwi/dwi", "format": "medimage:NiftiGzXFslgrad"},
    ]

    outputs = [
        {"name": "file1", "path": "file1", "format": "common:Text"},
        {"name": "file2", "path": "file2", "format": "common:Text"},
    ]

    return {
        "name": "bids-app-test",
        "pydra_task": "arcana.tasks.bids.app:bids_app",
        "inputs": inputs,
        "outputs": outputs,
        "description": "A pipeline to test wrapping of BIDS apps",
        "version": "0.1",
        "row_frequency": "session",
        "info_url": None,
        "configuration": {
            "inputs": inputs,
            "outputs": outputs,
            "executable": str(mock_bids_app_executable),
        },
    }
