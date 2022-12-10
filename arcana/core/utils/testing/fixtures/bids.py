import os
import pytest
import docker
from arcana.data.types.medimage import NiftiGzX
from arcana.core.deploy.image.components import BaseImage


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
    for inpt_path, datatype in [
        ("anat/T1w", NiftiGzX),
        ("anat/T2w", NiftiGzX),
        ("dwi/dwi", NiftiGzX),
    ]:
        subdir, suffix = inpt_path.split("/")
        file_tests += f"""
        if [ ! -f "$BIDS_DATASET/sub-${{SUBJ_ID}}/{subdir}/sub-${{SUBJ_ID}}_{suffix}.{datatype.ext}" ]; then
            echo "Did not find {suffix} file at $BIDS_DATASET/sub-${{SUBJ_ID}}/{subdir}/sub-${{SUBJ_ID}}_{suffix}.{datatype.ext}"
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
        base_image=BaseImage().reference,
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
    inputs = {
        "T1w": {
            "configuration": {
                "path": "anat/T1w",
            },
            "datatype": "medimage:NiftiGzX",
            "help_string": "T1-weighted image",
        },
        "T2w": {
            "configuration": {
                "path": "anat/T2w",
            },
            "datatype": "medimage:NiftiGzX",
            "help_string": "T2-weighted image",
        },
        "DWI": {
            "configuration": {
                "path": "dwi/dwi",
            },
            "datatype": "medimage:NiftiGzXFslgrad",
            "help_string": "DWI-weighted image",
        },
    }

    outputs = {
        "file1": {
            "configuration": {
                "path": "file1",
            },
            "datatype": "common:Text",
            "help_string": "an output file",
        },
        "file2": {
            "configuration": {
                "path": "file2",
            },
            "datatype": "common:Text",
            "help_string": "another output file",
        },
    }

    return {
        "task": "arcana.analysis.tasks.bids.app:bids_app",
        "inputs": inputs,
        "outputs": outputs,
        "row_frequency": "session",
        "configuration": {
            "inputs": inputs,
            "outputs": outputs,
            "executable": str(mock_bids_app_executable),
        },
    }
