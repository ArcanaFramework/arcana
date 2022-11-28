import pytest
from pathlib import Path
import docker
import docker.errors
import yaml
from arcana.core.test.utils import show_cli_trace
from arcana.cli.deploy import build


def create_spec_file(name: str, work_dir: Path):

    root_dir = work_dir / ORG
    root_dir.mkdir()
    spec_file = root_dir / (name + ".yaml")

    with open(spec_file, "w") as f:
        yaml.dump(TEST_IMAGE_SPEC, f)

    return root_dir


@pytest.fixture
def license_file(work_dir):
    license_src = work_dir / "license_file.txt"

    with open(license_src, "w") as f:
        f.write(LICENSE_CONTENTS)

    return license_src


def run_license_check(image_tag: str, work_dir: Path):

    dc = docker.from_env()

    dataset_dir = work_dir / "dataset"
    sample_dir = dataset_dir / "sample1"
    sample_dir.mkdir(parents=True)

    INPUT_NAME = "contents-file"
    OUTPUT_NAME = "validated-file"

    with open(sample_dir / (INPUT_NAME + ".txt"), "w") as f:
        f.write(LICENSE_CONTENTS)

    args = (
        "file///dataset "
        f"--input {LICENSE_INPUT_FIELD} '{INPUT_NAME}' "
        f"--output {LICENSE_OUTPUT_FIELD} '{OUTPUT_NAME}' "
        f"--parameter {LICENSE_PATH_PARAM} '{LICENSE_PATH}' "
    )

    try:
        result = dc.containers.run(
            image_tag,
            args,
            volumes=[f"{str(dataset_dir)}:/dataset:rw"],
            remove=True,
            stderr=True,
        )
    except docker.errors.ContainerError as e:
        raise RuntimeError(
            f"Running {image_tag} failed with args = {args}\n\n{e.stderr.decode('utf-8')}"
        )

    return result


def test_buildtime_license(license_file, run_prefix: str, work_dir: Path, cli_runner):

    # Create pipeline
    image_name = f"license-buildtime-{run_prefix}"
    image_tag = f"{REGISTRY}/{ORG}/{image_name}:{IMAGE_VERSION}-0"

    root_dir = create_spec_file(image_name, work_dir)

    build_dir = work_dir / "build"

    result = cli_runner(
        build,
        args=[
            "common:PipelineImage",
            str(root_dir),
            "--build-dir",
            str(build_dir),
            "--license",
            LICENSE_NAME,
            str(license_file),
            "--use-local-packages",
            "--install-extras",
            "test",
            "--raise-errors",
            "--registry",
            REGISTRY,
        ],
    )

    assert result.exit_code == 0, show_cli_trace(result)

    assert result.stdout.strip().splitlines()[-1] == image_tag
    assert run_license_check(image_tag, work_dir)


def test_runtime_license():
    pass


def test_site_runtime_license():
    pass


ORG = "arcana-tests"
REGISTRY = "a.docker.registry.io"
IMAGE_VERSION = "1.0"

LICENSE_PATH = "/path/to/licence.txt"

LICENSE_CONTENTS = "license contents"

LICENSE_NAME = "testlicense"

LICENSE_INPUT_FIELD = "license_file"

LICENSE_OUTPUT_FIELD = "validated_license_file"

LICENSE_PATH_PARAM = "license_contents"

TEST_IMAGE_SPEC = {
    "org": ORG,
    "version": "1.0",
    "authors": [{"name": "Some One", "email": "some.one@an.email.org"}],
    "info_url": "http://concatenate.readthefakedocs.io",
    "description": "A test of the license installation",
    "system_packages": [],
    "python_packages": [],
    "readme": "This is a test README",
    "licenses": {
        LICENSE_NAME: {
            "destination": LICENSE_PATH,
            "info_url": "http://license.test",
            "description": "This is a license to test the build structure",
        }
    },
    "command": {
        "task": "arcana.core.test.tasks:check_license",
        "data_space": "common:Samples",
        "row_frequency": "sample",
        "inputs": [
            {
                "name": LICENSE_INPUT_FIELD,
                "format": "common:Text",
                "task_field": "expected_license_contents",
                "description": "the path to the license",
            },
        ],
        "outputs": [
            {
                "name": LICENSE_OUTPUT_FIELD,
                "format": "common:Text",
                "task_field": "out",
                "description": "the validated license path",
            }
        ],
        "parameters": [
            {
                "name": LICENSE_PATH_PARAM,
                "type": "str",
                "task_field": "expected_license_path",
                "required": True,
                "description": "the expected contents of the license file",
            }
        ],
    },
}
