import pytest
import os
from unittest.mock import patch
from pathlib import Path
import docker
import docker.errors
from arcana.core.utils.testing import show_cli_trace
from arcana.core.cli.deploy import make, install_license
from arcana.deploy.common import PipelineImage


def test_buildtime_license(
    pipeline_image, license_file, run_prefix: str, work_dir: Path, cli_runner
):

    # Create pipeline
    image_name = f"license-buildtime-{run_prefix}"
    image_tag = f"{REGISTRY}/{ORG}/{image_name}:{IMAGE_VERSION}"

    root_dir = work_dir / ORG
    root_dir.mkdir()
    spec_file = root_dir / (image_name + ".yaml")

    pipeline_image.licenses[0].store_in_image = True
    pipeline_image.save(spec_file)

    build_dir = work_dir / "build"
    dataset_dir = work_dir / "dataset"

    result = cli_runner(
        make,
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
    assert run_license_check(image_tag, dataset_dir)


def test_site_runtime_license(
    pipeline_image, license_file, run_prefix, work_dir, cli_runner
):

    # Build the pipeline without a license installed
    image_name = f"license-site-runtime-{run_prefix}"
    image_tag = f"{REGISTRY}/{ORG}/{image_name}:{IMAGE_VERSION}"

    build_dir = work_dir / "build"
    dataset_dir = work_dir / "dataset"

    pipeline_image.make(
        build_dir=build_dir,
        use_local_packages=True,
        install_extras=["test"],
        raise_errors=True,
        registry=REGISTRY,
    )

    # Install license into the "site-wide" license location (i.e. in $ARCANA_HOME)
    test_home_dir = work_dir / "test-arcana-home"
    with patch.dict(os.environ, {"ARCANA_HOME": str(test_home_dir)}):

        result = cli_runner(install_license, args=[LICENSE_NAME, str(license_file)])
        assert result.exit_code == 0, show_cli_trace(result)
        assert run_license_check(image_tag, dataset_dir)


def test_dataset_runtime_license(license_file, run_prefix, work_dir, cli_runner):

    # Build the pipeline without a license installed
    image_name = f"license-dataset-runtime-{run_prefix}"
    image_tag = f"{REGISTRY}/{ORG}/{image_name}:{IMAGE_VERSION}"

    build_dir = work_dir / "build"
    dataset_dir = work_dir / "dataset"
    dataset_dir.mkdir()

    pipeline_image.make(
        build_dir=build_dir,
        use_local_packages=True,
        install_extras=["test"],
        raise_errors=True,
        registry=REGISTRY,
    )

    result = cli_runner(
        install_license,
        args=[
            LICENSE_NAME,
            str(license_file),
            f"file//{dataset_dir}",
        ],
    )

    assert result.exit_code == 0, show_cli_trace(result)
    assert run_license_check(image_tag, dataset_dir)


def run_license_check(image_tag: str, dataset_dir: Path):

    dc = docker.from_env()

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


@pytest.fixture
def license_file(work_dir) -> Path:
    license_src = work_dir / "license_file.txt"

    with open(license_src, "w") as f:
        f.write(LICENSE_CONTENTS)

    return license_src


@pytest.fixture
def pipeline_image() -> PipelineImage:
    return PipelineImage(
        org=ORG,
        version="1.0",
        authors=[{"name": "Some One", "email": "some.one@an.email.org"}],
        info_url="http://concatenate.readthefakedocs.io",
        description="A test of the license installation",
        readme="This is a test README",
        licenses={
            LICENSE_NAME: {
                "destination": LICENSE_PATH,
                "info_url": "http://license.test",
                "description": "This is a license to test the build structure",
            }
        },
        command={
            "task": "arcana.core.utils.testing.tasks:check_license",
            "row_frequency": "common:Samples[sample]",
            "inputs": [
                {
                    "name": LICENSE_INPUT_FIELD,
                    "datatype": "common:Text",
                    "field": "expected_license_contents",
                    "help_string": "the path to the license",
                },
            ],
            "outputs": [
                {
                    "name": LICENSE_OUTPUT_FIELD,
                    "datatype": "common:Text",
                    "field": "out",
                    "help_string": "the validated license path",
                }
            ],
            "parameters": [
                {
                    "name": LICENSE_PATH_PARAM,
                    "datatype": "str",
                    "field": "expected_license_path",
                    "required": True,
                    "help_string": "the expected contents of the license file",
                }
            ],
        },
    )


ORG = "arcana-tests"
REGISTRY = "a.docker.registry.io"
IMAGE_VERSION = "1.0"

LICENSE_PATH = "/path/to/licence.txt"

LICENSE_CONTENTS = "license contents"

LICENSE_NAME = "testlicense"

LICENSE_INPUT_FIELD = "license_file"

LICENSE_OUTPUT_FIELD = "validated_license_file"

LICENSE_PATH_PARAM = "license_contents"
