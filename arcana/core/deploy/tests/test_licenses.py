import pytest
import os
from unittest.mock import patch
from pathlib import Path
import docker
import docker.errors
from arcana.core.utils.testing import show_cli_trace
from arcana.core.cli.deploy import make_app, install_license
from arcana.core.deploy.image import App
from arcana.core.data.set import Dataset
from arcana.core.data import Samples


def test_buildtime_license(
    simple_store, license_file, run_prefix: str, work_dir: Path, cli_runner
):

    # Create pipeline
    image_name = f"license-buildtime-{run_prefix}"
    image_tag = f"{REGISTRY}/{ORG}/{image_name}:{IMAGE_VERSION}"

    root_dir = work_dir / ORG
    root_dir.mkdir()
    spec_file = root_dir / (image_name + ".yaml")

    LICENSE_PATH = "/path/to/licence.txt"

    pipeline_image = get_pipeline_image(LICENSE_PATH)
    pipeline_image.name = image_name
    pipeline_image.licenses[0].store_in_image = True
    pipeline_image.save(spec_file)

    build_dir = work_dir / "build"
    dataset_dir = work_dir / "dataset"
    dataset = make_dataset(simple_store, dataset_dir)

    result = cli_runner(
        make_app,
        args=[
            str(root_dir),
            "core:App",
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

    args = (
        "simple///dataset "
        f"--input {LICENSE_INPUT_FIELD} '{LICENSE_INPUT_PATH}' "
        f"--output {LICENSE_OUTPUT_FIELD} '{LICENSE_OUTPUT_PATH}' "
        f"--parameter {LICENSE_PATH_PARAM} '{LICENSE_PATH}' "
        f"--plugin serial "
        f"--raise-errors "
    )

    dc = docker.from_env()
    try:
        result = dc.containers.run(
            image_tag,
            args,
            volumes=[
                f"{str(dataset_dir)}:/dataset:rw",
                f"{os.environ['ARCANA_HOME']}:/arcana-home",
            ],
            remove=False,
            stdout=True,
            stderr=True,
        )
    except docker.errors.ContainerError as e:
        logs = e.container.logs().decode("utf-8")
        raise RuntimeError(
            f"Running {image_tag} failed with args = {args}" f"\n\nlogs:\n{logs}",
        )


def test_site_runtime_license(simple_store, license_file, work_dir, cli_runner):

    # build_dir = work_dir / "build"
    dataset_dir = work_dir / "dataset"

    LICENSE_PATH = work_dir / "license_location"

    pipeline_image = get_pipeline_image(LICENSE_PATH)

    # Install license into the "site-wide" license location (i.e. in $ARCANA_HOME)
    test_home_dir = work_dir / "test-arcana-home"
    with patch.dict(os.environ, {"ARCANA_HOME": str(test_home_dir)}):

        # Save it into the new home directory
        simple_store.save()
        dataset = make_dataset(simple_store, dataset_dir)

        result = cli_runner(
            install_license, args=[LICENSE_NAME, str(license_file), simple_store.name]
        )
        assert result.exit_code == 0, show_cli_trace(result)

        pipeline_image.command.execute(
            dataset.locator,
            input_values={LICENSE_INPUT_FIELD: LICENSE_INPUT_PATH},
            output_values={LICENSE_OUTPUT_FIELD: LICENSE_OUTPUT_PATH},
            parameter_values={LICENSE_PATH_PARAM: LICENSE_PATH},
            work_dir=work_dir / "pipeline",
            raise_errors=True,
            plugin="serial",
            loglevel="info",
        )


def test_dataset_runtime_license(
    simple_store, license_file, run_prefix, work_dir, cli_runner
):

    # build_dir = work_dir / "build"
    dataset_dir = work_dir / "dataset"

    dataset = make_dataset(simple_store, dataset_dir)

    LICENSE_PATH = work_dir / "license_location"
    pipeline_image = get_pipeline_image(LICENSE_PATH)

    result = cli_runner(
        install_license,
        args=[
            LICENSE_NAME,
            str(license_file),
            dataset.locator,
        ],
    )

    assert result.exit_code == 0, show_cli_trace(result)

    pipeline_image.command.execute(
        dataset.locator,
        input_values={LICENSE_INPUT_FIELD: LICENSE_INPUT_PATH},
        output_values={LICENSE_OUTPUT_FIELD: LICENSE_OUTPUT_PATH},
        parameter_values={LICENSE_PATH_PARAM: LICENSE_PATH},
        work_dir=work_dir / "pipeline",
        raise_errors=True,
        plugin="serial",
        loglevel="info",
    )


def get_pipeline_image(license_path) -> App:
    return App(
        name="to_be_overridden",
        org=ORG,
        version="1.0",
        authors=[{"name": "Some One", "email": "some.one@an.email.org"}],
        info_url="http://concatenate.readthefakedocs.io",
        description="A test of the license installation",
        readme="This is a test README",
        packages={
            "pip": ["fileformats-core", "fileformats-common"],
        },
        licenses={
            LICENSE_NAME: {
                "destination": license_path,
                "info_url": "http://license.test",
                "description": "This is a license to test the build structure",
            }
        },
        command={
            "task": "arcana.core.utils.testing.tasks:check_license",
            "row_frequency": "core:Samples[sample]",
            "inputs": [
                {
                    "name": LICENSE_INPUT_FIELD,
                    "datatype": "fileformats.common:Text",
                    "field": "expected_license_contents",
                    "help_string": "the path to the license",
                },
            ],
            "outputs": [
                {
                    "name": LICENSE_OUTPUT_FIELD,
                    "datatype": "fileformats.common:Text",
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


def make_dataset(simple_store, dataset_dir) -> Dataset:

    contents_dir = (
        dataset_dir / simple_store.LEAVES_DIR / "sample=sample1" / LICENSE_INPUT_PATH
    )
    contents_dir.mkdir(parents=True)

    with open(contents_dir / (LICENSE_INPUT_PATH + ".txt"), "w") as f:
        f.write(LICENSE_CONTENTS)

    dataset = simple_store.new_dataset(dataset_dir, space=Samples)
    dataset.save()
    return dataset


@pytest.fixture
def license_file(work_dir) -> Path:
    license_src = work_dir / "license_file.txt"

    with open(license_src, "w") as f:
        f.write(LICENSE_CONTENTS)

    return license_src


ORG = "arcana-tests"
REGISTRY = "a.docker.registry.io"
IMAGE_VERSION = "1.0"


LICENSE_CONTENTS = "license contents"

LICENSE_NAME = "testlicense"

LICENSE_INPUT_FIELD = "license_file"

LICENSE_OUTPUT_FIELD = "validated_license_file"

LICENSE_PATH_PARAM = "license_path"

LICENSE_INPUT_PATH = "contents-file"

LICENSE_OUTPUT_PATH = "validated-file"
