import pytest
from pathlib import Path
import docker
import docker.errors
from arcana.core.utils.misc import show_cli_trace
from arcana.core.cli.deploy import make_app, install_license
from arcana.testing.deploy.licenses import (
    get_pipeline_image,
    make_dataset,
    ORG,
    REGISTRY,
    IMAGE_VERSION,
    LICENSE_CONTENTS,
    LICENSE_NAME,
    LICENSE_INPUT_FIELD,
    LICENSE_OUTPUT_FIELD,
    LICENSE_PATH_PARAM,
    LICENSE_INPUT_PATH,
    LICENSE_OUTPUT_PATH,
)


def test_buildtime_license(license_file, run_prefix: str, work_dir: Path, cli_runner):

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
    make_dataset(dataset_dir)

    result = cli_runner(
        make_app,
        args=[
            str(root_dir),
            "stdlib:App",
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
        "dirtree///dataset "
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


def test_site_runtime_license(license_file, work_dir, arcana_home, cli_runner):

    # build_dir = work_dir / "build"
    dataset_dir = work_dir / "dataset"

    LICENSE_PATH = work_dir / "license_location"

    pipeline_image = get_pipeline_image(LICENSE_PATH)

    # Save it into the new home directory
    dataset = make_dataset(dataset_dir)

    result = cli_runner(install_license, args=[LICENSE_NAME, str(license_file)])
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


def test_dataset_runtime_license(license_file, run_prefix, work_dir, cli_runner):

    # build_dir = work_dir / "build"
    dataset_dir = work_dir / "dataset"

    dataset = make_dataset(dataset_dir)

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


@pytest.fixture
def license_file(work_dir) -> Path:
    license_src = work_dir / "license_file.txt"

    with open(license_src, "w") as f:
        f.write(LICENSE_CONTENTS)

    return license_src
