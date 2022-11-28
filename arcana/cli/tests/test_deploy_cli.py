import sys
import os
import shutil
from copy import copy
import re
from typing import Union, Dict, Tuple
import json
import yaml
from functools import reduce
from operator import mul
import tempfile
from pathlib import Path
from unittest.mock import patch
import pytest
import docker
import xnat
from arcana.cli.deploy import (
    build,
    build_docs,
    run_in_image,
    pull_xnat_images,
    xnat_auth_refresh,
    PULL_IMAGES_XNAT_HOST_KEY,
    PULL_IMAGES_XNAT_USER_KEY,
    PULL_IMAGES_XNAT_PASS_KEY,
)
from arcana.core.utils import class_location
from arcana.core.testing.fixtures.docs import all_docs_fixtures, DocsFixture
from arcana.core.testing.utils import show_cli_trace, make_dataset_id_str
from arcana.core.testing.formats import EncodedText
from arcana.core.testing.datasets import (
    make_dataset,
    TestDatasetBlueprint,
    TestDataSpace,
)
from arcana.data.formats.common import Text
from arcana.core.exceptions import ArcanaBuildError


def test_deploy_build_cli(command_spec, cli_runner, work_dir):

    DOCKER_ORG = "pulltestorg"
    DOCKER_REGISTRY = "test.registry.org"
    IMAGE_GROUP_NAME = "testpkg"

    concatenate_spec = {
        "command": command_spec,
        "version": "1.0",
        "spec_version": "1",
        "system_packages": [{"name": "vim"}],  # just to test it out
        "python_packages": [{"name": "pytest"}],  # just to test it out
        "authors": [{"name": "Some One", "email": "some.one@an.email.org"}],
        "info_url": "http://concatenate.readthefakedocs.io",
        "description": "a test image spec",
        "name": "test_deploy_build_cli",
    }

    build_dir = work_dir / "build"
    build_dir.mkdir()
    spec_path = work_dir / DOCKER_ORG
    sub_dir = spec_path / IMAGE_GROUP_NAME
    sub_dir.mkdir(parents=True)
    with open(sub_dir / "concatenate.yml", "w") as f:
        yaml.dump(concatenate_spec, f)

    result = cli_runner(
        build,
        [
            str(spec_path),
            "--build-dir",
            str(build_dir),
            "--registry",
            DOCKER_REGISTRY,
            "--loglevel",
            "warning",
            "--use-local-packages",
            "--install-extras",
            "test",
            "--raise-errors",
            "--use-test-config",
            "--dont-check-registry",
        ],
    )
    assert result.exit_code == 0, show_cli_trace(result)
    tag = result.output.strip().splitlines()[-1]
    assert tag == f"{DOCKER_REGISTRY}/{DOCKER_ORG}/{IMAGE_GROUP_NAME}.concatenate:1.0-1"

    # Clean up the built image
    dc = docker.from_env()
    dc.images.remove(tag)


def test_deploy_rebuild_cli(command_spec, docker_registry, cli_runner, run_prefix):
    """Tests the check to see whether"""

    IMAGE_GROUP_NAME = "testpkg-rebuild" + run_prefix

    def build_spec(spec, **kwargs):
        work_dir = Path(tempfile.mkdtemp())
        build_dir = work_dir / "build"
        build_dir.mkdir()
        spec_path = work_dir / "testorg"
        sub_dir = spec_path / IMAGE_GROUP_NAME
        sub_dir.mkdir(parents=True)
        with open(sub_dir / "concatenate.yml", "w") as f:
            yaml.dump(spec, f)

        result = cli_runner(
            build,
            [
                str(spec_path),
                "--build-dir",
                str(build_dir),
                "--registry",
                docker_registry,
                "--loglevel",
                "warning",
                "--use-local-packages",
                "--install-extras",
                "test",
                "--raise-errors",
                "--check-registry",
                "--use-test-config",
            ],
            **kwargs,
        )
        return result

    concatenate_spec = {
        "command": command_spec,
        "version": "1.0",
        "spec_version": "1",
        "system_packages": [],
        "python_packages": [],
        "description": "a test image",
        "name": "test_deploy_rebuild_cli",
        "authors": [{"name": "Some One", "email": "some.one@an.email.org"}],
        "info_url": "http://concatenate.readthefakedocs.io",
    }

    # Build a basic image
    result = build_spec(concatenate_spec)
    assert result.exit_code == 0, show_cli_trace(result)
    assert result.output
    tag = result.output.strip().splitlines()[-1]
    try:
        dc = docker.from_env()
        dc.api.push(tag)

        # FIXME: Need to ensure that logs are captured properly then we can test this
        # result = build_spec(concatenate_spec)
        # assert "Skipping" in result.output

        # Modify the spec so it doesn't match the original that has just been
        # built (but don't increment the version number -> image tag so there
        # is a clash)
        concatenate_spec["system_packages"].append({"name": "vim"})

        with pytest.raises(ArcanaBuildError) as excinfo:
            build_spec(concatenate_spec, catch_exceptions=False)

        assert "doesn't match the one that was used to build the image" in str(
            excinfo.value
        )

        # Increment the version number to avoid the clash
        concatenate_spec["spec_version"] = "2"

        result = build_spec(concatenate_spec)
        assert result.exit_code == 0, show_cli_trace(result)
        rebuilt_tag = result.output.strip().splitlines()[-1]
        dc.images.remove(rebuilt_tag)
    finally:
        # Clean up the built images
        dc.images.remove(tag)


def _build_docs(
    cli_runner,
    work_dir: Path,
    docs: Union[str, Dict[str, str]],
    *args,
    flatten: bool = None,
) -> Union[str, Dict[str, str]]:
    out_dir = work_dir / "out"
    specs_dir = work_dir / "specs"
    if specs_dir.exists():
        shutil.rmtree(specs_dir)
    specs_dir.mkdir()

    if type(docs) is str:
        (specs_dir / "spec.yaml").write_text(docs)
    else:
        for name, content in docs.items():
            path = specs_dir / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content)

    result = cli_runner(
        build_docs,
        [
            specs_dir.as_posix(),
            out_dir.as_posix(),
        ]
        + (["--flatten" if flatten else "--no-flatten"] if flatten is not None else [])
        + list(args),
    )

    assert result.exit_code == 0, show_cli_trace(result)

    if type(docs) is str:
        return (out_dir / "spec.md").read_text().strip()
    else:
        return {
            file.relative_to(out_dir).as_posix(): file.read_text().strip()
            for file in out_dir.glob("*.md")
        }


@pytest.mark.parametrize("fixture", all_docs_fixtures(), ids=lambda x: x[0])
def test_build_docs_cli(
    cli_runner, run_prefix, work_dir: Path, fixture: Tuple[str, DocsFixture]
):
    fixture_name, fixture_content = fixture

    # TODO handle multiple 'files' in a fixture
    print(f"Processing fixture: {fixture_name!r}")
    output = _build_docs(cli_runner, work_dir, fixture_content.yaml_src)

    strip_source_file_re = re.compile(r"source_file:.*")

    stripped_output = strip_source_file_re.sub("", output)
    stripped_reference = strip_source_file_re.sub("", fixture_content.markdown)

    assert (
        stripped_output == stripped_reference
    ), f"Fixture {fixture_name!r} didn't match output"


def test_run_pipeline_cli(concatenate_task, saved_dataset, cli_runner, work_dir):
    # Get CLI name for dataset (i.e. file system path prepended by 'file//')
    dataset_id_str = make_dataset_id_str(saved_dataset)
    bp = saved_dataset.__annotations__["blueprint"]
    duplicates = 1
    # Start generating the arguments for the CLI
    # Add source to loaded dataset
    result = cli_runner(
        run_in_image,
        [
            "arcana.core.testing.tasks:" + concatenate_task.__name__,
            "a_pipeline",
            dataset_id_str,
            "--input-config",
            "source1",
            "common:Text",
            "in_file1",
            "common:Text",
            "--input",
            "source1",
            "file1",
            "--input-config",
            "source2",
            "common:Text",
            "in_file2",
            "common:Text",
            "--input",
            "source2",
            "file2",
            "--output-config",
            "sink1",
            "common:Text",
            "out_file",
            "common:Text",
            "--parameter-config",
            "duplicates",
            "--output",
            "sink1",
            "concatenated",
            "--parameter",
            "duplicates",
            str(duplicates),
            "--raise-errors",
            "--plugin",
            "serial",
            "--work",
            str(work_dir),
            "--loglevel",
            "debug",
            "--dataset-space",
            class_location(bp.space),
            "--dataset-hierarchy",
        ]
        + [str(ln) for ln in bp.hierarchy],
    )
    assert result.exit_code == 0, show_cli_trace(result)
    # Add source column to saved dataset
    sink = saved_dataset.add_sink("concatenated", Text)
    assert len(sink) == reduce(mul, bp.dim_lengths)
    fnames = ["file1.txt", "file2.txt"]
    if concatenate_task.__name__.endswith("reverse"):
        fnames = [f[::-1] for f in fnames]
    expected_contents = "\n".join(fnames * duplicates)
    for item in sink:
        item.get(assume_exists=True)
        with open(item.fs_path) as f:
            contents = f.read()
        assert contents == expected_contents


def test_run_pipeline_cli_fail(concatenate_task, saved_dataset, cli_runner, work_dir):
    # Get CLI name for dataset (i.e. file system path prepended by 'file//')
    dataset_id_str = make_dataset_id_str(saved_dataset)
    bp = saved_dataset.__annotations__["blueprint"]
    duplicates = 1
    # Start generating the arguments for the CLI
    # Add source to loaded dataset
    result = cli_runner(
        run_in_image,
        [
            "arcana.core.testing.tasks:" + concatenate_task.__name__,
            "a_pipeline",
            dataset_id_str,
            "--input-config",
            "source1",
            "common:Text",
            "in_file1",
            "common:Text",
            "--input",
            "source1",
            "bad-file-path",
            "--input-config",
            "source2",
            "common:Directory",
            "in_file2",
            "common:Directory",
            "--input",
            "source2",
            "file2",
            "--output-config",
            "sink1",
            "common:Text",
            "out_file",
            "common:Text",
            "--output",
            "sink1",
            "concatenated",
            "--parameter",
            "duplicates",
            str(duplicates),
            "--plugin",
            "serial",
            "--loglevel",
            "error",
            "--work",
            str(work_dir),
            "--dataset-space",
            class_location(bp.space),
            "--dataset-hierarchy",
        ]
        + [str(ln) for ln in bp.hierarchy],
    )
    assert (
        result.exit_code == 1
    )  # fails due to missing path for source1 and incorrect format of source2
    # TODO: Should try to read logs to check for error message but can't work out how to capture them


def test_run_pipeline_on_row_cli(cli_runner, work_dir):

    # Create test dataset consisting of a single row with a range of filenames
    # from 0 to 4
    filenumbers = list(range(5))
    bp = TestDatasetBlueprint(
        [
            TestDataSpace.abcd
        ],  # e.g. XNAT where session ID is unique in project but final layer is organised by timepoint
        [1, 1, 1, 1],
        [f"{i}.txt" for i in filenumbers],
        {},
        {},
        [],
    )
    dataset_path = work_dir / "numbered_dataset"
    dataset = make_dataset(bp, dataset_path)
    dataset.save()

    # Get CLI name for dataset (i.e. file system path prepended by 'file//')
    dataset_id_str = make_dataset_id_str(dataset)

    def get_dataset_filenumbers():
        dataset.refresh()
        row = next(dataset.rows())
        return sorted(int(i.path) for i in row.unresolved)

    assert get_dataset_filenumbers() == filenumbers

    # Start generating the arguments for the CLI
    # Add source to loaded dataset
    result = cli_runner(
        run_in_image,
        [
            "arcana.core.testing.tasks:plus_10_to_filenumbers",
            "a_pipeline",
            dataset_id_str,
            "--input-config",
            "a_row",
            "arcana.core.data.row:DataRow",
            "filenumber_row",
            "arcana.core.data.row:DataRow",
            "--input",
            "a_row",
            "",
            "--plugin",
            "serial",
            "--work",
            str(work_dir),
            "--loglevel",
            "debug",
            "--raise-errors",
            "--dataset-space",
            class_location(bp.space),
            "--dataset-hierarchy",
        ]
        + [str(ln) for ln in bp.hierarchy],
    )
    assert result.exit_code == 0, show_cli_trace(result)

    assert get_dataset_filenumbers() == [i + 10 for i in filenumbers]


def test_run_pipeline_cli_converter_args(saved_dataset, cli_runner, work_dir):
    """Test passing arguments to file format converter tasks via input/output
    "qualifiers", e.g. 'converter.shift=3' using the arcana-run-pipeline CLI
    tool (as used in the XNAT CS commands)
    """
    # Get CLI name for dataset (i.e. file system path prepended by 'file//')
    dataset_id_str = make_dataset_id_str(saved_dataset)
    bp = saved_dataset.__annotations__["blueprint"]
    # Start generating the arguments for the CLI
    # Add source to loaded dataset
    result = cli_runner(
        run_in_image,
        [
            "arcana.core.testing.tasks:identity_file",
            "a_pipeline",
            dataset_id_str,
            "--input-config",
            "source",
            "common:Text",
            "in_file",
            "arcana.core.testing.formats:EncodedText",
            "--input",
            "source",
            "file1 converter.shift=3",
            "--output-config",
            "sink1",
            "arcana.core.testing.formats:EncodedText",
            "out",
            "arcana.core.testing.formats:EncodedText",
            "--output",
            "sink1",
            "encoded",
            "--output-config",
            "sink2",
            "arcana.core.testing.formats:DecodedText",
            "out",
            "arcana.core.testing.formats:EncodedText",
            "--output",
            "sink2",
            "decoded converter.shift=3",
            "--raise-errors",
            "--plugin",
            "serial",
            "--work",
            str(work_dir),
            "--loglevel",
            "debug",
            "--dataset-space",
            class_location(bp.space),
            "--dataset-hierarchy",
        ]
        + [str(ln) for ln in bp.hierarchy],
    )
    assert result.exit_code == 0, show_cli_trace(result)
    # Add source column to saved dataset
    saved_dataset.add_sink("sink1", EncodedText, path="encoded")
    saved_dataset.add_sink("sink2", Text, path="decoded")
    unencoded_contents = "file1.txt"
    encoded_contents = (
        "iloh41w{w"  # 'file1.txt' characters shifted up by 3 in ASCII code
    )
    for row in saved_dataset.rows(frequency="abcd"):
        enc_item = row["sink1"]
        dec_item = row["sink2"]
        enc_item.get(assume_exists=True)
        dec_item.get(assume_exists=True)
        with open(enc_item.fs_path) as f:
            enc_contents = f.read()
        with open(dec_item.fs_path) as f:
            dec_contents = f.read()
        assert enc_contents == encoded_contents
        assert dec_contents == unencoded_contents


@pytest.mark.xfail(
    sys.platform == "linux",
    reason=(
        "Haven't been able to setup either SSL for the Xnat4Tests test docker "
        "registry, or the internal host on GH Actions as an insecure "
        "registries"
    ),
)
def test_pull_xnat_images(
    xnat_repository,
    command_spec,
    work_dir,
    docker_registry_for_xnat_uri,
    cli_runner,
    run_prefix,
    xnat4tests_config,
):

    DOCKER_ORG = "pulltestorg"
    IMAGE_GROUP_NAME = "apackage"
    PKG_VERSION = "1.0"
    WRAPPER_VERSION = "1-pullimages"

    reverse_command_spec = copy(command_spec)
    reverse_command_spec["task"] = "arcana.core.testing.tasks:concatenate_reverse"

    spec_dir = work_dir / DOCKER_ORG
    pkg_path = spec_dir / IMAGE_GROUP_NAME
    pkg_path.mkdir(parents=True)

    expected_images = []
    expected_commands = []

    for name, cmd_spec in (
        ("forward_concat-pullimages", command_spec),
        ("reverse_concat-pullimages", reverse_command_spec),
    ):

        image_spec = {
            "name": name,
            "command": cmd_spec,
            "version": PKG_VERSION,
            "spec_version": WRAPPER_VERSION,
            "system_packages": [],
            "python_packages": [],
            "authors": [{"name": "Some One", "email": "some.one@an.email.org"}],
            "info_url": "http://concatenate.readthefakedocs.io",
            "description": "a command to test build process",
        }

        with open((pkg_path / name).with_suffix(".yaml"), "w") as f:
            yaml.dump(image_spec, f)

        expected_images.append(
            f"{docker_registry_for_xnat_uri}/{DOCKER_ORG}/{IMAGE_GROUP_NAME}"
            f".{name}:{PKG_VERSION}-{WRAPPER_VERSION}"
        )
        expected_commands.append(f"{IMAGE_GROUP_NAME}.{name}")

    expected_images.sort()
    expected_commands.sort()

    build_dir = work_dir / "build"
    build_dir.mkdir()
    manifest_path = work_dir / "manifest.json"

    result = cli_runner(
        build,
        [
            "xnat:XnatCSImage",
            str(spec_dir),
            "--build-dir",
            str(build_dir),
            "--registry",
            docker_registry_for_xnat_uri,
            "--loglevel",
            "warning",
            "--use-local-packages",
            "--install-extras",
            "test",
            "--raise-errors",
            "--release",
            "test-pipelines-metapackage",
            run_prefix,
            "--save-manifest",
            str(manifest_path),
            "--use-test-config",
            "--push",
        ],
    )

    assert result.exit_code == 0, show_cli_trace(result)

    with open(manifest_path) as f:
        manifest = json.load(f)

    assert (
        sorted(f"{i['name']}:{i['version']}" for i in manifest["images"])
        == expected_images
    )

    # Delete images from local Docker instance (which the test XNAT uses)
    dc = docker.from_env()
    for img in expected_images:
        dc.images.remove(img)

    filters_file = work_dir / "filters.yaml"
    with open(filters_file, "w") as f:
        yaml.dump(
            {
                "include": [
                    {
                        "name": (
                            f"{docker_registry_for_xnat_uri}/"
                            f"{DOCKER_ORG}/{IMAGE_GROUP_NAME}.*"
                        )
                    }
                ],
            },
            f,
        )

    with patch.dict(
        os.environ,
        {
            PULL_IMAGES_XNAT_HOST_KEY: xnat4tests_config.xnat_uri,
            PULL_IMAGES_XNAT_USER_KEY: xnat4tests_config.xnat_user,
            PULL_IMAGES_XNAT_PASS_KEY: xnat4tests_config.xnat_password,
        },
    ):
        result = cli_runner(
            pull_xnat_images,
            [str(manifest_path), "--filters", str(filters_file)],
        )

    assert result.exit_code == 0, show_cli_trace(result)

    with xnat_repository:

        xlogin = xnat_repository.login

        result = xlogin.get("/xapi/commands/")

    # Check commands have been installed
    available_cmds = [e["name"] for e in result.json()]
    assert all(cmd in available_cmds for cmd in expected_commands)


def test_xnat_auth_refresh(xnat_repository, work_dir, cli_runner):

    config_path = work_dir / "config.yaml"
    with open(config_path, "w") as f:
        yaml.dump(
            {
                "server": xnat_repository.server,
            },
            f,
        )

    auth_path = work_dir / "auth.json"
    with open(auth_path, "w") as f:
        json.dump(
            {"alias": "admin", "secret": "admin"},
            f,
        )

    result = cli_runner(
        xnat_auth_refresh,
        [str(config_path), str(auth_path)],
    )

    assert result.exit_code == 0, show_cli_trace(result)

    with open(auth_path) as f:
        auth = yaml.load(f, Loader=yaml.Loader)

    assert len(auth["alias"]) > 20
    assert len(auth["secret"]) > 20

    assert xnat.connect(
        xnat_repository.server, user=auth["alias"], password=auth["secret"]
    )
