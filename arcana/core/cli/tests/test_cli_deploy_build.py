import shutil
import re
from typing import Union, Dict, Tuple
import yaml
import tempfile
from pathlib import Path
import pytest
import docker
from arcana.core.cli.deploy import (
    build,
    build_docs,
)
from arcana.core.utils.testing.fixtures.docs import all_docs_fixtures, DocsFixture
from arcana.core.utils.testing import show_cli_trace
from arcana.core.exceptions import ArcanaBuildError


def test_deploy_build_cli(command_spec, cli_runner, work_dir):

    DOCKER_ORG = "testorg"
    DOCKER_REGISTRY = "test.registry.org"
    IMAGE_GROUP_NAME = "testpkg"

    concatenate_spec = {
        "command": command_spec,
        "version": "1.0",
        "build_iteration": "1",
        "packages": {
            "system": ["vim"],  # just to test it out
            "pip": {"click": None},  # just to test it out
        },
        "authors": [{"name": "Some One", "email": "some.one@an.email.org"}],
        "info_url": "http://concatenate.readthefakedocs.io",
        "description": "a test image spec",
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
            "xnat:XnatCSImage",
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
                "xnat:XnatCSImage",
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
        "build_iteration": "1",
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
        concatenate_spec["packages"] = {"system": ["vim"]}

        with pytest.raises(ArcanaBuildError) as excinfo:
            build_spec(concatenate_spec, catch_exceptions=False)

        assert "doesn't match the one that was used to build the image" in str(
            excinfo.value
        )

        # Increment the version number to avoid the clash
        concatenate_spec["build_iteration"] = "2"

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
