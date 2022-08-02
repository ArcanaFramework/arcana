import shutil
import traceback
from typing import Union, Dict, Tuple

import yaml
from functools import reduce
from operator import mul
import tempfile
from pathlib import Path
import pytest
import docker
from arcana.cli.deploy import build, build_docs, run_pipeline
from arcana.core.utils import class_location
from arcana.test.fixtures.docs import all_docs_fixtures, DocsFixture
from arcana.test.utils import show_cli_trace, make_dataset_id_str
from arcana.test.formats import EncodedText
from arcana.test.datasets import make_dataset, TestDatasetBlueprint, TestDataSpace
from arcana.data.formats.common import Text
from arcana.exceptions import ArcanaBuildError


def test_deploy_build_cli(command_spec, cli_runner, work_dir):

    DOCKER_ORG = "testorg"
    DOCKER_REGISTRY = "test.registry.org"
    PKG_NAME = "testpkg"

    concatenate_spec = {
        "commands": [command_spec],
        "pkg_version": "1.0",
        "wrapper_version": "1",
        "system_packages": ["vim"],  # just to test it out
        "python_packages": ["pytest"],  # just to test it out
        "authors": ["some.one@an.email.org"],
        "info_url": "http://concatenate.readthefakedocs.io",
    }

    build_dir = work_dir / "build"
    build_dir.mkdir()
    spec_path = work_dir / "test-specs"
    sub_dir = spec_path / PKG_NAME
    sub_dir.mkdir(parents=True)
    with open(sub_dir / "concatenate.yml", "w") as f:
        yaml.dump(concatenate_spec, f)

    result = cli_runner(
        build,
        [
            str(spec_path),
            DOCKER_ORG,
            "--build_dir",
            str(build_dir),
            "--registry",
            DOCKER_REGISTRY,
            "--loglevel",
            "warning",
            "--use-local-packages",
            "--install_extras",
            "test",
            "--raise-errors",
            "--use-test-config",
            "--dont-check-registry",
        ],
    )
    assert result.exit_code == 0, show_cli_trace(result)
    tag = result.output.strip()
    assert tag == f"{DOCKER_REGISTRY}/{DOCKER_ORG}/{PKG_NAME}.concatenate:1.0-1"

    # Clean up the built image
    dc = docker.from_env()
    dc.images.remove(tag)


def test_deploy_rebuild_cli(command_spec, docker_registry, cli_runner, run_prefix):
    """Tests the check to see whether"""

    DOCKER_ORG = "testorg"
    PKG_NAME = "testpkg-rebuild" + run_prefix

    def build_spec(spec, **kwargs):
        work_dir = Path(tempfile.mkdtemp())
        build_dir = work_dir / "build"
        build_dir.mkdir()
        spec_path = work_dir / "test-specs"
        sub_dir = spec_path / PKG_NAME
        sub_dir.mkdir(parents=True)
        with open(sub_dir / "concatenate.yml", "w") as f:
            yaml.dump(spec, f)

        result = cli_runner(
            build,
            [
                str(spec_path),
                DOCKER_ORG,
                "--build_dir",
                str(build_dir),
                "--registry",
                docker_registry,
                "--loglevel",
                "warning",
                "--use-local-packages",
                "--install_extras",
                "test",
                "--raise-errors",
                "--check-registry",
                "--use-test-config",
            ],
            **kwargs,
        )
        return result

    concatenate_spec = {
        "commands": [command_spec],
        "pkg_version": "1.0",
        "wrapper_version": "1",
        "system_packages": [],
        "python_packages": [],
        "authors": ["some.one@an.email.org"],
        "info_url": "http://concatenate.readthefakedocs.io",
    }

    # Build a basic image
    result = build_spec(concatenate_spec)
    assert result.exit_code == 0, show_cli_trace(result)
    tag = result.output.strip()
    try:
        dc = docker.from_env()
        dc.api.push(tag)

        # FIXME: Need to ensure that logs are captured properly then we can test this
        # result = build_spec(concatenate_spec)
        # assert "Skipping" in result.output

        # Modify the spec so it doesn't match the original that has just been
        # built (but don't increment the version number -> image tag so there
        # is a clash)
        concatenate_spec["system_packages"].append("vim")

        with pytest.raises(ArcanaBuildError) as excinfo:
            build_spec(concatenate_spec, catch_exceptions=False)

        assert "doesn't match the one that was used to build the image" in str(
            excinfo.value
        )

        # Increment the version number to avoid the clash
        concatenate_spec["wrapper_version"] = "2"

        result = build_spec(concatenate_spec)
        assert result.exit_code == 0, show_cli_trace(result)
        rebuilt_tag = result.output.strip()
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
            "--root",
            specs_dir,
        ]
        + (["--flatten" if flatten else "--no-flatten"] if flatten is not None else [])
        + list(args),
    )

    if result.exit_code != 0:
        print(result.output)
        if result.exception:
            traceback.print_exception(
                type(result.exception), result.exception, result.exception.__traceback__
            )
    assert result.exit_code == 0

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

    assert (
        output == fixture_content.markdown
    ), f"Fixture {fixture_name!r} didn't match output"


def test_run_pipeline_cli(concatenate_task, saved_dataset, cli_runner, work_dir):
    # Get CLI name for dataset (i.e. file system path prepended by 'file//')
    dataset_id_str = make_dataset_id_str(saved_dataset)
    bp = saved_dataset.blueprint
    duplicates = 1
    # Start generating the arguments for the CLI
    # Add source to loaded dataset
    result = cli_runner(
        run_pipeline,
        [
            dataset_id_str,
            "a_pipeline",
            "arcana.test.tasks:" + concatenate_task.__name__,
            "--input",
            "source1",
            "common:Text",
            "file1",
            "in_file1",
            "common:Text",
            "--input",
            "source2",
            "common:Text",
            "file2",
            "in_file2",
            "common:Text",
            "--output",
            "sink1",
            "common:Text",
            "concatenated",
            "out_file",
            "common:Text",
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
            "--dataset_space",
            class_location(bp.space),
            "--dataset_hierarchy",
        ]
        + [str(l) for l in bp.hierarchy],
    )
    assert result.exit_code == 0, show_cli_trace(result)
    # Add source column to saved dataset
    sink = saved_dataset.add_sink("concatenated", Text)
    assert len(sink) == reduce(mul, saved_dataset.blueprint.dim_lengths)
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
    bp = saved_dataset.blueprint
    duplicates = 1
    # Start generating the arguments for the CLI
    # Add source to loaded dataset
    result = cli_runner(
        run_pipeline,
        [
            dataset_id_str,
            "a_pipeline",
            "arcana.test.tasks:" + concatenate_task.__name__,
            "--input",
            "source1",
            "common:Text",
            "bad-file-path",
            "in_file1",
            "common:Text",
            "--input",
            "source2",
            "common:Directory",
            "file2",
            "in_file2",
            "common:Directory",
            "--output",
            "sink1",
            "common:Text",
            "concatenated",
            "out_file",
            "common:Text",
            "--parameter",
            "duplicates",
            str(duplicates),
            "--plugin",
            "serial",
            "--loglevel",
            "error",
            "--work",
            str(work_dir),
            "--dataset_space",
            class_location(bp.space),
            "--dataset_hierarchy",
        ]
        + [str(l) for l in bp.hierarchy],
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
        run_pipeline,
        [
            dataset_id_str,
            "a_pipeline",
            "arcana.test.tasks:plus_10_to_filenumbers",
            "--input",
            "a_row",
            "arcana.core.data.row:DataRow",
            "",
            "filenumber_row",
            "arcana.core.data.row:DataRow",
            "--plugin",
            "serial",
            "--work",
            str(work_dir),
            "--loglevel",
            "debug",
            "--raise-errors",
            "--dataset_space",
            class_location(bp.space),
            "--dataset_hierarchy",
        ]
        + [str(l) for l in bp.hierarchy],
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
    bp = saved_dataset.blueprint
    duplicates = 1
    # Start generating the arguments for the CLI
    # Add source to loaded dataset
    result = cli_runner(
        run_pipeline,
        [
            dataset_id_str,
            "a_pipeline",
            "arcana.test.tasks:identity_file",
            "--input",
            "source",
            "common:Text",
            "file1 converter.shift=3",
            "in_file",
            "arcana.test.formats:EncodedText",
            "--output",
            "sink1",
            "arcana.test.formats:EncodedText",
            "encoded",
            "out",
            "arcana.test.formats:EncodedText",
            "--output",
            "sink2",
            "arcana.test.formats:DecodedText",
            "decoded converter.shift=3",
            "out",
            "arcana.test.formats:EncodedText",
            "--raise-errors",
            "--plugin",
            "serial",
            "--work",
            str(work_dir),
            "--loglevel",
            "debug",
            "--dataset_space",
            class_location(bp.space),
            "--dataset_hierarchy",
        ]
        + [str(l) for l in bp.hierarchy],
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
