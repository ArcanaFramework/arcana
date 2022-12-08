from functools import reduce
from operator import mul
from arcana.cli.deploy import image_entrypoint
from arcana.core.testing.utils import show_cli_trace, make_dataset_id_str
from arcana.core.testing.data.types import EncodedText
from arcana.core.testing.data.sets import (
    make_dataset,
    TestDatasetBlueprint,
    TestDataSpace,
)
from arcana.data.types.common import Text
from arcana.deploy.common import PipelineImage


def write_spec(name, command, path):
    image_spec = PipelineImage(
        name=name,
        version=1.0,
        build_iteration="1",
        description="a test image",
        authors=[{"name": "Some One", "email": "some.one@an.email.org"}],
        info_url="http://concatenate.readthefakedocs.io",
        command=command,
    )
    image_spec.save(path)

    return path


def test_entrypoint_cli(concatenate_task, saved_dataset, cli_runner, work_dir):
    # Get CLI name for dataset (i.e. file system path prepended by 'file//')
    dataset_id_str = make_dataset_id_str(saved_dataset)
    bp = saved_dataset.__annotations__["blueprint"]
    duplicates = 1

    spec_path = write_spec(
        name="test_entrypoint_cli",
        command={
            "task": "arcana.core.testing.tasks:" + concatenate_task.__name__,
            "row_frequency": bp.space.default(),
            "inputs": [
                {
                    "name": "source1",
                    "datatype": "common:Text",
                    "field": "in_file1",
                    "help_string": "dummy",
                },
                {
                    "name": "source2",
                    "datatype": "common:Text",
                    "field": "in_file2",
                    "help_string": "dummy",
                },
            ],
            "outputs": [
                {
                    "name": "sink1",
                    "datatype": "common:Text",
                    "field": "out_file",
                    "help_string": "dummy",
                }
            ],
            "parameters": [
                {
                    "name": "duplicates",
                    "datatype": "int",
                    "default": 2,
                    "help_string": "dummy",
                }
            ],
        },
        path=work_dir / "spec.yaml",
    )
    # Start generating the arguments for the CLI
    # Add source to loaded dataset
    result = cli_runner(
        image_entrypoint,
        [
            dataset_id_str,
            "--input",
            "source1",
            "file1",
            "--input",
            "source2",
            "file2",
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
            "--spec-path",
            str(spec_path),
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


def test_entrypoint_cli_fail(concatenate_task, saved_dataset, cli_runner, work_dir):
    # Get CLI name for dataset (i.e. file system path prepended by 'file//')
    dataset_id_str = make_dataset_id_str(saved_dataset)
    bp = saved_dataset.__annotations__["blueprint"]
    duplicates = 1

    spec_path = write_spec(
        name="test_entrypoint_cli_fail",
        command={
            "task": "arcana.core.testing.tasks:" + concatenate_task.__name__,
            "row_frequency": bp.space.default(),
            "inputs": [
                {
                    "name": "source1",
                    "datatype": "common:Text",
                    "field": "in_file1",
                    "help_string": "dummy",
                },
                {
                    "name": "source2",
                    "datatype": "common:Directory",
                    "field": "in_file2",
                    "help_string": "dummy",
                },
            ],
            "outputs": [
                {
                    "name": "sink1",
                    "datatype": "common:Text",
                    "field": "out_file",
                    "help_string": "dummy",
                }
            ],
            "parameters": [
                {
                    "name": "duplicates",
                    "datatype": "int",
                    "default": 2,
                    "help_string": "dummy",
                }
            ],
        },
        path=work_dir / "spec.yaml",
    )

    # Start generating the arguments for the CLI
    # Add source to loaded dataset
    result = cli_runner(
        image_entrypoint,
        [
            dataset_id_str,
            "--input",
            "source1",
            "bad-file-path",
            "--input",
            "source2",
            "file2",
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
            "--spec-path",
            str(spec_path),
            "--dataset-hierarchy",
        ]
        + [str(ln) for ln in bp.hierarchy],
    )
    assert (
        result.exit_code == 1
    )  # fails due to missing path for source1 and incorrect datatype of source2
    # TODO: Should try to read logs to check for error message but can't work out how to capture them


def test_entrypoint_cli_on_row(cli_runner, work_dir):

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

    spec_path = write_spec(
        name="test_entrypoint_cli_on_row",
        command={
            "task": "arcana.core.testing.tasks:plus_10_to_filenumbers",
            "row_frequency": bp.space.default(),
            "inputs": [
                {
                    "name": "a_row",
                    "datatype": "arcana.core.data.row:DataRow",
                    "field": "filenumber_row",
                    "help_string": "dummy",
                },
            ],
        },
        path=work_dir / "spec.yaml",
    )

    # Start generating the arguments for the CLI
    # Add source to loaded dataset
    result = cli_runner(
        image_entrypoint,
        [
            dataset_id_str,
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
            "--spec-path",
            spec_path,
            "--dataset-hierarchy",
        ]
        + [str(ln) for ln in bp.hierarchy],
    )
    assert result.exit_code == 0, show_cli_trace(result)

    assert get_dataset_filenumbers() == [i + 10 for i in filenumbers]


def test_entrypoint_cli_with_converter_args(saved_dataset, cli_runner, work_dir):
    """Test passing arguments to file format converter tasks via input/output
    "qualifiers", e.g. 'converter.shift=3' using the arcana-run-pipeline CLI
    tool (as used in the XNAT CS commands)
    """
    # Get CLI name for dataset (i.e. file system path prepended by 'file//')
    dataset_id_str = make_dataset_id_str(saved_dataset)
    bp = saved_dataset.__annotations__["blueprint"]
    # Start generating the arguments for the CLI
    # Add source to loaded dataset
    spec_path = write_spec(
        name="test_entrypoint_cli_fail",
        command={
            "task": "arcana.core.testing.tasks:identity_file",
            "row_frequency": bp.space.default(),
            "inputs": [
                {
                    "name": "source",
                    "datatype": "arcana.core.testing.data.types:EncodedText",
                    "default_column": {"datatype": "common:Text"},
                    "field": "in_file",
                    "help_string": "dummy",
                },
            ],
            "outputs": [
                {
                    "name": "sink1",
                    "datatype": "arcana.core.testing.data.types:EncodedText",
                    "field": "out",
                    "help_string": "dummy",
                },
                {
                    "name": "sink2",
                    "datatype": "arcana.core.testing.data.types:EncodedText",
                    "default_column": {
                        "datatype": "arcana.core.testing.data.types:DecodedText"
                    },
                    "field": "out",
                    "help_string": "dummy",
                },
            ],
        },
        path=work_dir / "spec.yaml",
    )

    result = cli_runner(
        image_entrypoint,
        [
            dataset_id_str,
            "--input",
            "source",
            "file1 converter.shift=3",
            "--output",
            "sink1",
            "encoded",
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
            "--spec-path",
            spec_path,
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
