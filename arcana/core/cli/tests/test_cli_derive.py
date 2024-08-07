from functools import reduce
from operator import mul
from arcana.core.cli.apply import apply_pipeline
from arcana.core.cli.derive import derive_column
from arcana.core.utils.misc import show_cli_trace
from fileformats.text import TextFile


def test_derive_cli(saved_dataset, concatenate_task, cli_runner):
    # Get CLI name for dataset (i.e. file system path prepended by 'file//')
    bp = saved_dataset.__annotations__["blueprint"]
    duplicates = 3
    # Start generating the arguments for the CLI
    # Add source to loaded dataset
    result = cli_runner(
        apply_pipeline,
        [
            saved_dataset.locator,
            "a_pipeline",
            "arcana.testing.tasks:" + concatenate_task.__name__,
            "--source",
            "file1",
            "in_file1",
            "text/text-file",
            "--source",
            "file2",
            "in_file2",
            "text/text-file",
            "--sink",
            "concatenated",
            "out_file",
            "text/text-file",
            "--parameter",
            "duplicates",
            str(duplicates),
        ],
    )
    assert result.exit_code == 0, show_cli_trace(result)
    # Add source column to saved dataset
    result = cli_runner(
        derive_column, [saved_dataset.locator, "concatenated", "--plugin", "serial"]
    )
    assert result.exit_code == 0, show_cli_trace(result)
    sink = saved_dataset.add_sink("concatenated", TextFile)
    assert len(sink) == reduce(mul, bp.dim_lengths)
    fnames = ["file1.txt", "file2.txt"]
    if concatenate_task.__name__.endswith("reverse"):
        fnames = [f[::-1] for f in fnames]
    expected_contents = "\n".join(fnames * duplicates)
    for item in sink:
        with open(item) as f:
            contents = f.read()
        assert contents == expected_contents
