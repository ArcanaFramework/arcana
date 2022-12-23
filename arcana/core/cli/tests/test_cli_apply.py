from arcana.core.data.set import Dataset
from fileformats.common import Text
from arcana.core.cli.apply import apply_pipeline
from arcana.core.utils.testing import show_cli_trace


def test_apply_pipeline_cli(saved_dataset: Dataset, concatenate_task, cli_runner):
    # Get CLI name for dataset (i.e. file system path prepended by 'file//')
    # Start generating the arguments for the CLI
    # Add source to loaded dataset
    duplicates = 5
    saved_dataset.add_source("file1", Text)
    saved_dataset.add_source("file2", Text)
    saved_dataset.add_sink("concatenated", Text)
    saved_dataset.apply_pipeline(
        name="a_pipeline",
        workflow=concatenate_task(name="workflow", duplicates=duplicates),
        inputs=[("file1", "in_file1"), ("file2", "in_file2")],
        outputs=[("concatenated", "out_file")],
    )
    # Add source column to saved dataset
    result = cli_runner(
        apply_pipeline,
        [
            saved_dataset.locator,
            "a_pipeline",
            "arcana.core.utils.testing.tasks:" + concatenate_task.__name__,
            "--source",
            "file1",
            "in_file1",
            "fileformats.common:Text",
            "--source",
            "file2",
            "in_file2",
            "fileformats.common:Text",
            "--sink",
            "concatenated",
            "out_file",
            "fileformats.common:Text",
            "--parameter",
            "duplicates",
            str(duplicates),
        ],
    )
    assert result.exit_code == 0, show_cli_trace(result)
    loaded_dataset = Dataset.load(saved_dataset.locator)
    assert saved_dataset.pipelines == loaded_dataset.pipelines
