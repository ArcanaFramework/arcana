from arcana.core.data.set import Dataset
from arcana.data.formats.common import Text
from arcana.cli.apply import apply_workflow
from arcana.test.utils import show_cli_trace, make_dataset_id_str
from arcana.test.tasks import concatenate


def test_apply_workflow_cli(saved_dataset, cli_runner):
    # Get CLI name for dataset (i.e. file system path prepended by 'file//')
    dataset_id_str = make_dataset_id_str(saved_dataset)
    # Start generating the arguments for the CLI
    # Add source to loaded dataset
    duplicates = 5
    saved_dataset.add_source('file1', Text)
    saved_dataset.add_source('file2', Text)
    saved_dataset.add_sink('concatenated', Text)
    saved_dataset.apply_workflow(
        name='a_pipeline',
        workflow=concatenate(
            duplicates=duplicates),
        inputs=[('file1', 'in_file1'),
                ('file2', 'in_file2')],
        outputs=[('concatenated', 'out_file')])
    # Add source column to saved dataset
    result = cli_runner(
        apply_workflow,
        [dataset_id_str, 'a_pipeline', 'arcana.test.tasks:concatenate',
         '--source', 'file1', 'in_file1', 'common:Text',
         '--source', 'file2', 'in_file2', 'common:Text',
         '--sink', 'concatenated', 'out_file', 'common:Text'])
    assert result.exit_code == 0, show_cli_trace(result)
    loaded_dataset = Dataset.load(dataset_id_str)
    assert saved_dataset.pipelines == loaded_dataset.pipelines