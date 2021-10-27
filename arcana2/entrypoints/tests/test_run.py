from argparse import ArgumentParser
from arcana2.test_fixtures.dataset import TEST_DATASET_BLUEPRINTS, make_dataset
from arcana2.entrypoints.run import RunCmd
from arcana2.datatypes import text


def test_run_app(work_dir):

    dataset = make_dataset(TEST_DATASET_BLUEPRINTS['basic'], work_dir)
    
    parser = ArgumentParser()
    RunCmd.construct_parser(parser)
    args = parser.parse_args([
        'arcana2.test_fixtures.tasks.concatenate',
        str(dataset.name),
        '--repository', 'file_system',
        '--input', 'in_file1', 'file1', 'text',
        '--input', 'in_file2', 'file2', 'text',
        '--output', 'out_file', 'deriv', 'text',
        '--dataspace', 'arcana2.test_fixtures.dataset.TestDataSpace',
        '--hierarchy', 'abcd',
        '--frequency', 'abcd',
        '--parameter', 'duplicates', '2'])
    RunCmd().run(args)

    dataset.add_sink('deriv', text)

    for item in dataset['deriv']:
        with open(item.fs_path) as f:
            contents = f.read()
        assert contents == '\n'.join(['file1.txt', 'file2.txt'] * 2)
