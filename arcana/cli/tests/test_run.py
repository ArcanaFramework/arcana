import tempfile
from pathlib import Path
from argparse import ArgumentParser
from arcana.data.stores.tests.fixtures import TEST_DATASET_BLUEPRINTS, make_dataset
from arcana.data.stores.xnat.tests.fixtures import (
    make_mutable_dataset as make_xnat_dataset,
    TEST_DATASET_BLUEPRINTS as TEST_XNAT_DATASET_BLUEPRINTS)
from arcana.cli.derive import RunCmd
from arcana.data.formats import text


def test_run_app(work_dir, test_dataspace_location):

    dataset = make_dataset(TEST_DATASET_BLUEPRINTS['concatenate_test'], work_dir)
    
    parser = ArgumentParser()
    RunCmd.construct_parser(parser)
    args = parser.parse_args([
        'arcana.tasks.tests.fixtures:concatenate',
        str(dataset.id),
        '--store', 'file_system',
        '--input', 'in_file1', 'text', 'file1', 
        '--input', 'in_file2', 'text','file2',
        '--output', 'out_file', 'text', 'deriv',
        '--dataspace', test_dataspace_location,
        '--hierarchy', 'abcd',
        '--frequency', 'abcd',
        '--parameter', 'duplicates', '2',
        '--pydra_plugin', 'serial'])
    RunCmd().run(args)

    dataset.add_sink('deriv', text)

    for item in dataset['deriv']:
        with open(str(item.fs_path)) as f:
            contents = f.read()
        assert contents == '\n'.join(['file1.txt', 'file2.txt'] * 2)


def test_run_app_via_xnat_api(xnat_repository, xnat_archive_dir, work_dir):

    dataset = make_xnat_dataset(xnat_repository, xnat_archive_dir,
                                test_name='concatenate_test.api')
    
    parser = ArgumentParser()
    RunCmd.construct_parser(parser)
    args = parser.parse_args([
        'arcana.tasks.tests.fixtures:concatenate',
        dataset.id,
        '--input', 'in_file1', 'text', 'scan1:text',
        '--input', 'in_file2', 'text', 'scan2:text',
        '--output', 'out_file', 'text', 'deriv:text',
        '--parameter', 'duplicates', '2',
        '--work', str(work_dir),
        '--store', 'xnat', xnat_repository.server, xnat_repository.user, xnat_repository.password,
        '--ids', 'timepoint0group0member0',
        '--pydra_plugin', 'serial'])
    RunCmd().run(args)

    dataset.add_sink('deriv', text)

    for item in dataset['deriv']:
        item.get()
        with open(item.fs_path) as f:
            contents = f.read()
        assert contents == '\n'.join(['file1.txt', 'file2.txt'] * 2)


def test_run_app_via_xnat_cs(xnat_repository, xnat_archive_dir, work_dir):

    dataset = make_xnat_dataset(xnat_repository, xnat_archive_dir,
                                test_name='concatenate_test.direct')

    output_dir = Path(tempfile.mkdtemp())

    session_label = 'timepoint0group0member0'
    
    parser = ArgumentParser()
    RunCmd.construct_parser(parser)
    args = parser.parse_args([
        'arcana.tasks.tests.fixtures:concatenate',
        dataset.id,
        '--input', 'in_file1', 'text', 'scan1:text',
        '--input', 'in_file2', 'text', 'scan2:text',
        '--output', 'out_file', 'text', 'deriv:text',
        '--parameter', 'duplicates', '2',
        '--work', str(work_dir),
        '--store', 'xnat_via_cs', 'session', session_label,
        xnat_repository.server, xnat_repository.user, xnat_repository.password, 
        str(xnat_archive_dir / dataset.id / 'arc001' / session_label),
        str(output_dir),
        '--ids', session_label, '--pydra_plugin', 'serial'])
    RunCmd().run(args)

    output_files = [p.name for p in output_dir.iterdir()]
    assert output_files == ['deriv.txt']
    with open(output_dir / 'deriv.txt') as f:
        contents = f.read()
    assert contents == '\n'.join(['file1.txt', 'file2.txt'] * 2)
