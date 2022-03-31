import tempfile
from pathlib import Path
from argparse import ArgumentParser
import pytest
from arcana.test.datasets import make_dataset
from arcana.test.fixtures.common import TEST_DATASET_BLUEPRINTS
from arcana.test.fixtures.medimage import (
    make_mutable_dataset as make_xnat_dataset,
    TEST_DATASET_BLUEPRINTS as TEST_XNAT_DATASET_BLUEPRINTS)
from arcana.data.formats import Text


@pytest.mark.skip("needs to be updated to match refactoring")
def test_derive_cli(work_dir, test_dataspace_location):

    dataset = make_dataset(TEST_DATASET_BLUEPRINTS['concatenate_test'], work_dir)
    
    parser = ArgumentParser()
    RunCmd.construct_parser(parser)
    args = parser.parse_args([
        'arcana.test.tasks:concatenate',
        str(dataset.id),
        '--store', 'common',
        '--input', 'in_file1', 'common:Text', 'file1', 
        '--input', 'in_file2', 'common:Text','file2',
        '--output', 'out_file', 'common:Text', 'deriv',
        '--dataspace', test_dataspace_location,
        '--hierarchy', 'abcd',
        '--frequency', 'abcd',
        '--parameter', 'duplicates', '2',
        '--pydra_plugin', 'serial'])
    RunCmd().run(args)

    dataset.add_sink('deriv', Text)

    for item in dataset['deriv']:
        with open(str(item.fs_path)) as f:
            contents = f.read()
        assert contents == '\n'.join(['file1.txt', 'file2.txt'] * 2)

@pytest.mark.skip("needs to be updated to match refactoring")
def test_derive_cli_via_xnat_api(xnat_repository, xnat_archive_dir, work_dir):

    dataset = make_xnat_dataset(xnat_repository, xnat_archive_dir,
                                test_name='concatenate_test.api')
    
    parser = ArgumentParser()
    RunCmd.construct_parser(parser)
    args = parser.parse_args([
        'arcana.test.tasks:concatenate',
        dataset.id,
        '--input', 'in_file1', 'Text', 'scan1:Text',
        '--input', 'in_file2', 'Text', 'scan2:Text',
        '--output', 'out_file', 'Text', 'deriv:Text',
        '--parameter', 'duplicates', '2',
        '--work', str(work_dir),
        '--store', 'xnat', xnat_repository.server, xnat_repository.user, xnat_repository.password,
        '--ids', 'timepoint0group0member0',
        '--pydra_plugin', 'serial'])
    RunCmd().run(args)

    dataset.add_sink('deriv', Text)

    for item in dataset['deriv']:
        item.get()
        with open(item.fs_path) as f:
            contents = f.read()
        assert contents == '\n'.join(['file1.txt', 'file2.txt'] * 2)

@pytest.mark.skip("needs to be updated to match refactoring")
def test_derive_cli_via_xnat_cs(xnat_repository, xnat_archive_dir, work_dir):

    dataset = make_xnat_dataset(xnat_repository, xnat_archive_dir,
                                test_name='concatenate_test.direct')

    output_dir = Path(tempfile.mkdtemp())

    session_label = 'timepoint0group0member0'
    
    parser = ArgumentParser()
    RunCmd.construct_parser(parser)
    args = parser.parse_args([
        'arcana.test.tasks:concatenate',
        dataset.id,
        '--input', 'in_file1', 'Text', 'scan1:Text',
        '--input', 'in_file2', 'Text', 'scan2:Text',
        '--output', 'out_file', 'Text', 'deriv:Text',
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
