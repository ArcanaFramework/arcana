from argparse import ArgumentParser
from arcana2.entrypoints.wrap4xnat import Wrap4XnatCmd
from arcana2.repositories.xnat.cs import InputArg, OutputArg
from arcana2.datatypes import text
from arcana2.dataspaces.clinical import Clinical
from arcana2.test_fixtures.tasks import concatenate


def test_wrap4xnat_argparse():
    parser = ArgumentParser()
    Wrap4XnatCmd.construct_parser(parser)
    args = parser.parse_args([
        'arcana2.test_fixtures.tasks.concatenate',
        'arcana-test-concatenate',
        '--input', 'in_file1', 'text',
        '--input', 'in_file2', 'text',
        '--output', 'out_file', 'text',
        '--parameter', 'duplicates',
        '--requirement', 'mrtrix',
        '--package', 'sympy=1.1',
        '--frequency', 'session',
        '--registry', 'localhost:5959'])

    frequency = Wrap4XnatCmd.parse_frequency(args)
    assert frequency == Clinical.session
    inputs = list(Wrap4XnatCmd.parse_input_args(args, frequency))
    outputs = list(Wrap4XnatCmd.parse_output_args(args))

    assert inputs == [InputArg('in_file1', text, Clinical.session),
                      InputArg('in_file2', text, Clinical.session)]
    assert outputs == [OutputArg('out_file', text)]

    assert Wrap4XnatCmd.parse_image_name(args) == 'arcana-test-concatenate:latest'

    pydra_task = Wrap4XnatCmd.parse_interface(args)

    assert pydra_task.name == concatenate().name
