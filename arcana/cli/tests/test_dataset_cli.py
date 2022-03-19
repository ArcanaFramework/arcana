import pytest
import os.path
from arcana.core.data.set import Dataset
from arcana.core.data.spec import DataSource, DataSink
from arcana.core.enum import DataQuality, DataSalience
from arcana.data.stores.tests.fixtures import (
  TestDatasetBlueprint, TestDataSpace, make_dataset, get_dataset_path)
from arcana.cli.dataset import define, add_source, add_sink, missing_items
from arcana.data.formats.common import text

ARBITRARY_INTS_A = [234221, 93380, 43271, 137483, 30009, 214205, 363526]
ARBITRARY_INTS_B = [353726, 29202, 32867, 129872, 12281, 776524, 908763]


def get_arbitrary_slice(i, dim_length):
    a = ARBITRARY_INTS_A[i] % dim_length
    b = ARBITRARY_INTS_B[i] % dim_length
    lower = min(a, b)
    upper = max(a, b) + 1
    return lower, upper

@pytest.fixture
def basic_dataset(work_dir):

    blueprint = TestDatasetBlueprint(
        [TestDataSpace.abcd],  # e.g. XNAT where session ID is unique in project but final layer is organised by timepoint
        [1, 1, 1, 1],
        ['file1.txt'],
        {}, {}, [])

    dataset_path = get_dataset_path('most_basic', work_dir)
    return make_dataset(blueprint, dataset_path)


def test_add_column_cli(basic_dataset, cli_runner):
    # Get CLI name for dataset (i.e. file system path prepended by 'file//')
    dataset_path = 'file//' + os.path.abspath(basic_dataset.id)
    # Start generating the arguments for the CLI
    args = [str(h) for h in basic_dataset.blueprint.hierarchy]
    # Generate "arbitrary" values for included and excluded from dim length
    # and index
    args.extend(['--space', 'arcana.data.stores.tests.fixtures:TestDataSpace'])
    # Run the command line
    result = cli_runner(define, [dataset_path, *args])
    # Check tool completed successfully
    assert result.exit_code == 0
    # Add source to loaded dataset
    basic_dataset.add_source(
        name='a_source',
        path='file1',
        format=text,
        frequency=TestDataSpace.d,
        quality_threshold=DataQuality.questionable,
        order=1,
        header_vals={},
        is_regex=False)
    # Add source column to saved dataset
    result = cli_runner(
        add_source,
        [dataset_path, 'a_source', 'common:Text',
         '--path', 'file1',
         '--frequency', 'd',
         '--quality', 'questionable',
         '--no-regex'])
    assert result.exit_code == 0
    # Add source to loaded dataset
    basic_dataset.add_sink(
        name='a_sink',
        path='deriv',
        format=text,
        frequency=TestDataSpace.d,
        salience=DataSalience.qa,
        pipeline_name='a_pipeline')
    result = cli_runner(add_sink, [
        dataset_path, 'a_sink', 'common:Text',
        '--path', 'deriv',
        '--frequency', 'd',
        '--salience', 'qa'])
    assert result.exit_code == 0
    # Reload the saved dataset and check the parameters were saved/loaded
    # correctly
    loaded_dataset = Dataset.load(dataset_path)
    assert basic_dataset.column_specs == loaded_dataset.column_specs


@pytest.mark.skip("Not implemented")
def test_add_missing_items_cli(dataset, cli_runner):
  result = cli_runner(missing_items, [])
  assert result.exit_code == 0


def test_define_cli(dataset, cli_runner):
    # Get CLI name for dataset (i.e. file system path prepended by 'file//')
    path = 'file//' + os.path.abspath(dataset.id)
    # Start generating the arguments for the CLI
    args = [str(h) for h in dataset.blueprint.hierarchy]
    # Generate "arbitrary" values for included and excluded from dim length
    # and index
    included = []
    excluded = []
    for i, (dim_length, axis) in enumerate(zip(dataset.blueprint.dim_lengths,
                                               dataset.space)):
        a, b = get_arbitrary_slice(i, dim_length)
        if i % 2:
            included.append((axis, f'{a}:{b}'))
        elif (b - a) < dim_length:  # Check that we aren't excluding all
            excluded.append((axis, f'{a}:{b}'))
    # Add include and exclude options
    for axis, slce in included:
        args.extend(['--include', str(axis), slce])
    for axis, slce in excluded:
        args.extend(['--exclude', str(axis), slce])
    args.extend(['--space', 'arcana.data.stores.tests.fixtures:TestDataSpace'])
    # Run the command line
    result = cli_runner(define, [path, *args])
    # Check tool completed successfully
    assert result.exit_code == 0
    # Reload the saved dataset and check the parameters were saved/loaded
    # correctly
    loaded_dataset = Dataset.load(path)
    assert loaded_dataset.hierarchy == dataset.blueprint.hierarchy
    assert loaded_dataset.include == included
    assert loaded_dataset.exclude == excluded

