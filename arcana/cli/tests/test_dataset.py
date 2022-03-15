import pytest
from click.testing import CliRunner
from arcana.core.data.set import Dataset
from arcana.cli.dataset import define, add_source, add_sink, missing_items

ARBITRARY_INTS_A = [234221, 93380, 43271, 137483, 30009, 214205, 363526]
ARBITRARY_INTS_B = [353726, 29202, 32867, 129872, 12281, 776524, 908763]

def get_arbitrary_slice(i, dim_length):
    a = ARBITRARY_INTS_A[i] % dim_length
    b = ARBITRARY_INTS_B[i] % dim_length
    lower = min(a, b)
    upper = max(a, b) + 1
    return lower, upper


@pytest.mark.skip("Not implemented")
def test_add_source_cli(dataset):
  runner = CliRunner()
  result = runner.invoke(add_source, [])
  assert result.exit_code == 0


@pytest.mark.skip('Not implemented')
def test_dataset_define_cli(dataset):
    runner = CliRunner()
    # Get CLI name for dataset (i.e. file system path prepended by 'file/')
    path = 'file/' + dataset.id
    # Start generating the arguments for the CLI
    args = [str(h) for h in dataset.blueprint.hierarchy]
    # Generate "arbitrary" values for included and excluded from dim length
    # and index
    included = {}
    excluded = {}
    for i, (dim_length, axis) in enumerate(zip(dataset.blueprint.dim_lengths,
                                            dataset.space.axes)):
        slce = get_arbitrary_slice(i, dim_length)
        if i % 2:
            included[axis] = slce
        elif (slce[1] - slce[0]) < dim_length:  # Check that we aren't excluding all
            excluded[axis] = slce
    # Add include and exclude options
    for axis, (a, b) in included.items():
        args.extend(['--include', f'{a}:{b}'])
    for axis, (a, b) in included.items():
        args.extend(['--exclude', f'{a}:{b}'])
    # Run the command line
    result = runner.invoke(define, [path, *args])
    # Check tool completed successfully
    assert result.exit_code == 0
    # Reload the saved dataset and check the parameters were saved/loaded
    # correctly
    loaded_dataset = Dataset.load(path)
    assert loaded_dataset.hierarchy == dataset.blueprint.hierarchy
    assert loaded_dataset.included == included
    assert loaded_dataset.excluded == excluded

