from click.testing import CliRunner
from ..column import add_source, add_sink


def test_column_ci(dataset):
  runner = CliRunner()
  result = runner.invoke(hello, ['Peter'])
  assert result.exit_code == 0
  assert result.output == 'Hello Peter!\n'