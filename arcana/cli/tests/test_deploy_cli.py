import pytest
import tempfile
from pathlib import Path
from click.testing import CliRunner
from arcana.cli.deploy import build_all

@pytest.mark.skip("needs to be updated to match refactoring")
def test_deploy_build():

    build_dir = Path(tempfile.mkdtemp())
    docs_dir = Path(tempfile.mkdtemp())
    pkg_dir = Path(tempfile.mkdtemp()) / 'arcanatest'
    sub_pkg_dir = pkg_dir / 'wrapper'
    sub_pkg_dir.mkdir(parents=True)
    # Write package __init__.py
    for d in [pkg_dir, sub_pkg_dir]:
        with open(d / '__init__.py', 'w') as f:
            f.write('\n')
    with open(sub_pkg_dir / 'concatenate.py', 'w') as f:
        f.write(concatenate_module_contents)

    runner = CliRunner()
    result = runner.invoke(build_all,
                           [str(pkg_dir), '--build_dir', str(build_dir),
                            '--docs', str(docs_dir)])
    assert result.exit_code == 0, result.stdout
    assert result.output == 'docker.io/arcanatest/wrapper.concatenate:1.0-1\n'

concatenate_module_contents = """from arcana.data.formats.common import text

spec = {
    'commands': [
        {'pydra_task': 'arcana.tasks.tests.fixtures:concatenate',
         'inputs': [('in_file1', text), ('in_file2', text)],
         'outputs': [('out_file', text)],
         'parameters': ['duplicates'],
         'description': (
             "Concatenates two text files together into a single text file")}],
    'pkg_version': '1.0',
    'wrapper_version': '1',
    'packages': [],
    'python_packages': [],
    'base_image': None,
    'authors': ['some.one@an.email.org'],
    'info_url': 'http://concatenate.readthefakedocs.io'}
"""