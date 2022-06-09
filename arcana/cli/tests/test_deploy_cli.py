import yaml
from functools import reduce
from operator import mul
import logging
import sys
import tempfile
from pathlib import Path
import pytest
import docker
from arcana.cli.deploy import build, run_pipeline
from arcana.core.utils import class_location
from arcana.test.utils import show_cli_trace, make_dataset_id_str
from arcana.data.formats.common import Text
from arcana.exceptions import ArcanaBuildError


def test_deploy_build_cli(command_spec, cli_runner, work_dir):

    DOCKER_ORG = 'testorg'
    DOCKER_REGISTRY = 'test.registry.org'
    PKG_NAME = 'testpkg'

    concatenate_spec = {
        'commands': [command_spec],
        'pkg_version': '1.0',
        'wrapper_version': '1',
        'system_packages': ['vim'],  # just to test it out
        'python_packages': ['pytest'],  # just to test it out
        'authors': ['some.one@an.email.org'],
        'info_url': 'http://concatenate.readthefakedocs.io'}

    build_dir = work_dir / 'build'
    build_dir.mkdir()
    spec_path = work_dir / 'test-specs'
    sub_dir = spec_path / PKG_NAME
    sub_dir.mkdir(parents=True)
    with open(sub_dir / 'concatenate.yml', 'w') as f:
        yaml.dump(concatenate_spec, f)

    result = cli_runner(build,
                        [str(spec_path), DOCKER_ORG,
                         '--build_dir', str(build_dir),
                         '--registry', DOCKER_REGISTRY,
                         '--loglevel', 'warning',
                         '--use-local-packages',
                         '--install_extras', 'test',
                         '--raise-errors',
                         '--use-test-config',
                         '--dont-check-against-prebuilt'])
    assert result.exit_code == 0, show_cli_trace(result)
    tag = result.output.strip()
    assert tag == f'{DOCKER_REGISTRY}/{DOCKER_ORG}/{PKG_NAME}.concatenate:1.0-1'

    # Clean up the built image
    dc = docker.from_env()
    dc.images.remove(tag)


def test_deploy_rebuild_cli(command_spec, docker_registry, cli_runner, run_prefix):
    """Tests the check to see whether """

    DOCKER_ORG = 'testorg'
    PKG_NAME = 'testpkg-rebuild' + run_prefix

    def build_spec(spec):
        work_dir = Path(tempfile.mkdtemp())
        build_dir = work_dir / 'build'
        build_dir.mkdir()
        spec_path = work_dir / 'test-specs'
        sub_dir = spec_path / PKG_NAME
        sub_dir.mkdir(parents=True)
        with open(sub_dir / 'concatenate.yml', 'w') as f:
            yaml.dump(spec, f)

        result = cli_runner(build,
                            [str(spec_path), DOCKER_ORG,
                            '--build_dir', str(build_dir),
                            '--registry', docker_registry,
                            '--loglevel', 'warning',
                            '--use-local-packages',
                            '--install_extras', 'test',
                            '--raise-errors',
                            '--use-test-config'])
        assert result.exit_code == 0, show_cli_trace(result)
        return result

    concatenate_spec = {
        'commands': [command_spec],
        'pkg_version': '1.0',
        'wrapper_version': '1',
        'system_packages': [],
        'python_packages': [],
        'authors': ['some.one@an.email.org'],
        'info_url': 'http://concatenate.readthefakedocs.io'}

    # Build a basic image
    result = build_spec(concatenate_spec)
    tag = result.output.strip()

    dc = docker.from_env()
    dc.api.push(tag)

    # FIXME: Need to ensure that logs are captured properly
    # result = build_spec(concatenate_spec)
    # assert "Skipping" in result.output

    # Modify the spec so it doesn't match the original that has just been
    # built (but don't increment the version number -> image tag so there
    # is a clash)
    concatenate_spec['system_packages'].append('vim')

    with pytest.raises(ArcanaBuildError) as excinfo:
        build_spec(concatenate_spec)

    assert "doesn't match the one that was used to build the image" in str(excinfo.value)

    # Increment the version number to avoid the clash
    concatenate_spec['wrapper_version'] = '2'

    result = build_spec(concatenate_spec) 
    rebuilt_tag = result.output.strip()

    # Clean up the built images
    dc.images.remove(tag)
    dc.images.remove(rebuilt_tag)


def test_run_pipeline_cli(concatenate_task, saved_dataset, cli_runner, work_dir):
    # Get CLI name for dataset (i.e. file system path prepended by 'file//')
    dataset_id_str = make_dataset_id_str(saved_dataset)
    bp = saved_dataset.blueprint
    duplicates = 1
    # Start generating the arguments for the CLI
    # Add source to loaded dataset
    result = cli_runner(
        run_pipeline,
        [dataset_id_str, 'a_pipeline', 'arcana.test.tasks:' + concatenate_task.__name__,
         '--input', 'source1', 'common:Text', 'file1', 'in_file1', 'common:Text',
         '--input', 'source2', 'common:Text', 'file2', 'in_file2', 'common:Text',
         '--output', 'sink1', 'common:Text', 'concatenated', 'out_file', 'common:Text',
         '--parameter', 'duplicates', str(duplicates),
         '--plugin', 'serial',
         '--work', str(work_dir),
         '--loglevel', 'debug',
         '--dataset_space', class_location(bp.space),
         '--dataset_hierarchy'] + [str(l) for l in bp.hierarchy])
    assert result.exit_code == 0, show_cli_trace(result)
    # Add source column to saved dataset
    sink = saved_dataset.add_sink('concatenated', Text)
    assert len(sink) == reduce(mul, saved_dataset.blueprint.dim_lengths)
    fnames = ['file1.txt', 'file2.txt']
    if concatenate_task.__name__.endswith('reverse'):
        fnames = [f[::-1] for f in fnames]
    expected_contents = '\n'.join(fnames * duplicates)
    for item in sink:
        item.get(assume_exists=True)
        with open(item.fs_path) as f:
            contents = f.read()
        assert contents == expected_contents


def test_run_pipeline_cli_fail(concatenate_task, saved_dataset, cli_runner, work_dir):
    # Get CLI name for dataset (i.e. file system path prepended by 'file//')
    dataset_id_str = make_dataset_id_str(saved_dataset)
    bp = saved_dataset.blueprint
    duplicates = 1
    # Start generating the arguments for the CLI
    # Add source to loaded dataset
    result = cli_runner(
        run_pipeline,
        [dataset_id_str, 'a_pipeline', 'arcana.test.tasks:' + concatenate_task.__name__,
        '--input', 'source1', 'common:Text', 'bad-file-path', 'in_file1', 'common:Text',
        '--input', 'source2', 'common:Directory', 'file2', 'in_file2', 'common:Directory',
        '--output', 'sink1', 'common:Text', 'concatenated', 'out_file', 'common:Text',
        '--parameter', 'duplicates', str(duplicates),
        '--plugin', 'serial',
        '--loglevel', 'error',
        '--work', str(work_dir),
        '--dataset_space', class_location(bp.space),
        '--dataset_hierarchy'] + [str(l) for l in bp.hierarchy])
    assert result.exit_code == 1  # fails due to missing path for source1 and incorrect format of source2
    # TODO: Should try to read logs to check for error message but can't work out how to capture them
