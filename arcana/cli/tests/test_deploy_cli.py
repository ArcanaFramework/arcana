import yaml
from arcana.cli.deploy import build
from arcana.test.utils import show_cli_trace


def test_deploy_build(command_spec, cli_runner, work_dir):

    DOCKER_ORG = 'test-org'
    DOCKER_REGISTRY = 'test-registry'
    PKG_NAME = 'test-pkg'

    concatenate_spec = {
        'pkg_name': 'concatenate',
        'commands': [command_spec],
        'pkg_version': '1.0',
        'wrapper_version': '1',
        'packages': [],
        'python_packages': [],
        'base_image': None,
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
                         '--loglevel', 'warning'])
    assert result.exit_code == 0, show_cli_trace(result)
    assert result.output == f'{DOCKER_REGISTRY}/{DOCKER_ORG}/{PKG_NAME}.concatenate:1.0-1\n'
