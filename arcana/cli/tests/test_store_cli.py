import os
from unittest.mock import patch
from arcana.cli.store import add, ls, remove, rename
from arcana.test.utils import show_cli_trace


def test_store_cli(xnat_repository, cli_runner, work_dir):
    test_home_dir = work_dir / 'test-arcana-home'
    # Create a new home directory so it doesn't conflict with user settings
    with patch.dict(os.environ, {'ARCANA_HOME': str(test_home_dir)}):
        # Add new XNAT configuration
        result = cli_runner(
            add,
            ['test-xnat', 'medimage:Xnat', xnat_repository.server,
             '--user', xnat_repository.user,
             '--password', xnat_repository.password])
        assert result.exit_code == 0, show_cli_trace(result)
        # List all saved and built-in stores
        result = cli_runner(ls, [])
        assert result.exit_code == 0, show_cli_trace(result)
        assert 'bids - arcana.data.stores.bids.structure:Bids' in result.output
        assert 'file - arcana.data.stores.common.file_system:FileSystem' in result.output
        assert 'test-xnat - arcana.data.stores.medimage.xnat.api:Xnat' in result.output
        assert '    server: ' + xnat_repository.server in result.output
        

def test_store_cli_remove(xnat_repository, cli_runner, work_dir):
    test_home_dir = work_dir / 'test-arcana-home'
    new_store_name = 'test-xnat'
    # Create a new home directory so it doesn't conflict with user settings
    with patch.dict(os.environ, {'ARCANA_HOME': str(test_home_dir)}):
        # Add new XNAT configuration
        cli_runner(
            add,
            [new_store_name, 'medimage:Xnat', xnat_repository.server,
             '--user', xnat_repository.user,
             '--password', xnat_repository.password])
        # Check store is saved
        result = cli_runner(ls, [])
        assert new_store_name in result.output

        cli_runner(remove, new_store_name)
        # Check store is gone
        result = cli_runner(ls, [])
        assert new_store_name not in result.output
        
def test_store_cli_rename(xnat_repository, cli_runner, work_dir):
    test_home_dir = work_dir / 'test-arcana-home'
    old_store_name = 'test-xnat'
    new_store_name = 'test-xnat-renamed'
    # Create a new home directory so it doesn't conflict with user settings
    with patch.dict(os.environ, {'ARCANA_HOME': str(test_home_dir)}):
        # Add new XNAT configuration
        cli_runner(
            add,
            [old_store_name, 'medimage:Xnat', xnat_repository.server,
             '--user', xnat_repository.user,
             '--password', xnat_repository.password])
        # Check store is saved
        result = cli_runner(ls, [])
        assert old_store_name in result.output

        cli_runner(rename,[old_store_name,new_store_name])
        # Check store is renamed
        result = cli_runner(ls, [])
        assert old_store_name not in result.output
        assert new_store_name in result.output
        
