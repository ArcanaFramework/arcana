import os
from unittest.mock import patch
from arcana.cli.store import add, ls
from arcana.tests.utils import show_cli_trace


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
        
