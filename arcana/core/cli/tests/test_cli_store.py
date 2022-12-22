import os
from unittest.mock import patch
from arcana.core.cli.store import add, ls, remove, rename
from arcana.core.utils.testing import show_cli_trace
from arcana.core.data.store import DataStore

STORE_URI = "http://dummy.uri"
STORE_USER = "a_user"
STORE_PASSWORD = "a-password"


def test_store_cli(cli_runner, work_dir):
    test_home_dir = work_dir / "test-arcana-home"
    store_name = "test-mock"
    # Create a new home directory so it doesn't conflict with user settings
    with patch.dict(os.environ, {"ARCANA_HOME": str(test_home_dir)}):
        # Add new XNAT configuration
        result = cli_runner(
            add,
            [
                store_name,
                "arcana.core.utils.testing.data:MockDataStore",
                STORE_URI,
                "--user",
                STORE_USER,
                "--password",
                STORE_PASSWORD,
            ],
        )
        assert result.exit_code == 0, show_cli_trace(result)
        # List all saved and built-in stores
        result = cli_runner(ls, [])
        assert result.exit_code == 0, show_cli_trace(result)
        assert "file - arcana.dirtree.data.file_system:FileSystem" in result.output
        assert (
            f"{store_name} - arcana.core.utils.testing.data:MockDataStore"
            in result.output
        )
        assert "    server: " + STORE_URI in result.output


def test_store_cli_remove(cli_runner, work_dir):
    test_home_dir = work_dir / "test-arcana-home"
    new_store_name = "a-new-mock"
    # Create a new home directory so it doesn't conflict with user settings
    with patch.dict(os.environ, {"ARCANA_HOME": str(test_home_dir)}):
        # Add new XNAT configuration
        cli_runner(
            add,
            [
                new_store_name,
                "arcana.core.utils.testing.data:MockDataStore",
                STORE_URI,
                "--user",
                STORE_USER,
                "--password",
                STORE_PASSWORD,
            ],
        )
        # Check store is saved
        result = cli_runner(ls, [])
        assert new_store_name in result.output

        cli_runner(remove, new_store_name)
        # Check store is gone
        result = cli_runner(ls, [])
        assert new_store_name not in result.output


def test_store_cli_rename(cli_runner, work_dir):
    test_home_dir = work_dir / "test-arcana-home"
    old_store_name = "i123"
    new_store_name = "y456"
    # Create a new home directory so it doesn't conflict with user settings
    with patch.dict(os.environ, {"ARCANA_HOME": str(test_home_dir)}):
        # Add new XNAT configuration
        cli_runner(
            add,
            [
                old_store_name,
                "arcana.core.utils.testing.data:MockDataStore",
                STORE_URI,
                "--user",
                STORE_USER,
                "--password",
                STORE_PASSWORD,
            ],
        )
        # Check store is saved
        result = cli_runner(ls, [])
        assert "i123 - arcana.core.utils.testing.data:MockDataStore" in result.output

        cli_runner(rename, [old_store_name, new_store_name])
        # Check store is renamed
        result = cli_runner(ls, [])
        assert (
            "i123 - arcana.core.utils.testing.data:MockDataStore" not in result.output
        )
        assert "y456 - arcana.core.utils.testing.data:MockDataStore" in result.output


def test_store_cli_encrypt_credentials(cli_runner, work_dir):
    test_home_dir = work_dir / "test-arcana-home"
    store_name = "another-test-mock"
    # Create a new home directory so it doesn't conflict with user settings
    with patch.dict(os.environ, {"ARCANA_HOME": str(test_home_dir)}):
        # Add new XNAT configuration
        result = cli_runner(
            add,
            [
                store_name,
                "arcana.core.utils.testing.data:MockDataStore",
                STORE_URI,
                "--user",
                STORE_USER,
                "--password",
                STORE_PASSWORD,
            ],
        )
        assert result.exit_code == 0, show_cli_trace(result)
        # Check credentials have been encrypted
        loaded_store = DataStore.load(store_name)
        assert loaded_store.password != ""
        assert loaded_store.password is not STORE_PASSWORD
        assert loaded_store.user != ""
        assert loaded_store.user is not STORE_USER
