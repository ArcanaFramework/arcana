from arcana.core.cli.store import add, ls, remove, rename
from arcana.core.utils.misc import show_cli_trace
from arcana.core.data.store import DataStore

STORE_URI = "http://dummy.uri"
STORE_USER = "a_user"
STORE_PASSWORD = "a-password"


def test_store_cli(cli_runner, arcana_home, work_dir):
    store_name = "test-mock-remote"
    # Add new XNAT configuration
    result = cli_runner(
        add,
        [
            store_name,
            "testing:MockRemote",
            "--server",
            STORE_URI,
            "--user",
            STORE_USER,
            "--password",
            STORE_PASSWORD,
            "--option",
            "remote_dir",
            str(work_dir / "remote-dir"),
        ],
    )
    assert result.exit_code == 0, show_cli_trace(result)
    # List all saved and built-in stores
    result = cli_runner(ls, [])
    assert result.exit_code == 0, show_cli_trace(result)
    assert f"{store_name} - arcana.testing.data.store:MockRemote" in result.output
    assert "    server: " + STORE_URI in result.output


def test_store_cli_remove(cli_runner, arcana_home, work_dir):
    new_store_name = "a-new-mock"
    # Add new XNAT configuration
    cli_runner(
        add,
        [
            new_store_name,
            "testing:MockRemote",
            "--server",
            STORE_URI,
            "--user",
            STORE_USER,
            "--password",
            STORE_PASSWORD,
            "--option",
            "remote_dir",
            str(work_dir / "remote-dir"),
        ],
    )
    # Check store is saved
    result = cli_runner(ls, [])
    assert new_store_name in result.output

    cli_runner(remove, new_store_name)
    # Check store is gone
    result = cli_runner(ls, [])
    assert new_store_name not in result.output


def test_store_cli_rename(cli_runner, arcana_home, work_dir):
    old_store_name = "i123"
    new_store_name = "y456"
    # Add new XNAT configuration
    cli_runner(
        add,
        [
            old_store_name,
            "testing:MockRemote",
            "--server",
            STORE_URI,
            "--user",
            STORE_USER,
            "--password",
            STORE_PASSWORD,
            "--option",
            "remote_dir",
            str(work_dir / "remote-dir"),
        ],
    )
    # Check store is saved
    result = cli_runner(ls, [])
    assert "i123 - arcana.testing.data.store:MockRemote" in result.output

    cli_runner(rename, [old_store_name, new_store_name])
    # Check store is renamed
    result = cli_runner(ls, [])
    assert "i123 - arcana.testing.data.store:MockRemote" not in result.output
    assert "y456 - arcana.testing.data.store:MockRemote" in result.output


def test_store_cli_encrypt_credentials(cli_runner, arcana_home, work_dir):
    store_name = "another-test-mock"
    # Add new XNAT configuration
    result = cli_runner(
        add,
        [
            store_name,
            "testing:MockRemote",
            "--server",
            STORE_URI,
            "--user",
            STORE_USER,
            "--password",
            STORE_PASSWORD,
            "--option",
            "remote_dir",
            str(work_dir / "remote-dir"),
        ],
    )
    assert result.exit_code == 0, show_cli_trace(result)
    # Check credentials have been encrypted
    loaded_store = DataStore.load(store_name)
    assert loaded_store.password != ""
    assert loaded_store.password is not STORE_PASSWORD
    assert loaded_store.user != ""
    assert loaded_store.user is not STORE_USER
