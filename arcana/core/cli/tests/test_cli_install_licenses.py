from pathlib import Path
import tempfile
import pytest
from arcana.core.data.set import Dataset
from arcana.core.cli.deploy import install_license
from arcana.core.utils.misc import show_cli_trace
from arcana.testing import MockRemote

LICENSE_CONTENTS = "test license"


@pytest.fixture(scope="module")
def test_license():
    tmp_dir = Path(tempfile.mkdtemp())
    test_license = tmp_dir / "license.txt"
    test_license.write_text(LICENSE_CONTENTS)
    return str(test_license)


def test_cli_install_dataset_license(
    saved_dataset: Dataset, test_license, arcana_home, cli_runner, tmp_path
):
    store_nickname = saved_dataset.id + "_store"
    license_name = "test-license"
    saved_dataset.store.save(store_nickname)
    dataset_locator = (
        store_nickname + "//" + saved_dataset.id + "@" + saved_dataset.name
    )

    result = cli_runner(
        install_license,
        [
            license_name,
            test_license,
            dataset_locator,
        ],
    )
    assert result.exit_code == 0, show_cli_trace(result)
    assert saved_dataset.get_license_file(license_name).contents == LICENSE_CONTENTS

    # Test overwriting
    new_contents = "new_contents"
    new_license = tmp_path / "new-license.txt"
    new_license.write_text(new_contents)

    result = cli_runner(
        install_license,
        [
            license_name,
            str(new_license),
            dataset_locator,
        ],
    )
    assert result.exit_code == 0, show_cli_trace(result)
    assert saved_dataset.get_license_file(license_name).contents == new_contents


def test_cli_install_site_license(
    data_store,
    test_license: str,
    arcana_home,
    cli_runner,
):
    store_nickname = "site_license_store"
    license_name = "test-license"
    data_store.save(store_nickname)

    if isinstance(data_store, MockRemote):
        env = {
            data_store.SITE_LICENSES_USER_ENV: "arbitrary_user",
            data_store.SITE_LICENSES_PASS_ENV: "arbitrary_password",
        }
    else:
        env = {}

    result = cli_runner(
        install_license,
        [
            license_name,
            test_license,
            store_nickname,
        ],
        env=env,
    )

    assert result.exit_code == 0, show_cli_trace(result)

    assert (
        data_store.get_site_license_file(
            license_name, user="arbitrary_user", password="arbitrary_password"
        ).contents
        == LICENSE_CONTENTS
    )
