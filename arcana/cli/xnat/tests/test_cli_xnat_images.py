import sys
import os
from copy import copy
import json
import yaml
from unittest.mock import patch
import pytest
import docker
import xnat
from arcana.core.cli.deploy import make
from arcana.cli.xnat.update_release import (
    pull_xnat_images,
    xnat_auth_refresh,
    PULL_IMAGES_XNAT_HOST_KEY,
    PULL_IMAGES_XNAT_USER_KEY,
    PULL_IMAGES_XNAT_PASS_KEY,
)
from arcana.core.utils.testing import show_cli_trace


@pytest.mark.xfail(
    sys.platform == "linux",
    reason=(
        "Haven't been able to setup either SSL for the Xnat4Tests test docker "
        "registry, or the internal host on GH Actions as an insecure "
        "registries"
    ),
)
def test_pull_xnat_images(
    xnat_repository,
    command_spec,
    work_dir,
    docker_registry_for_xnat_uri,
    cli_runner,
    run_prefix,
    xnat4tests_config,
):

    DOCKER_ORG = "pulltestorg"
    IMAGE_GROUP_NAME = "apackage"
    PKG_VERSION = "1.0"
    WRAPPER_VERSION = "1-pullimages"

    reverse_command_spec = copy(command_spec)
    reverse_command_spec["task"] = "arcana.core.utils.testing.tasks:concatenate_reverse"

    spec_dir = work_dir / DOCKER_ORG
    pkg_path = spec_dir / IMAGE_GROUP_NAME
    pkg_path.mkdir(parents=True)

    expected_images = []
    expected_commands = []

    for name, cmd_spec in (
        ("forward_concat-pullimages", command_spec),
        ("reverse_concat-pullimages", reverse_command_spec),
    ):

        image_spec = {
            "command": cmd_spec,
            "version": PKG_VERSION,
            "build_iteration": WRAPPER_VERSION,
            "authors": [{"name": "Some One", "email": "some.one@an.email.org"}],
            "info_url": "http://concatenate.readthefakedocs.io",
            "description": "a command to test build process",
        }

        with open((pkg_path / name).with_suffix(".yaml"), "w") as f:
            yaml.dump(image_spec, f)

        expected_images.append(
            f"{docker_registry_for_xnat_uri}/{DOCKER_ORG}/{IMAGE_GROUP_NAME}"
            f".{name}:{PKG_VERSION}-{WRAPPER_VERSION}"
        )
        expected_commands.append(f"{IMAGE_GROUP_NAME}.{name}")

    expected_images.sort()
    expected_commands.sort()

    build_dir = work_dir / "build"
    build_dir.mkdir()
    manifest_path = work_dir / "manifest.json"

    result = cli_runner(
        make,
        [
            str(spec_dir),
            "xnat:XnatCSImage",
            "--build-dir",
            str(build_dir),
            "--registry",
            docker_registry_for_xnat_uri,
            "--loglevel",
            "warning",
            "--use-local-packages",
            "--install-extras",
            "test",
            "--raise-errors",
            "--release",
            "test-pipelines-metapackage",
            run_prefix,
            "--save-manifest",
            str(manifest_path),
            "--use-test-config",
            "--push",
        ],
    )

    assert result.exit_code == 0, show_cli_trace(result)

    with open(manifest_path) as f:
        manifest = json.load(f)

    assert (
        sorted(f"{i['name']}:{i['version']}" for i in manifest["images"])
        == expected_images
    )

    # Delete images from local Docker instance (which the test XNAT uses)
    dc = docker.from_env()
    for img in expected_images:
        dc.images.remove(img)

    filters_file = work_dir / "filters.yaml"
    with open(filters_file, "w") as f:
        yaml.dump(
            {
                "include": [
                    {
                        "name": (
                            f"{docker_registry_for_xnat_uri}/"
                            f"{DOCKER_ORG}/{IMAGE_GROUP_NAME}.*"
                        )
                    }
                ],
            },
            f,
        )

    with patch.dict(
        os.environ,
        {
            PULL_IMAGES_XNAT_HOST_KEY: xnat4tests_config.xnat_uri,
            PULL_IMAGES_XNAT_USER_KEY: xnat4tests_config.xnat_user,
            PULL_IMAGES_XNAT_PASS_KEY: xnat4tests_config.xnat_password,
        },
    ):
        result = cli_runner(
            pull_xnat_images,
            [str(manifest_path), "--filters", str(filters_file)],
        )

    assert result.exit_code == 0, show_cli_trace(result)

    with xnat_repository:

        xlogin = xnat_repository.login

        result = xlogin.get("/xapi/commands/")

    # Check commands have been installed
    available_cmds = [e["name"] for e in result.json()]
    assert all(cmd in available_cmds for cmd in expected_commands)


def test_xnat_auth_refresh(xnat_repository, work_dir, cli_runner):

    config_path = work_dir / "config.yaml"
    with open(config_path, "w") as f:
        yaml.dump(
            {
                "server": xnat_repository.server,
            },
            f,
        )

    auth_path = work_dir / "auth.json"
    with open(auth_path, "w") as f:
        json.dump(
            {"alias": "admin", "secret": "admin"},
            f,
        )

    result = cli_runner(
        xnat_auth_refresh,
        [str(config_path), str(auth_path)],
    )

    assert result.exit_code == 0, show_cli_trace(result)

    with open(auth_path) as f:
        auth = yaml.load(f, Loader=yaml.Loader)

    assert len(auth["alias"]) > 20
    assert len(auth["secret"]) > 20

    assert xnat.connect(
        xnat_repository.server, user=auth["alias"], password=auth["secret"]
    )
