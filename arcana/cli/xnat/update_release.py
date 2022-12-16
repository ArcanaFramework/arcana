import re
import json
import yaml
import click
import xnat
from .base import xnat_group


PULL_IMAGES_XNAT_HOST_KEY = "XNAT_HOST"
PULL_IMAGES_XNAT_USER_KEY = "XNAT_USER"
PULL_IMAGES_XNAT_PASS_KEY = "XNAT_PASS"


@xnat_group.command(
    name="pull-images",
    help=f"""Updates the installed pipelines on an XNAT instance from a manifest
JSON file using the XNAT instance's REST API.

MANIFEST_FILE is a JSON file containing a list of container images built in a release
created by `arcana deploy xnat build`

Authentication credentials can be passed through the {PULL_IMAGES_XNAT_USER_KEY}
and {PULL_IMAGES_XNAT_PASS_KEY} environment variables. Otherwise, tokens can be saved
in a JSON file passed to '--auth'.

Which of available pipelines to install can be controlled by a YAML file passed to the
'--filters' option of the form
    \b
    include:
    - tag: ghcr.io/Australian-Imaging-Service/mri.human.neuro.*
    - tag: ghcr.io/Australian-Imaging-Service/pet.rodent.*
    exclude:
    - tag: ghcr.io/Australian-Imaging-Service/mri.human.neuro.bidsapps.
""",
)
@click.argument("manifest_file", type=click.File())
@click.option(
    "--server",
    type=str,
    envvar=PULL_IMAGES_XNAT_HOST_KEY,
    help=("the username used to authenticate with the XNAT instance to update"),
)
@click.option(
    "--user",
    envvar=PULL_IMAGES_XNAT_USER_KEY,
    help=("the username used to authenticate with the XNAT instance to update"),
)
@click.option(
    "--password",
    envvar=PULL_IMAGES_XNAT_PASS_KEY,
    help=("the password used to authenticate with the XNAT instance to update"),
)
@click.option(
    "--filters",
    "filters_file",
    default=None,
    type=click.File(),
    help=("a YAML file containing filter rules for the images to install"),
)
def pull_xnat_images(manifest_file, server, user, password, filters_file):
    manifest = json.load(manifest_file)
    filters = yaml.load(filters_file, Loader=yaml.Loader) if filters_file else {}

    def matches_entry(entry, match_exprs, default=True):
        """Determines whether an entry meets the inclusion and exclusion criteria

        Parameters
        ----------
        entry : dict[str, Any]
            a image entry in the manifest
        exprs : list[dict[str, str]]
            match criteria
        default : bool
            the value if match_exprs are empty
        """
        if not match_exprs:
            return default
        return re.match(
            "|".join(
                i["name"].replace(".", "\\.").replace("*", ".*") for i in match_exprs
            ),
            entry["name"],
        )

    with xnat.connect(
        server=server,
        user=user,
        password=password,
    ) as xlogin:

        for entry in manifest["images"]:
            if matches_entry(entry, filters.get("include")) and not matches_entry(
                entry, filters.get("exclude"), default=False
            ):
                tag = f"{entry['name']}:{entry['version']}"
                xlogin.post(
                    "/xapi/docker/pull", query={"image": tag, "save-commands": True}
                )

                # Enable the commands in the built image
                for cmd in xlogin.get("/xapi/commands").json():
                    if cmd["image"] == tag:
                        for wrapper in cmd["xnat"]:
                            xlogin.put(
                                f"/xapi/commands/{cmd['id']}/"
                                f"wrappers/{wrapper['id']}/enabled"
                            )
                click.echo(f"Installed and enabled {tag}")
            else:
                click.echo(f"Skipping {tag} as it doesn't match filters")

    click.echo(
        f"Successfully updated all container images from '{manifest['release']}' of "
        f"'{manifest['package']}' package that match provided filters"
    )


@xnat_group.command(
    name="auth-refresh",
    help="""Logs into the XNAT instance and regenerates a new authorisation token
to avoid them expiring (2 days by default)

CONFIG_YAML a YAML file contains the login details for the XNAT server to update
""",
)
@click.argument("config_yaml_file", type=click.File())
@click.argument("auth_file_path", type=click.Path(exists=True))
def xnat_auth_refresh(config_yaml_file, auth_file_path):
    config = yaml.load(config_yaml_file, Loader=yaml.Loader)
    with open(auth_file_path) as fp:
        auth = json.load(fp)

    with xnat.connect(
        server=config["server"], user=auth["alias"], password=auth["secret"]
    ) as xlogin:
        alias, secret = xlogin.services.issue_token()

    with open(auth_file_path, "w") as f:
        json.dump(
            {
                "alias": alias,
                "secret": secret,
            },
            f,
        )

    click.echo(f"Updated XNAT connection token to {config['server']} successfully")
