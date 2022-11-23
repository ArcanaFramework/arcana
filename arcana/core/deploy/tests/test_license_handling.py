from pathlib import Path
import docker.errors
from arcana.deploy.common import PipelineImage


license_path = "/path/to/licence.txt"

license_contents = "license contents"

license_build = {
    "org": "arcana-tests",
    "name": "concatenate-xnat-cs",
    "version": "1.0",
    "authors": [{"name": "Some One", "email": "some.one@an.email.org"}],
    "info_url": "http://concatenate.readthefakedocs.io",
    "system_packages": [],
    "python_packages": [],
    "readme": "This is a test README",
    "registry": "a.docker.registry.io",
    "licenses": [
        {
            "name": "testlicense",
            "destination": license_path,
            "info_url": "http://license.test",
            "description": "This is a license to test the build structure",
        }
    ],
    "command": {
        "task": "arcana.test.tasks:check_license",
        "description": "A pipeline to test Arcana's deployment tool",
        "inputs": [
            {
                "name": "license_file",
                "format": "common:Text",
                "task_field": "license_path",
                "description": "the path to the license",
            },
        ],
        "outputs": [
            {
                "name": "validated_license",
                "format": "common:Text",
                "task_field": "out",
                "description": "the validated license path",
            }
        ],
        "parameters": [
            {
                "name": "license_contents",
                "type": "str",
                "task_field": "license_contents",
                "required": True,
                "description": "the expected contents of the license file",
            }
        ],
        "row_frequency": "session",
    },
}


def test_license_installation(work_dir: Path, run_prefix: str, xnat_connect):

    # Create pipeline
    image_spec = PipelineImage(**license_build)

    # Build
    build_dir = work_dir / "build"
    build_dir.mkdir()
    image_spec.make(build_dir)

    dc = docker.from_env()

    args = [
        "--input",
        "license_file",
        license_path,
        "--parameter",
        "license_contents",
        f"'{license_contents}'",
    ]

    dc.run(image_spec.tag, args)


def test_license_download():
    pass


def test_license_site_download():
    pass
