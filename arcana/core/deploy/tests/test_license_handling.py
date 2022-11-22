from pathlib import Path
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
        "task": "arcana.test.tasks:concatenate",
        "description": "A pipeline to test Arcana's deployment tool",
        "inputs": [
            {
                "name": "first_file",
                "format": "common:Text",
                "task_field": "in_file1",
                "description": "the first file to pass as an input",
            },
            {
                "name": "second_file",
                "format": "common:Text",
                "task_field": "in_file2",
                "description": "the second file to pass as an input",
            },
        ],
        "outputs": [
            {
                "name": "concatenated",
                "format": "common:Text",
                "task_field": "out_file",
                "description": "an output file",
            }
        ],
        "parameters": [
            {
                "name": "number_of_duplicates",
                "type": "int",
                "task_field": "duplicates",
                "required": True,
                "description": "a parameter",
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


def test_license_download():
    pass


def test_license_site_download():
    pass
