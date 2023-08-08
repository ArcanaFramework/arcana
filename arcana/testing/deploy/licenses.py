from arcana.core.deploy.image import App
from arcana.core.data.set import Dataset
from arcana.common import DirTree, Samples


def get_pipeline_image(license_path, app_cls=App) -> App:
    return app_cls(
        title="A test of the license installation",
        name="to_be_overridden",
        org=ORG,
        version="1.0",
        authors=[{"name": "Some One", "email": "some.one@an.email.org"}],
        docs={
            "info_url": "http://concatenate.readthefakedocs.io",
        },
        readme="This is a test README",
        packages={
            "pip": ["fileformats"],
        },
        licenses={
            LICENSE_NAME: {
                "destination": license_path,
                "info_url": "http://license.test",
                "description": "This is a license to test the build structure",
            }
        },
        command={
            "task": "arcana.testing.tasks:check_license",
            "row_frequency": "common:Samples[sample]",
            "inputs": [
                {
                    "name": LICENSE_INPUT_FIELD,
                    "datatype": "text/plain",
                    "field": "expected_license_contents",
                    "help": "the path to the license",
                },
            ],
            "outputs": [
                {
                    "name": LICENSE_OUTPUT_FIELD,
                    "datatype": "text/plain",
                    "field": "out",
                    "help": "the validated license path",
                }
            ],
            "parameters": [
                {
                    "name": LICENSE_PATH_PARAM,
                    "datatype": "field/text",
                    "field": "expected_license_path",
                    "required": True,
                    "help": "the expected contents of the license file",
                }
            ],
        },
    )


def make_dataset(dataset_dir) -> Dataset:

    contents_dir = dataset_dir / "sample1"
    contents_dir.mkdir(parents=True)

    with open(contents_dir / (LICENSE_INPUT_PATH + ".txt"), "w") as f:
        f.write(LICENSE_CONTENTS)

    dataset = DirTree().define_dataset(dataset_dir, space=Samples)
    dataset.save()
    return dataset


ORG = "arcana-tests"
REGISTRY = "a.docker.registry.io"
IMAGE_VERSION = "1.0"


LICENSE_CONTENTS = "license contents"

LICENSE_NAME = "testlicense"

LICENSE_INPUT_FIELD = "license_file"

LICENSE_OUTPUT_FIELD = "validated_license_file"

LICENSE_PATH_PARAM = "license_path"

LICENSE_INPUT_PATH = "contents-file"

LICENSE_OUTPUT_PATH = "validated-file"
