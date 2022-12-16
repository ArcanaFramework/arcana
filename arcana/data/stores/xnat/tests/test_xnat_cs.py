import pytest
from arcana.core.utils.testing.fixtures.xnat import (
    make_mutable_dataset,
    TEST_XNAT_DATASET_BLUEPRINTS,
    TestXnatDatasetBlueprint,
    ResourceBlueprint,
    ScanBlueprint,
)
from arcana.deploy.xnat.image import XnatCSImage
from arcana.deploy.xnat.command import XnatCSCommand
from arcana.core.utils.testing.data.stores.xnat import (
    install_and_launch_xnat_cs_command,
)
from arcana.data.types.medimage import NiftiGzX, NiftiGzXFslgrad


PIPELINE_NAME = "test-concatenate"


@pytest.fixture(params=["func", "bids_app"], scope="session")
def run_spec(
    command_spec,
    bids_command_spec,
    xnat_repository,
    xnat_archive_dir,
    request,
    nifti_sample_dir,
    mock_bids_app_image,
):
    spec = {}
    if request.param == "func":
        spec["build"] = {
            "org": "arcana-tests",
            "name": "concatenate-xnat-cs",
            "version": "1.0",
            "description": "A pipeline to test Arcana's deployment tool",
            "command": command_spec,
            "authors": [{"name": "Some One", "email": "some.one@an.email.org"}],
            "info_url": "http://concatenate.readthefakedocs.io",
            "readme": "This is a test README",
            "registry": "a.docker.registry.io",
            "packages": {"system": ["git", "vim"]},
        }
        spec["dataset"] = make_mutable_dataset(
            dataset_id="xnat_cs_func",
            blueprint=TEST_XNAT_DATASET_BLUEPRINTS["concatenate_test"],
            xnat_repository=xnat_repository,
            xnat_archive_dir=xnat_archive_dir,
            access_method="cs",
        )
        spec["params"] = {"duplicates": 2}
    elif request.param == "bids_app":
        bids_command_spec["configuration"]["executable"] = "/launch.sh"
        spec["build"] = {
            "org": "arcana-tests",
            "name": "bids-app-xnat-cs",
            "version": "1.0",
            "description": "A pipeline to test wrapping of BIDS apps",
            "base_image": {
                "name": mock_bids_app_image,
                "package_manager": "apt",
            },
            "packages": {"system": ["git", "vim"]},
            "command": bids_command_spec,
            "authors": [
                {"name": "Some One Else", "email": "some.oneelse@an.email.org"}
            ],
            "info_url": "http://a-bids-app.readthefakedocs.io",
            "readme": "This is another test README for BIDS app image",
            "registry": "another.docker.registry.io",
        }
        blueprint = TestXnatDatasetBlueprint(
            [1, 1, 1],
            [
                ScanBlueprint(
                    "anat/T1w",
                    [
                        ResourceBlueprint(
                            "NiftiGzX", NiftiGzX, ["anat/T1w.nii.gz", "anat/T1w.json"]
                        )
                    ],
                ),
                ScanBlueprint(
                    "anat/T2w",
                    [
                        ResourceBlueprint(
                            "NiftiGzX", NiftiGzX, ["anat/T2w.nii.gz", "anat/T2w.json"]
                        )
                    ],
                ),
                ScanBlueprint(
                    "dwi/dwi",
                    [
                        ResourceBlueprint(
                            "NiftiGzXFslgrad",
                            NiftiGzXFslgrad,
                            [
                                "dwi/dwi.nii.gz",
                                "dwi/dwi.json",
                                "dwi/dwi.bvec",
                                "dwi/dwi.bval",
                            ],
                        )
                    ],
                ),
            ],
            {},
            [],
        )
        spec["dataset"] = make_mutable_dataset(
            dataset_id="xnat_cs_bids_app",
            blueprint=blueprint,
            xnat_repository=xnat_repository,
            xnat_archive_dir=xnat_archive_dir,
            access_method="cs",
            source_data=nifti_sample_dir,
        )
        spec["params"] = {}
    else:
        assert False, f"unrecognised request param '{request.param}'"
    return spec


def test_xnat_cs_pipeline(xnat_repository, run_spec, run_prefix, work_dir):
    """Tests the complete XNAT deployment pipeline by building and running a
    container"""

    # Retrieve test dataset and build and command specs from fixtures
    build_spec = run_spec["build"]
    dataset = run_spec["dataset"]
    params = run_spec["params"]
    blueprint = dataset.__annotations__["blueprint"]

    # Append run_prefix to command name to avoid clash with previous test runs
    build_spec["name"] = "xnat-cs-test" + run_prefix

    image_spec = XnatCSImage(**build_spec)

    image_spec.make(
        build_dir=work_dir,
        arcana_install_extras=["test"],
        use_local_packages=True,
        use_test_config=True,
    )

    # We manually set the command in the test XNAT instance as commands are
    # loaded from images when they are pulled from a registry and we use
    # the fact that the container service test XNAT instance shares the
    # outer Docker socket. Since we build the pipeline image with the same
    # socket there is no need to pull it.
    xnat_command = image_spec.command.make_json()

    launch_inputs = {}

    for inpt, scan in zip(xnat_command["inputs"], blueprint.scans):
        launch_inputs[XnatCSCommand.path2xnatname(inpt["name"])] = scan.name

    for pname, pval in params.items():
        launch_inputs[pname] = pval

    with xnat_repository:

        xlogin = xnat_repository.login

        test_xsession = next(iter(xlogin.projects[dataset.id].experiments.values()))

        workflow_id, status, out_str = install_and_launch_xnat_cs_command(
            command_json=xnat_command,
            project_id=dataset.id,
            session_id=test_xsession.id,
            inputs=launch_inputs,
            xlogin=xlogin,
        )

        assert status == "Complete", f"Workflow {workflow_id} failed.\n{out_str}"

        for deriv in blueprint.derivatives:
            assert list(test_xsession.resources[deriv.name].files) == deriv.filenames
