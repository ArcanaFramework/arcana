import time
import pytest
from arcana.deploy.medimage.xnat import build_xnat_cs_image, generate_xnat_cs_command, path2xnatname
from arcana.test.fixtures.medimage import (
    make_mutable_dataset,
    TEST_XNAT_DATASET_BLUEPRINTS,
    TestXnatDatasetBlueprint,
    ResourceBlueprint,
    ScanBlueprint,
)
from arcana.data.formats.medimage import NiftiGzX
from arcana.core.utils import path2varname


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
        spec['build'] = {
            "image_tag": "arcana-tests/concatenate-xnat-cs",
            "commands": [command_spec],
            "authors": ["some.one@an.org"],
            "info_url": "http://concatenate.readthefakedocs.io",
            "system_packages": [],
            "python_packages": [],
            "readme": "This is a test README",
            "docker_registry": "a.docker.registry.io",
            "use_local_packages": True,
            "arcana_install_extras": ["test"],
            "test_config": True,
        }
        spec['dataset'] = make_mutable_dataset(
            dataset_name="xnat_cs_func",
            blueprint=TEST_XNAT_DATASET_BLUEPRINTS["concatenate_test"],
            xnat_repository=xnat_repository,
            xnat_archive_dir=xnat_archive_dir,
            access_method="cs",
        )
        spec['params'] = {'duplicates': 2}
    elif request.param == "bids_app":
        spec['build'] = {
            "image_tag": "arcana-tests/bids-app-xnat-cs",
            "base_image": mock_bids_app_image,
            "commands": [bids_command_spec],
            "authors": ["some.one.else@another.org"],
            "info_url": "http://a-bids-app.readthefakedocs.io",
            "system_packages": [],
            "python_packages": [],
            "package_manager": "apt",
            "readme": "This is another test README for BIDS app image",
            "docker_registry": "another.docker.registry.io",
            "use_local_packages": True,
            "test_config": True,
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
                            "NiftiGzX",
                            NiftiGzX,
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
        spec['dataset'] = make_mutable_dataset(
            dataset_name="xnat_cs_bids_app",
            blueprint=blueprint,
            xnat_repository=xnat_repository,
            xnat_archive_dir=xnat_archive_dir,
            access_method="cs",
            source_data=nifti_sample_dir,
        )
        spec['params'] = {}
    else:
        assert False, f"unrecognised request param '{request.param}'"
    return spec


def test_xnat_cs_pipeline(
    xnat_repository, run_spec, run_prefix, work_dir
):
    """Tests the complete XNAT deployment pipeline by building and running a
    container"""

    # Retrieve test dataset and build and command specs from fixtures
    build_spec = run_spec["build"]
    dataset = run_spec["dataset"]
    params = run_spec["params"]
    command_spec = build_spec["commands"][0]

    # Append run_prefix to command name to avoid clash with previous test runs
    cmd_name = command_spec["name"] = "xnat-cs-test" + run_prefix

    # build_xnat_cs_image(build_dir=work_dir, **run_spec["commands"])

    with xnat_repository:

        xlogin = xnat_repository.login

        # We manually set the command in the test XNAT instance as commands are
        # loaded from images when they are pulled from a registry and we use
        # the fact that the container service test XNAT instance shares the
        # outer Docker socket. Since we build the pipeline image with the same
        # socket there is no need to pull it.
        xnat_command = generate_xnat_cs_command(
            image_tag=build_spec["image_tag"], **command_spec
        )
        cmd_id = xlogin.post("/xapi/commands", json=xnat_command).json()

        # Enable the command globally and in the project
        xlogin.put(f"/xapi/commands/{cmd_id}/wrappers/{cmd_name}/enabled")
        xlogin.put(
            f"/xapi/projects/{dataset.id}/commands/{cmd_id}/wrappers/{cmd_name}/enabled"
        )

        test_xsession = next(iter(xlogin.projects[dataset.id].experiments.values()))

        launch_json = {"SESSION": f"/archive/experiments/{test_xsession.id}"}

        for inpt, scan in zip(xnat_command['inputs'], dataset.blueprint.scans):
            launch_json[path2xnatname(inpt['path'])] = scan.name

        for pname, pval in params.items():
            launch_json[pname] = pval

        launch_result = xlogin.post(
            f"/xapi/projects/{dataset.id}/wrappers/{cmd_id}/root/SESSION/launch",
            json=launch_json
        ).json()

        assert launch_result["status"] == "success"
        workflow_id = launch_result["workflow-id"]
        assert workflow_id != "To be assigned"

        NUM_ATTEMPTS = 100
        SLEEP_PERIOD = 10
        max_runtime = NUM_ATTEMPTS * SLEEP_PERIOD

        INCOMPLETE_STATES = (
            "Pending",
            "Running",
            "_Queued",
            "Staging",
            "Finalizing",
            "Created",
        )

        for i in range(NUM_ATTEMPTS):
            wf_result = xlogin.get(f"/xapi/workflows/{workflow_id}").json()
            if wf_result["status"] not in INCOMPLETE_STATES:
                break
            time.sleep(SLEEP_PERIOD)

        # Get workflow stdout/stderr for error messages if required
        out_str = ""
        stdout_result = xlogin.get(
            f"/xapi/workflows/{workflow_id}/logs/stdout", accepted_status=[200, 204]
        )
        if stdout_result.status_code == 200:
            out_str = f"stdout:\n{stdout_result.content.decode('utf-8')}\n"
        stderr_result = xlogin.get(
            f"/xapi/workflows/{workflow_id}/logs/stderr", accepted_status=[200, 204]
        )
        if stderr_result.status_code == 200:
            out_str += f"\nstderr:\n{stderr_result.content.decode('utf-8')}"

        assert (
            i != 99
        ), f"Workflow {workflow_id} did not complete in {max_runtime}.\n{out_str}"
        assert (
            wf_result["status"] == "Complete"
        ), f"Workflow {workflow_id} failed.\n{out_str}"

        for deriv in dataset.blueprint.derivatives:
            assert list(test_xsession.resources[deriv.name].files) == deriv.filenames
