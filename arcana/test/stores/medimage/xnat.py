import typing as ty
import time
from pathlib import Path
import random
from dataclasses import dataclass
from tempfile import mkdtemp
from itertools import product
import xnat
import xnat4tests
from arcana.data.stores.medimage.xnat.api import Xnat
from arcana.data.stores.medimage.xnat.cs import XnatViaCS
from arcana.data.spaces.medimage import Clinical
from arcana.test.datasets import create_test_file
from arcana.exceptions import ArcanaError


@dataclass
class ResourceBlueprint:

    name: str
    format: type
    filenames: ty.List[str]


@dataclass
class ScanBlueprint:

    name: str
    resources: ty.List[ResourceBlueprint]


@dataclass
class DerivBlueprint:

    name: str
    row_frequency: Clinical
    format: type
    filenames: ty.List[str]


@dataclass
class TestXnatDatasetBlueprint:

    dim_lengths: ty.List[int]
    scans: ty.List[ScanBlueprint]
    id_inference: ty.Dict[str, str]
    derivatives: ty.List[DerivBlueprint]  # files to insert as derivatives


def make_mutable_dataset(
    dataset_id: str,
    blueprint: TestXnatDatasetBlueprint,
    xnat_repository: Xnat,
    xnat_archive_dir: Path,
    access_method: str,
    dataset_name: str = None,
    source_data: Path = None,
):
    """Create a dataset (project) in the test XNAT repository"""
    test_suffix = "mutable" + access_method + str(hex(random.getrandbits(16)))[2:]
    run_prefix = xnat_repository.__annotations__["run_prefix"]
    # Need to create a new dataset per function so it can be safely modified
    # by the test without messing up other tests.
    create_dataset_data_in_repo(
        dataset_name=dataset_id,
        blueprint=blueprint,
        run_prefix=run_prefix,
        test_suffix=test_suffix,
        source_data=source_data,
    )
    return access_dataset(
        xnat_repository=xnat_repository,
        dataset_id=dataset_id,
        dataset_name=dataset_name,
        blueprint=blueprint,
        access_method=access_method,
        xnat_archive_dir=xnat_archive_dir,
        test_suffix=test_suffix,
    )


def make_project_id(dataset_name: str, run_prefix: str = None, test_suffix: str = ""):
    return (run_prefix if run_prefix else "") + dataset_name + test_suffix


def access_dataset(
    xnat_repository: Xnat,
    dataset_id: str,
    blueprint: TestXnatDatasetBlueprint,
    access_method: str,
    xnat_archive_dir: Path,
    dataset_name: str = None,
    test_suffix: str = "",
):

    run_prefix = xnat_repository.__annotations__["run_prefix"]
    proj_name = make_project_id(dataset_id, run_prefix, test_suffix)
    if access_method == "cs":
        # Create a new repository access object that accesses data directly
        # via the XNAT archive directory, like
        proj_dir = xnat_archive_dir / proj_name / "arc001"
        xnat_repository = XnatViaCS(
            server=xnat_repository.server,
            user=xnat_repository.user,
            password=xnat_repository.password,
            cache_dir=xnat_repository.cache_dir,
            row_frequency=Clinical.dataset,
            input_mount=proj_dir,
            output_mount=Path(mkdtemp()),
        )
    elif access_method != "api":
        assert False

    dataset = xnat_repository.new_dataset(
        proj_name, id_inference=blueprint.id_inference, name=dataset_name
    )
    # Stash the args used to create the dataset in attributes so they can be
    # used by tests
    dataset.__annotations__["blueprint"] = blueprint
    dataset.__annotations__["access_method"] = access_method
    return dataset


def create_dataset_data_in_repo(
    dataset_name: str,
    blueprint: TestXnatDatasetBlueprint,
    run_prefix: str = "",
    test_suffix: str = "",
    source_data: Path = None,
):
    """
    Creates dataset for each entry in dataset_structures
    """
    proj_name = make_project_id(dataset_name, run_prefix, test_suffix)

    with xnat4tests.connect() as login:
        login.put(f"/data/archive/projects/{proj_name}")

    with xnat4tests.connect() as login:
        xproject = login.projects[proj_name]
        xclasses = login.classes
        for id_tple in product(*(list(range(d)) for d in blueprint.dim_lengths)):
            ids = dict(zip(Clinical.axes(), id_tple))
            # Create subject
            subject_label = "".join(f"{b}{ids[b]}" for b in Clinical.subject.span())
            xsubject = xclasses.SubjectData(label=subject_label, parent=xproject)
            # Create session
            session_label = "".join(f"{b}{ids[b]}" for b in Clinical.session.span())
            xsession = xclasses.MrSessionData(label=session_label, parent=xsubject)

            for i, scan in enumerate(blueprint.scans, start=1):
                # Create scan
                xscan = xclasses.MrScanData(id=i, type=scan.name, parent=xsession)
                for resource in scan.resources:

                    tmp_dir = Path(mkdtemp())
                    # Create the resource
                    xresource = xscan.create_resource(resource.name)
                    # Create the dummy files
                    for fname in resource.filenames:
                        if source_data is not None:
                            fpath = source_data.joinpath(*fname.split("/"))
                            target_fpath = fpath.name
                        else:
                            fpath = create_test_file(fname, tmp_dir)
                            target_fpath = str(fpath)
                        xresource.upload(str(tmp_dir / fpath), target_fpath)


# List of intermediatary states can pass through
# before completing successfully
INCOMPLETE_CS_STATES = (
    "Pending",
    "Running",
    "_Queued",
    "Staging",
    "Finalizing",
    "Created",
    "_die",
)


def install_and_launch_xnat_cs_command(
    command_json: dict,
    project_id: str,
    session_id: str,
    inputs: ty.Dict[str, str],
    xlogin: xnat.XNATSession,
    timeout: int = 1000,  # seconds
    poll_interval: int = 10,  # seconds
):
    """Installs a new command for the XNAT container service and lanches it on
    the specified session.

    Parameters
    ----------
    cmd_name : str
        The name to install the command as
    command_json : dict[str, Any]
        JSON that defines the XNAT command in the container service (see `generate_xnat_command`)
    project_id : str
        ID of the project to enable the command for
    session_id : str
        ID of the session to launch the command on
    inputs : dict[str, str]
        Inputs passed to the pipeline at launch (i.e. typically through text fields in the CS launch UI)
    xlogin : xnat.XNATSession
        XnatPy connection to the XNAT server
    timeout : int
        the time to wait for the pipeline to complete (seconds)
    poll_interval : int
        the time interval between status polls (seconds)

    Returns
    -------
    workflow_id : int
        the auto-generated ID for the launched workflow
    status : str
        the status of the completed workflow
    out_str : str
        stdout and stderr from the workflow run
    """

    cmd_name = command_json["name"]
    wrapper_name = command_json["xnat"][0]["name"]
    cmd_id = xlogin.post("/xapi/commands", json=command_json).json()

    # Enable the command globally and in the project
    xlogin.put(f"/xapi/commands/{cmd_id}/wrappers/{wrapper_name}/enabled")
    xlogin.put(
        f"/xapi/projects/{project_id}/commands/{cmd_id}/wrappers/{wrapper_name}/enabled"
    )

    launch_json = {"SESSION": f"/archive/experiments/{session_id}"}

    launch_json.update(inputs)

    launch_result = xlogin.post(
        f"/xapi/projects/{project_id}/wrappers/{cmd_id}/root/SESSION/launch",
        json=launch_json,
    ).json()

    if launch_result["status"] != "success":
        raise ArcanaError(
            f"{cmd_name} workflow wasn't launched successfully ({launch_result['status']})"
        )
    workflow_id = launch_result["workflow-id"]
    assert workflow_id != "To be assigned"

    num_attempts = (timeout // poll_interval) + 1

    for i in range(num_attempts):
        wf_result = xlogin.get(f"/xapi/workflows/{workflow_id}").json()
        if wf_result["status"] not in INCOMPLETE_CS_STATES:
            break
        time.sleep(poll_interval)

    max_runtime = num_attempts * poll_interval

    container_id = wf_result["comments"]

    # Get workflow stdout/stderr for error messages if required
    out_str = ""
    stdout_result = xlogin.get(
        f"/xapi/containers/{container_id}/logs/stdout", accepted_status=[200, 204]
    )
    if stdout_result.status_code == 200:
        out_str = f"stdout:\n{stdout_result.content.decode('utf-8')}\n"

    stderr_result = xlogin.get(
        f"/xapi/containers/{container_id}/logs/stderr", accepted_status=[200, 204]
    )
    if stderr_result.status_code == 200:
        out_str += f"\nstderr:\n{stderr_result.content.decode('utf-8')}"

    if i == num_attempts - 1:
        status = f"NotCompletedAfter{max_runtime}Seconds"
    else:
        status = wf_result["status"]

    return workflow_id, status, out_str
