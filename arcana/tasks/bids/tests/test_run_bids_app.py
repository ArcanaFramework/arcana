from functools import reduce
import json
from operator import mul
from arcana.core.test.fixtures.common import make_dataset, TestDatasetBlueprint
from arcana.data.formats.common import Text
from arcana.data.spaces.medimage import Clinical
from arcana.data.formats.medimage import NiftiGzX
from arcana.cli.deploy import run_in_image
from arcana.core.utils import class_location, path2varname
from arcana.core.test.utils import show_cli_trace


def test_run_bids_pipeline(
    mock_bids_app_executable, cli_runner, nifti_sample_dir, work_dir
):

    blueprint = TestDatasetBlueprint(
        hierarchy=[Clinical.subject, Clinical.session],
        dim_lengths=[1, 1, 1],
        files=[
            "anat/T1w.nii.gz",
            "anat/T1w.json",
            "anat/T2w.nii.gz",
            "anat/T2w.json",
            "dwi/dwi.nii.gz",
            "dwi/dwi.json",
            "dwi/dwi.bvec",
            "dwi/dwi.bval",
        ],
        expected_formats={
            "anat/T1w": (NiftiGzX, ["T1w.nii.gz", "T1w.json"]),
            "anat/T2w": (NiftiGzX, ["T2w.nii.gz", "T2w.json"]),
            "dwi/dwi": (NiftiGzX, ["dwi.nii.gz", "dwi.json", "dwi.bvec", "dwi.bval"]),
        },
        derivatives=[
            ("file1", Clinical.session, Text, ["file1.txt"]),
            ("file2", Clinical.session, Text, ["file2.txt"]),
        ],
    )

    dataset_path = work_dir / "bids-dataset"

    dataset = make_dataset(
        dataset_path=dataset_path, blueprint=blueprint, source_data=nifti_sample_dir
    )

    blueprint = dataset.__annotations__["blueprint"]

    dataset_id_str = f"file//{dataset_path}"
    # Start generating the arguments for the CLI
    # Add source to loaded dataset
    args = [
        "arcana.tasks.bids.app:bids_app",
        "a_bids_app",
        dataset_id_str,
        "--plugin",
        "serial",
        "--work",
        str(work_dir),
        "--configuration",
        "executable",
        str(mock_bids_app_executable),
        "--dataset-space",
        class_location(blueprint.space),
        "--dataset-hierarchy",
        ",".join([str(ln) for ln in blueprint.hierarchy]),
    ]
    inputs_config = []
    for path, (format, _) in blueprint.expected_formats.items():
        format_str = class_location(format)
        varname = path2varname(path)
        args.extend(["--input-config", varname, format_str, varname, format_str])
        args.extend(["--input", varname, varname])
        inputs_config.append({"name": varname, "path": path, "format": format_str})
    args.extend(
        ["--configuration", "inputs", json.dumps(inputs_config)]
    )  # .replace('"', '\\"')
    outputs_config = []
    for path, _, format, _ in blueprint.derivatives:
        format_str = class_location(format)
        varname = path2varname(path)
        args.extend(["--output-config", varname, format_str, varname, format_str])
        args.extend(["--output", varname, varname])
        outputs_config.append({"name": varname, "path": path, "format": format_str})
    args.extend(
        ["--configuration", "outputs", json.dumps(outputs_config)]
    )  # .replace('"', '\\"')

    result = cli_runner(run_in_image, args)
    assert result.exit_code == 0, show_cli_trace(result)
    # Add source column to saved dataset
    for fname in ["file1", "file2"]:
        sink = dataset.add_sink(fname, Text)
        assert len(sink) == reduce(mul, blueprint.dim_lengths)
        for item in sink:
            item.get(assume_exists=True)
            with open(item.fs_path) as f:
                contents = f.read()
            assert contents == fname + "\n"
