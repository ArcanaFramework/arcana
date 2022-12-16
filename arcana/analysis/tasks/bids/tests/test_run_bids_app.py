from functools import reduce
from operator import mul
from arcana.core.utils.testing.fixtures.common import make_dataset, TestDatasetBlueprint
from arcana.data.types.common import Text
from arcana.data.spaces.medimage import Clinical
from arcana.data.types.medimage import NiftiGzX
from arcana.cli.xnat import cs_entrypoint
from arcana.core.utils.serialize import ClassResolver
from arcana.core.utils.misc import path2varname
from arcana.core.utils.testing import show_cli_trace
from arcana.deploy.common import PipelineImage


def test_bids_app_entrypoint(
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

    spec_path = work_dir / "spec.yaml"

    blueprint = dataset.__annotations__["blueprint"]

    dataset_locator = f"file//{dataset_path}"
    # Start generating the arguments for the CLI
    # Add source to loaded dataset
    args = [
        dataset_locator,
        "--plugin",
        "serial",
        "--work",
        str(work_dir),
        "--spec-path",
        spec_path,
        "--dataset-hierarchy",
        ",".join([str(ln) for ln in blueprint.hierarchy]),
    ]
    inputs_config = {}
    for path, (datatype, _) in blueprint.expected_formats.items():
        format_str = ClassResolver.tostr(datatype)
        varname = path2varname(path)
        args.extend(["--input", varname, varname])
        inputs_config[varname] = {
            "configuration": {
                "path": path,
            },
            "datatype": format_str,
            "help_string": "dummy",
        }

    outputs_config = {}
    for path, _, datatype, _ in blueprint.derivatives:
        format_str = ClassResolver.tostr(datatype)
        varname = path2varname(path)
        args.extend(["--output", varname, varname])
        outputs_config[varname] = {
            "configuration": {
                "path": path,
            },
            "datatype": format_str,
            "help_string": "dummy",
        }

    image_spec = PipelineImage(
        name="test_bids_app_entrypoint",
        version="1.0",
        build_iteration="1",
        description="a test image",
        authors=[{"name": "Some One", "email": "some.one@an.email.org"}],
        info_url="http://concatenate.readthefakedocs.io",
        command={
            "task": "arcana.analysis.tasks.bids.app:bids_app",
            "row_frequency": "medimage:Clinical[session]",
            "inputs": inputs_config,
            "outputs": outputs_config,
            "configuration": {
                "executable": str(mock_bids_app_executable),
            },
        },
    )
    image_spec.save(spec_path)

    result = cli_runner(cs_entrypoint, args)
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
