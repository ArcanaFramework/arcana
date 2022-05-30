from functools import reduce
import json
from operator import mul
from arcana.test.fixtures.common import (
    make_dataset,
    TestDatasetBlueprint)
from arcana.data.formats.common import Text
from arcana.data.spaces.medimage import Clinical
from arcana.data.formats.medimage import NiftiGzX
from arcana.cli.deploy import run_pipeline
from arcana.core.utils import class_location, path2varname
from arcana.test.utils import show_cli_trace


def test_run_bids_pipeline(mock_bids_app_executable, cli_runner, nifti_sample_dir, work_dir):

    blueprint = TestDatasetBlueprint(
        hierarchy=[Clinical.subject, Clinical.session],
        dim_lengths=[1, 1, 1],
        files=["anat/T1w.nii.gz", "anat/T1w.json", "anat/T2w.nii.gz", "anat/T2w.json",
               "dwi/dwi.nii.gz", "dwi/dwi.json", "dwi/dwi.bvec", "dwi/dwi.bval"],
        expected_formats={
            "anat/T1w": (NiftiGzX, ["T1w.nii.gz", "T1w.json"]),
            "anat/T2w": (NiftiGzX, ["T2w.nii.gz", "T2w.json"]),
            "dwi/dwi": (NiftiGzX, ["dwi.nii.gz", "dwi.json", "dwi.bvec", "dwi.bval"])},
        derivatives=[('file1', Clinical.session, Text, ['file1.txt']),
                     ('file2', Clinical.session, Text, ['file2.txt'])])
        
    dataset_path = work_dir / 'bids-dataset'
    
    dataset = make_dataset(
        dataset_path=dataset_path,
        blueprint=blueprint,
        source_data=nifti_sample_dir)

    dataset_id_str = f'file//{dataset_path}'
    # Start generating the arguments for the CLI
    # Add source to loaded dataset
    args = [dataset_id_str, 'a_bids_app', 'arcana.tasks.bids.app:bids_app',
            '--plugin', 'serial',
            '--work', str(work_dir),
            '--configuration', 'executable', str(mock_bids_app_executable),
            '--dataset_space', class_location(blueprint.space),
            '--dataset_hierarchy', ','.join([str(l) for l in blueprint.hierarchy])]
    inputs_config = []
    for path, (format, _) in blueprint.expected_formats.items():
        format_str = class_location(format)
        varname = path2varname(path)
        args.extend(['--input', varname, format_str, varname, varname, format_str])
        inputs_config.append({'name': varname, 'path': path, 'format': format_str})
    args.extend(['--configuration', 'inputs', json.dumps(inputs_config).replace('"', '\\"')])
    outputs_config = []
    for path, _, format, _ in blueprint.derivatives:
        format_str = class_location(format)
        varname = path2varname(path)
        args.extend(['--output', varname, format_str, varname, varname, format_str])
        outputs_config.append({'name': varname, 'path': path, 'format': format_str})
    args.extend(['--configuration', 'outputs', json.dumps(outputs_config).replace('"', '\\"')])
    
    result = cli_runner(run_pipeline, args)
    assert result.exit_code == 0, show_cli_trace(result)
    # Add source column to saved dataset
    for fname in ['file1', 'file2']:
        sink = dataset.add_sink(fname, Text)
        assert len(sink) == reduce(mul, dataset.blueprint.dim_lengths)
        for item in sink:
            item.get(assume_exists=True)
            with open(item.fs_path) as f:
                contents = f.read()
            assert contents == fname + '\n'
