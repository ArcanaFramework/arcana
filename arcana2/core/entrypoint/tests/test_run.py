import os.path
from unittest.mock import Mock
import pytest
from argparse import ArgumentParser
from arcana2.core.entrypoint.run import RunAppCmd

def test_run_app(test_dicom_dataset_dir):
    parser = ArgumentParser()
    RunAppCmd.construct_parser(parser)
    args = parser.parse_args([
        'pydra.tasks.dcm2niix.Dcm2Niix',
        str(test_dicom_dataset_dir),
        '--repository', 'file_system',
        '--input', 'in_dir', 'sample-dicom', 'dicom',
        '--output', 'out_file', 'output-nifti', 'niftix_gz',
        '--dataspace', 'clinical.Clinical',
        '--hierarchy', 'session',
        '--frequency', 'session'
        # '--ids', None,
        # '--container', None,
        # '--id_inference', None,
        # '--included', [],
        # '--excluded', [],
        # '--workflow_format', [],
        # '--app_arg', []
        ])
    pipeline = RunAppCmd().run(args)

# def test_run_app(pydra_task_details, dataset):
#     task_location, task_inputs, task_outputs = pydra_task_details
#     parser = ArgumentParser()
#     RunAppCmd.construct_parser(parser)
#     args = parser.parse_args([
#         task_location,
#         str(dataset.name),
#         '--repository', 'file_system',
#         '--input', 'in_dir', 'sample-dicom', 'dicom',
#         '--output', 'out_file', 'output-nifti', 'niftix_gz',
#         '--dataspace', dataset.space.__module__ + '.' + dataset.space.__name__,
#         '--hierarchy'] + [str(l) for l in dataset.hierarchy] + [
#         '--frequency', str(max(dataset.space))
#         ])
#     pipeline = RunAppCmd().run(args)
#     # pipeline.workflow.pickle_task()
#     # pipeline(plugin='serial')